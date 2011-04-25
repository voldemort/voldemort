/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.VoldemortAdminRequest;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.routing.RoutingStrategy;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.rebalance.RebalancePartitionsInfoLiveCycle;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.storage.StorageService;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreOperationFailureException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.RebalancePartitionsInfoLifeCycleStatus;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.stats.StreamStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * Protocol buffers implementation of a {@link RequestHandler}
 * 
 */
public class AdminServiceRequestHandler implements RequestHandler {

    private final static Logger logger = Logger.getLogger(AdminServiceRequestHandler.class);

    private final static Object lock = new Object();

    private final ErrorCodeMapper errorCodeMapper;
    private final MetadataStore metadataStore;
    private final StorageService storageService;
    private final StoreRepository storeRepository;
    private final NetworkClassLoader networkClassLoader;
    private final VoldemortConfig voldemortConfig;
    private final AsyncOperationService asyncService;
    private final Rebalancer rebalancer;
    private final StreamStats stats;
    private FileFetcher fileFetcher;

    public AdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                      StorageService storageService,
                                      StoreRepository storeRepository,
                                      MetadataStore metadataStore,
                                      VoldemortConfig voldemortConfig,
                                      AsyncOperationService asyncService,
                                      Rebalancer rebalancer,
                                      StreamStats stats) {
        this.errorCodeMapper = errorCodeMapper;
        this.storageService = storageService;
        this.metadataStore = metadataStore;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
        this.asyncService = asyncService;
        this.rebalancer = rebalancer;
        this.stats = stats;
        setFetcherClass(voldemortConfig);
    }

    private void setFetcherClass(VoldemortConfig voldemortConfig) {
        if(voldemortConfig != null) {
            String className = voldemortConfig.getAllProps().getString("file.fetcher.class", null);
            if(className == null || className.trim().length() == 0) {
                this.fileFetcher = null;
            } else {
                try {
                    logger.info("Loading fetcher " + className);
                    Class<?> cls = Class.forName(className.trim());
                    this.fileFetcher = (FileFetcher) ReflectUtils.callConstructor(cls,
                                                                                  new Class<?>[] { Props.class },
                                                                                  new Object[] { voldemortConfig.getAllProps() });
                } catch(Exception e) {
                    throw new VoldemortException("Error loading file fetcher class " + className, e);
                }
            }
        } else {
            this.fileFetcher = null;
        }
    }

    public StreamRequestHandler handleRequest(final DataInputStream inputStream,
                                              final DataOutputStream outputStream)
            throws IOException {
        // Another protocol buffers bug here, temp. work around
        VoldemortAdminRequest.Builder request = VoldemortAdminRequest.newBuilder();
        int size = inputStream.readInt();

        if(logger.isTraceEnabled())
            logger.trace("In handleRequest, request specified size of " + size + " bytes");

        if(size < 0)
            throw new IOException("In handleRequest, request specified size of " + size + " bytes");

        byte[] input = new byte[size];
        ByteUtils.read(inputStream, input);
        request.mergeFrom(input);

        switch(request.getType()) {
            case GET_METADATA:
                ProtoUtils.writeMessage(outputStream, handleGetMetadata(request.getGetMetadata()));
                break;
            case UPDATE_METADATA:
                ProtoUtils.writeMessage(outputStream,
                                        handleUpdateMetadata(request.getUpdateMetadata()));
                break;
            case DELETE_PARTITION_ENTRIES:
                ProtoUtils.writeMessage(outputStream,
                                        handleDeletePartitionEntries(request.getDeletePartitionEntries()));
                break;
            case FETCH_PARTITION_ENTRIES:
                return handleFetchPartitionEntries(request.getFetchPartitionEntries());

            case UPDATE_PARTITION_ENTRIES:
                return handleUpdatePartitionEntries(request.getUpdatePartitionEntries());

            case INITIATE_FETCH_AND_UPDATE:
                ProtoUtils.writeMessage(outputStream,
                                        handleFetchAndUpdate(request.getInitiateFetchAndUpdate()));
                break;
            case ASYNC_OPERATION_STATUS:
                ProtoUtils.writeMessage(outputStream,
                                        handleAsyncStatus(request.getAsyncOperationStatus()));
                break;
            case INITIATE_REBALANCE_NODE:
                ProtoUtils.writeMessage(outputStream,
                                        handleRebalanceNode(request.getInitiateRebalanceNode()));
                break;
            case ASYNC_OPERATION_LIST:
                ProtoUtils.writeMessage(outputStream,
                                        handleAsyncOperationList(request.getAsyncOperationList()));
                break;
            case ASYNC_OPERATION_STOP:
                ProtoUtils.writeMessage(outputStream,
                                        handleAsyncOperationStop(request.getAsyncOperationStop()));
                break;
            case TRUNCATE_ENTRIES:
                ProtoUtils.writeMessage(outputStream,
                                        handleTruncateEntries(request.getTruncateEntries()));
                break;
            case ADD_STORE:
                ProtoUtils.writeMessage(outputStream, handleAddStore(request.getAddStore()));
                break;
            case DELETE_STORE:
                ProtoUtils.writeMessage(outputStream, handleDeleteStore(request.getDeleteStore()));
                break;
            case FETCH_STORE:
                ProtoUtils.writeMessage(outputStream, handleFetchStore(request.getFetchStore()));
                break;
            case SWAP_STORE:
                ProtoUtils.writeMessage(outputStream, handleSwapStore(request.getSwapStore()));
                break;
            case ROLLBACK_STORE:
                ProtoUtils.writeMessage(outputStream,
                                        handleRollbackStore(request.getRollbackStore()));
                break;
            case GET_RO_METADATA:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROMetadata(request.getGetRoMetadata()));
                break;
            case FETCH_PARTITION_FILES:
                return handleFetchPartitionFiles(request.getFetchPartitionFiles());
            case UPDATE_SLOP_ENTRIES:
                return handleUpdateSlopEntries(request.getUpdateSlopEntries());
            case FAILED_FETCH_STORE:
                ProtoUtils.writeMessage(outputStream,
                                        handleFailedFetch(request.getFailedFetchStore()));
                break;
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }

        return null;
    }

    private VAdminProto.GetROMetadataResponse handleGetROMetadata(VAdminProto.GetROMetadataRequest request) {
        final List<String> storeNames = request.getStoreNameList();
        VAdminProto.GetROMetadataResponse.Builder response = VAdminProto.GetROMetadataResponse.newBuilder();

        try {
            for(String storeName: storeNames) {

                ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                       storeRepository,
                                                                       storeName);
                VAdminProto.ROMetadataMap.Builder storeResponseBuilder = VAdminProto.ROMetadataMap.newBuilder()
                                                                                                  .setStoreName(storeName);

                switch(request.getType()) {
                    case CURRENT_VERSION_DIR:
                        storeResponseBuilder.setValue(store.getCurrentDirPath());
                        break;

                    case MAX_VERSION_DIR:
                        File storeDirPath = new File(store.getStoreDirPath());

                        if(!storeDirPath.exists())
                            throw new VoldemortException("Unable to locate the directory of the read-only store "
                                                         + storeName);

                        File[] versionDirs = ReadOnlyUtils.getVersionDirs(storeDirPath);
                        File[] kthDir = ReadOnlyUtils.findKthVersionedDir(versionDirs,
                                                                          versionDirs.length - 1,
                                                                          versionDirs.length - 1);
                        storeResponseBuilder.setValue(kthDir[0].getAbsolutePath());
                        break;

                    case STORAGE_FORMAT:
                        storeResponseBuilder.setValue(store.getReadOnlyStorageFormat().getCode());
                        break;
                    default:
                        throw new VoldemortException("Metadata type not supported");
                }
                response.addRoStoreMetadata(storeResponseBuilder.build());
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetROMetadata failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    private VAdminProto.FailedFetchStoreResponse handleFailedFetch(VAdminProto.FailedFetchStoreRequest request) {
        final String storeDir = request.getStoreDir();
        final String storeName = request.getStoreName();
        VAdminProto.FailedFetchStoreResponse.Builder response = VAdminProto.FailedFetchStoreResponse.newBuilder();

        try {

            if(!Utils.isReadableDir(storeDir))
                throw new VoldemortException("Could not read folder " + storeDir
                                             + " correctly to delete it");

            final ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                         storeRepository,
                                                                         storeName);

            if(store.getCurrentVersionId() == ReadOnlyUtils.getVersionId(new File(storeDir))) {
                logger.warn("Cannot delete " + storeDir + " for " + storeName
                            + " since it is the current dir");
                return response.build();
            }

            // Lets delete the folder
            Utils.rm(new File(storeDir));

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleFailedFetch failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    public StreamRequestHandler handleFetchPartitionFiles(VAdminProto.FetchPartitionFilesRequest request) {
        return new FetchPartitionFileStreamRequestHandler(request,
                                                          metadataStore,
                                                          voldemortConfig,
                                                          storeRepository,
                                                          stats);
    }

    public StreamRequestHandler handleUpdateSlopEntries(VAdminProto.UpdateSlopEntriesRequest request) {
        return new UpdateSlopEntriesRequestHandler(request, errorCodeMapper, storeRepository, stats);
    }

    public StreamRequestHandler handleFetchPartitionEntries(VAdminProto.FetchPartitionEntriesRequest request) {
        boolean fetchValues = request.hasFetchValues() && request.getFetchValues();

        if(fetchValues) {
            boolean fetchMasterValues = request.hasFetchMasterEntries()
                                        && request.getFetchMasterEntries();

            if(fetchMasterValues) {
                return new FetchMasterEntriesStreamRequestHandler(request,
                                                                  metadataStore,
                                                                  errorCodeMapper,
                                                                  voldemortConfig,
                                                                  storeRepository,
                                                                  networkClassLoader,
                                                                  stats);
            } else {
                return new FetchEntriesStreamRequestHandler(request,
                                                            metadataStore,
                                                            errorCodeMapper,
                                                            voldemortConfig,
                                                            storeRepository,
                                                            networkClassLoader,
                                                            stats);
            }
        } else
            return new FetchKeysStreamRequestHandler(request,
                                                     metadataStore,
                                                     errorCodeMapper,
                                                     voldemortConfig,
                                                     storeRepository,
                                                     networkClassLoader,
                                                     stats);
    }

    public StreamRequestHandler handleUpdatePartitionEntries(VAdminProto.UpdatePartitionEntriesRequest request) {
        return new UpdatePartitionEntriesStreamRequestHandler(request,
                                                              errorCodeMapper,
                                                              voldemortConfig,
                                                              storeRepository,
                                                              networkClassLoader,
                                                              stats);
    }

    public VAdminProto.AsyncOperationStatusResponse handleRebalanceNode(VAdminProto.InitiateRebalanceNodeRequest request) {
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder();
        try {
            if(!voldemortConfig.isEnableRebalanceService())
                throw new VoldemortException("Rebalance service is not enabled for node: "
                                             + metadataStore.getNodeId());

            VAdminProto.RebalancePartitionInfoMap rebalancePartitionInfoMap = request.getRebalancePartitionInfo();
            RebalancePartitionsInfo rebalanceStealInfo = new RebalancePartitionsInfo(rebalancePartitionInfoMap.getStealerId(),
                                                                                     rebalancePartitionInfoMap.getDonorId(),
                                                                                     rebalancePartitionInfoMap.getStealMasterPartitionsList(),
                                                                                     rebalancePartitionInfoMap.getStealReplicaPartitionsList(),
                                                                                     rebalancePartitionInfoMap.getDeletePartitionsList(),
                                                                                     rebalancePartitionInfoMap.getUnbalancedStoresList(),
                                                                                     rebalancePartitionInfoMap.getAttempt());

            RebalancePartitionsInfoLiveCycle rebalancePartitionsInfoLiveCycle = new RebalancePartitionsInfoLiveCycle(rebalanceStealInfo,
                                                                                                                     RebalancePartitionsInfoLifeCycleStatus.NEW);

            int requestId = rebalancer.rebalanceLocalNode(rebalancePartitionsInfoLiveCycle);

            response.setRequestId(requestId)
                    .setDescription(rebalanceStealInfo.toString())
                    .setStatus("Started")
                    .setComplete(false);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleRebalanceNode failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.AsyncOperationListResponse handleAsyncOperationList(VAdminProto.AsyncOperationListRequest request) {
        VAdminProto.AsyncOperationListResponse.Builder response = VAdminProto.AsyncOperationListResponse.newBuilder();
        boolean showComplete = request.hasShowComplete() && request.getShowComplete();
        try {
            response.addAllRequestIds(asyncService.getAsyncOperationList(showComplete));
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAsyncOperationList failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.AsyncOperationStopResponse handleAsyncOperationStop(VAdminProto.AsyncOperationStopRequest request) {
        VAdminProto.AsyncOperationStopResponse.Builder response = VAdminProto.AsyncOperationStopResponse.newBuilder();
        int requestId = request.getRequestId();
        try {
            asyncService.stopOperation(requestId);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAsyncOperationStop failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.RollbackStoreResponse handleRollbackStore(VAdminProto.RollbackStoreRequest request) {
        final String storeName = request.getStoreName();
        final long pushVersion = request.getPushVersion();
        VAdminProto.RollbackStoreResponse.Builder response = VAdminProto.RollbackStoreResponse.newBuilder();

        try {
            ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                   storeRepository,
                                                                   storeName);

            File rollbackVersionDir = new File(store.getStoreDirPath(), "version-" + pushVersion);

            store.rollback(rollbackVersionDir);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleRollbackStore failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    /**
     * Given a read-only store name and a directory, swaps it in while returning
     * the directory path being swapped out
     * 
     * @param storeName The name of the read-only store
     * @param directory The directory being swapped in
     * @return The directory path which was swapped out
     * @throws VoldemortException
     */
    private String swapStore(String storeName, String directory) throws VoldemortException {

        ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                               storeRepository,
                                                               storeName);

        if(!Utils.isReadableDir(directory))
            throw new VoldemortException("Store directory '" + directory
                                         + "' is not a readable directory.");

        String currentDirPath = store.getCurrentDirPath();

        store.swapFiles(directory);

        return currentDirPath;
    }

    public VAdminProto.SwapStoreResponse handleSwapStore(VAdminProto.SwapStoreRequest request) {
        final String dir = request.getStoreDir();
        final String storeName = request.getStoreName();
        VAdminProto.SwapStoreResponse.Builder response = VAdminProto.SwapStoreResponse.newBuilder();

        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server not in normal state")));
            return response.build();
        }

        try {
            response.setPreviousStoreDir(swapStore(storeName, dir));
            return response.build();
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleSwapStore failed for request(" + request.toString() + ")", e);
            return response.build();
        }
    }

    public VAdminProto.AsyncOperationStatusResponse handleFetchStore(VAdminProto.FetchStoreRequest request) {
        final String fetchUrl = request.getStoreDir();
        final String storeName = request.getStoreName();

        int requestId = asyncService.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Fetch store")
                                                                                                            .setStatus("started");
        try {
            final ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                         storeRepository,
                                                                         storeName);
            final long pushVersion;
            if(request.hasPushVersion()) {
                pushVersion = request.getPushVersion();
                if(pushVersion <= store.getCurrentVersionId())
                    throw new VoldemortException("Version of push specified (" + pushVersion
                                                 + ") should be greater than current version "
                                                 + store.getCurrentVersionId());
            } else {
                // Find the max version
                long maxVersion;
                File[] storeDirList = ReadOnlyUtils.getVersionDirs(new File(store.getStoreDirPath()));
                if(storeDirList == null || storeDirList.length == 0) {
                    throw new VoldemortException("Push version required since no version folders exist");
                } else {
                    maxVersion = ReadOnlyUtils.getVersionId(ReadOnlyUtils.findKthVersionedDir(storeDirList,
                                                                                              storeDirList.length - 1,
                                                                                              storeDirList.length - 1)[0]);
                }
                pushVersion = maxVersion + 1;
            }

            asyncService.submitOperation(requestId, new AsyncOperation(requestId, "Fetch store") {

                private String fetchDirPath = null;

                @Override
                public void markComplete() {
                    if(fetchDirPath != null)
                        status.setStatus(fetchDirPath);
                    status.setComplete(true);
                }

                @Override
                public void operate() {

                    File fetchDir = null;

                    if(fileFetcher == null) {

                        logger.warn("File fetcher class has not instantiated correctly. Assuming local file");

                        if(!Utils.isReadableDir(fetchUrl)) {
                            throw new VoldemortException("Fetch url " + fetchUrl
                                                         + " is not readable");
                        }

                        fetchDir = new File(store.getStoreDirPath(), "version-"
                                                                     + Long.toString(pushVersion));

                        if(fetchDir.exists())
                            throw new VoldemortException("Version directory "
                                                         + fetchDir.getAbsolutePath()
                                                         + " already exists");

                        Utils.move(new File(fetchUrl), fetchDir);

                    } else {

                        logger.info("Executing fetch of " + fetchUrl);
                        updateStatus("0 MB copied at 0 MB/sec - 0 % complete");
                        try {
                            fileFetcher.setAsyncOperationStatus(status);
                            fetchDir = fileFetcher.fetch(fetchUrl, store.getStoreDirPath()
                                                                   + File.separator + "version-"
                                                                   + Long.toString(pushVersion));
                            updateStatus("Completed fetch of " + fetchUrl);

                            if(fetchDir == null) {
                                throw new VoldemortException("File fetcher failed for "
                                                             + fetchUrl
                                                             + " and store name = "
                                                             + storeName
                                                             + " due to incorrect input path/checksum error");
                            } else {
                                logger.info("Fetch complete.");
                            }
                        } catch(Exception e) {
                            throw new VoldemortException("Exception in Fetcher = " + e.getMessage());
                        }

                    }
                    fetchDirPath = fetchDir.getAbsolutePath();
                }

                @Override
                public void stop() {
                    status.setException(new VoldemortException("Fetcher interrupted"));
                }
            });

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleFetchStore failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.AsyncOperationStatusResponse handleFetchAndUpdate(VAdminProto.InitiateFetchAndUpdateRequest request) {
        final int nodeId = request.getNodeId();
        final List<Integer> partitions = request.getPartitionsList();
        final VoldemortFilter filter = request.hasFilter() ? getFilterFromRequest(request.getFilter(),
                                                                                  voldemortConfig,
                                                                                  networkClassLoader)
                                                          : new DefaultVoldemortFilter();
        final String storeName = request.getStore();

        int requestId = asyncService.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Fetch and update")
                                                                                                            .setStatus("started");
        final boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                     .getType()
                                                     .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

        try {
            asyncService.submitOperation(requestId, new AsyncOperation(requestId,
                                                                       "Fetch and Update") {

                private final AtomicBoolean running = new AtomicBoolean(true);

                @Override
                public void stop() {
                    running.set(false);
                }

                @Override
                public void operate() {
                    AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                                   metadataStore.getCluster(),
                                                                                   4,
                                                                                   2);
                    try {
                        StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(storeRepository,
                                                                                                  storeName);
                        updateStatus("Initated fetchPartitionEntries");
                        EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxWriteBytesPerSec());

                        if(isReadOnlyStore) {
                            String destinationDir = ((ReadOnlyStorageEngine) storageEngine).getCurrentDirPath();
                            adminClient.fetchPartitionFiles(nodeId,
                                                            storeName,
                                                            partitions,
                                                            destinationDir);

                        } else {
                            Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                                    storeName,
                                                                                                                    partitions,
                                                                                                                    filter,
                                                                                                                    false);

                            for(long i = 0; running.get() && entriesIterator.hasNext(); i++) {
                                Pair<ByteArray, Versioned<byte[]>> entry = entriesIterator.next();

                                ByteArray key = entry.getFirst();
                                Versioned<byte[]> value = entry.getSecond();
                                try {
                                    storageEngine.put(key, value, null);
                                } catch(ObsoleteVersionException e) {
                                    // log and ignore
                                    logger.debug("migratePartition threw ObsoleteVersionException, Ignoring.");
                                }

                                throttler.maybeThrottle(key.length() + valueSize(value));
                                if((i % 1000) == 0) {
                                    updateStatus(i + " entries processed");
                                }
                            }
                        }

                    } finally {
                        adminClient.stop();
                    }
                }
            });

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleFetchAndUpdate failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.AsyncOperationStatusResponse handleAsyncStatus(VAdminProto.AsyncOperationStatusRequest request) {
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder();
        try {
            int requestId = request.getRequestId();
            AsyncOperationStatus operationStatus = asyncService.getOperationStatus(requestId);
            boolean requestComplete = asyncService.isComplete(requestId);
            response.setDescription(operationStatus.getDescription());
            response.setComplete(requestComplete);
            response.setStatus(operationStatus.getStatus());
            response.setRequestId(requestId);
            if(operationStatus.hasException())
                throw new VoldemortException(operationStatus.getException());
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAsyncStatus failed for request(" + request.toString().trim() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.DeletePartitionEntriesResponse handleDeletePartitionEntries(VAdminProto.DeletePartitionEntriesRequest request) {
        VAdminProto.DeletePartitionEntriesResponse.Builder response = VAdminProto.DeletePartitionEntriesResponse.newBuilder();
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;
        try {
            String storeName = request.getStore();
            final List<Integer> partitions = request.getPartitionsList();
            StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(storeRepository,
                                                                                      storeName);
            VoldemortFilter filter = (request.hasFilter()) ? getFilterFromRequest(request.getFilter(),
                                                                                  voldemortConfig,
                                                                                  networkClassLoader)
                                                          : new DefaultVoldemortFilter();
            RoutingStrategy routingStrategy = metadataStore.getRoutingStrategy(storageEngine.getName());

            EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
            iterator = storageEngine.entries();
            int deleteSuccess = 0;

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                ByteArray key = entry.getFirst();
                Versioned<byte[]> value = entry.getSecond();
                throttler.maybeThrottle(key.length() + valueSize(value));
                if(checkKeyBelongsToDeletePartition(key.get(), partitions, routingStrategy)
                   && filter.accept(key, value)) {
                    if(storageEngine.delete(key, value.getVersion()))
                        deleteSuccess++;
                }
            }
            response.setCount(deleteSuccess);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleDeletePartitionEntries failed for request(" + request.toString()
                         + ")", e);
        } finally {
            if(null != iterator)
                iterator.close();
        }

        return response.build();
    }

    public VAdminProto.UpdateMetadataResponse handleUpdateMetadata(VAdminProto.UpdateMetadataRequest request) {
        VAdminProto.UpdateMetadataResponse.Builder response = VAdminProto.UpdateMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");
            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                Versioned<byte[]> versionedValue = ProtoUtils.decodeVersioned(request.getVersioned());
                metadataStore.put(new ByteArray(ByteUtils.getBytes(keyString, "UTF-8")),
                                  versionedValue,
                                  null);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleUpdateMetadata failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.GetMetadataResponse handleGetMetadata(VAdminProto.GetMetadataRequest request) {
        VAdminProto.GetMetadataResponse.Builder response = VAdminProto.GetMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");
            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                List<Versioned<byte[]>> versionedList = metadataStore.get(key, null);
                int size = (versionedList.size() > 0) ? 1 : 0;

                if(size > 0) {
                    Versioned<byte[]> versioned = versionedList.get(0);
                    response.setVersion(ProtoUtils.encodeVersioned(versioned));
                }
            } else {
                throw new VoldemortException("Metadata Key passed " + keyString
                                             + " is not handled yet ...");
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetMetadata failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.TruncateEntriesResponse handleTruncateEntries(VAdminProto.TruncateEntriesRequest request) {
        VAdminProto.TruncateEntriesResponse.Builder response = VAdminProto.TruncateEntriesResponse.newBuilder();
        try {
            String storeName = request.getStore();
            StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(storeRepository,
                                                                                      storeName);

            storageEngine.truncate();
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleTruncateEntries failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.DeleteStoreResponse handleDeleteStore(VAdminProto.DeleteStoreRequest request) {
        VAdminProto.DeleteStoreResponse.Builder response = VAdminProto.DeleteStoreResponse.newBuilder();

        // don't try to delete a store in the middle of rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server is not in normal state")));
            return response.build();
        }

        try {
            String storeName = request.getStoreName();

            synchronized(lock) {

                if(storeRepository.hasLocalStore(storeName)) {
                    if(storeName.compareTo(SlopStorageEngine.SLOP_STORE_NAME) == 0) {
                        storageService.unregisterEngine(storeName,
                                                        "slop",
                                                        storeRepository.getStorageEngine(storeName));
                    } else {
                        // update stores list in metadata store
                        List<StoreDefinition> oldStoreDefList = metadataStore.getStoreDefList();
                        List<StoreDefinition> newStoreDefList = new ArrayList<StoreDefinition>();

                        for(StoreDefinition storeDef: oldStoreDefList) {
                            if(storeDef.isView()) {
                                if(storeDef.getViewTargetStoreName().compareTo(storeName) != 0) {
                                    newStoreDefList.add(storeDef);
                                } else {
                                    storageService.unregisterEngine(storeDef.getName(),
                                                                    storeDef.getType(),
                                                                    storeRepository.getStorageEngine(storeDef.getName()));
                                }
                            } else {
                                if(storeDef.getName().compareTo(storeName) != 0) {
                                    newStoreDefList.add(storeDef);
                                } else {
                                    storageService.unregisterEngine(storeDef.getName(),
                                                                    storeDef.getType(),
                                                                    storeRepository.getStorageEngine(storeDef.getName()));
                                }
                            }
                        }

                        try {
                            metadataStore.put(MetadataStore.STORES_KEY, newStoreDefList);
                        } catch(Exception e) {
                            throw new VoldemortException(e);
                        }
                    }

                } else {
                    throw new StoreOperationFailureException(String.format("Store '%s' does not exist on this server",
                                                                           storeName));
                }
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleDeleteStore failed for request(" + request.toString() + ")", e);
        }

        return response.build();

    }

    public VAdminProto.AddStoreResponse handleAddStore(VAdminProto.AddStoreRequest request) {
        VAdminProto.AddStoreResponse.Builder response = VAdminProto.AddStoreResponse.newBuilder();

        // don't try to add a store when not in normal state
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server is not in normal state")));
            return response.build();
        }

        try {
            // adding a store requires decoding the passed in store string
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            StoreDefinition def = mapper.readStore(new StringReader(request.getStoreDefinition()));

            synchronized(lock) {
                // only allow a single store to be created at a time. We'll see
                // concurrent errors when writing the
                // stores.xml file out otherwise. (see
                // ConfigurationStorageEngine.put for details)

                if(!storeRepository.hasLocalStore(def.getName())) {
                    // open the store
                    storageService.openStore(def);

                    // update stores list in metadata store (this also has the
                    // effect of updating the stores.xml file)
                    List<StoreDefinition> currentStoreDefs;
                    List<Versioned<byte[]>> v = metadataStore.get(MetadataStore.STORES_KEY, null);

                    if(((v.size() > 0) ? 1 : 0) > 0) {
                        Versioned<byte[]> currentValue = v.get(0);
                        currentStoreDefs = mapper.readStoreList(new StringReader(ByteUtils.getString(currentValue.getValue(),
                                                                                                     "UTF-8")));
                    } else {
                        currentStoreDefs = Lists.newArrayList();
                    }
                    currentStoreDefs.add(def);
                    try {
                        metadataStore.put(MetadataStore.STORES_KEY, currentStoreDefs);
                    } catch(Exception e) {
                        throw new VoldemortException(e);
                    }
                } else {
                    throw new StoreOperationFailureException(String.format("Store '%s' already exists on this server",
                                                                           def.getName()));
                }
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAddStore failed for request(" + request.toString() + ")", e);
        }

        return response.build();

    }

    /**
     * This method is used by non-blocking code to determine if the give buffer
     * represents a complete request. Because the non-blocking code can by
     * definition not just block waiting for more data, it's possible to get
     * partial reads, and this identifies that case.
     * 
     * @param buffer Buffer to check; the buffer is reset to position 0 before
     *        calling this method and the caller must reset it after the call
     *        returns
     * @return True if the buffer holds a complete request, false otherwise
     */
    public boolean isCompleteRequest(ByteBuffer buffer) {
        DataInputStream inputStream = new DataInputStream(new ByteBufferBackedInputStream(buffer));

        try {
            int dataSize = inputStream.readInt();

            if(logger.isTraceEnabled())
                logger.trace("In isCompleteRequest, dataSize: " + dataSize + ", buffer position: "
                             + buffer.position());

            if(dataSize == -1)
                return true;

            // Here we skip over the data (without reading it in) and
            // move our position to just past it.
            buffer.position(buffer.position() + dataSize);

            return true;
        } catch(Exception e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isTraceEnabled())
                logger.trace("In isCompleteRequest, probable partial read occurred: " + e);

            return false;
        }
    }

    static VoldemortFilter getFilterFromRequest(VAdminProto.VoldemortFilter request,
                                                VoldemortConfig voldemortConfig,
                                                NetworkClassLoader networkClassLoader) {
        VoldemortFilter filter = null;

        byte[] classBytes = ProtoUtils.decodeBytes(request.getData()).get();
        String className = request.getName();
        logger.debug("Attempt to load VoldemortFilter class:" + className);

        try {
            if(voldemortConfig.isNetworkClassLoaderEnabled()) {
                // TODO: network class loader was throwing NoClassDefFound for
                // voldemort.server package classes, Need testing and fixes
                logger.warn("NetworkLoader is experimental and should not be used for now.");

                Class<?> cl = networkClassLoader.loadClass(className,
                                                           classBytes,
                                                           0,
                                                           classBytes.length);
                filter = (VoldemortFilter) cl.newInstance();
            } else {
                Class<?> cl = Thread.currentThread().getContextClassLoader().loadClass(className);
                filter = (VoldemortFilter) cl.newInstance();
            }
        } catch(Exception e) {
            throw new VoldemortException("Failed to load and instantiate the filter class", e);
        }

        return filter;
    }

    static int valueSize(Versioned<byte[]> value) {
        return value.getValue().length + ((VectorClock) value.getVersion()).sizeInBytes() + 1;
    }

    static ReadOnlyStorageEngine getReadOnlyStorageEngine(MetadataStore metadata,
                                                          StoreRepository repo,
                                                          String name) {
        StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(repo, name);
        if(metadata.getStoreDef(name).getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) != 0)
            throw new VoldemortException("Store " + name
                                         + " is not a read-only store, cannot complete operation");
        return (ReadOnlyStorageEngine) storageEngine;
    }

    static StorageEngine<ByteArray, byte[], byte[]> getStorageEngine(StoreRepository storeRepository,
                                                                     String storeName) {
        StorageEngine<ByteArray, byte[], byte[]> storageEngine = storeRepository.getStorageEngine(storeName);

        if(storageEngine == null) {
            throw new VoldemortException("No store named '" + storeName + "'.");
        }

        return storageEngine;
    }

    /**
     * Check that the key belong to a delete partition.
     * <p>
     * return false if key is mastered at or replicated at any of the partitions
     * belonging to the node not specified to be deleted specifically by the
     * user. <br>
     * Fix problem during rebalancing with accidental deletion of data due to
     * changes in replication partition set.
     * 
     * TODO LOW: This assumes that the underlying storageEngines saves all
     * partition together and there is no need to copy data from partition a -->
     * b on the same machine. if this changes this will need to be made as an
     * active copy here.
     * 
     * @param key
     * @param partitionList
     * @param routingStrategy
     * @return
     */
    protected boolean checkKeyBelongsToDeletePartition(byte[] key,
                                                       List<Integer> partitionList,
                                                       RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);
        List<Integer> ownedPartitions = new ArrayList<Integer>(metadataStore.getCluster()
                                                                            .getNodeById(metadataStore.getNodeId())
                                                                            .getPartitionIds());

        ownedPartitions.removeAll(partitionList);

        for(int p: keyPartitions) {
            if(ownedPartitions.contains(p)) {
                return false;
            }

            if(partitionList.contains(p)) {
                return true;
            }
        }

        return false;
    }
}

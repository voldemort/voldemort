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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.RebalancePartitionInfoMap;
import voldemort.client.protocol.pb.VAdminProto.VoldemortAdminRequest;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.storage.StorageService;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreOperationFailureException;
import voldemort.store.metadata.MetadataStore;
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
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
            case GET_RO_MAX_VERSION_DIR:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROMaxVersionDir(request.getGetRoMaxVersionDir()));
                break;
            case GET_RO_CURRENT_VERSION_DIR:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROCurrentVersionDir(request.getGetRoCurrentVersionDir()));
                break;
            case GET_RO_STORAGE_FORMAT:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROStorageFormat(request.getGetRoStorageFormat()));
                break;
            case FETCH_PARTITION_FILES:
                return handleFetchPartitionFiles(request.getFetchPartitionFiles());
            case UPDATE_SLOP_ENTRIES:
                return handleUpdateSlopEntries(request.getUpdateSlopEntries());
            case FAILED_FETCH_STORE:
                ProtoUtils.writeMessage(outputStream,
                                        handleFailedFetch(request.getFailedFetchStore()));
                break;
            case REBALANCE_STATE_CHANGE:
                ProtoUtils.writeMessage(outputStream,
                                        handleRebalanceStateChange(request.getRebalanceStateChange()));
                break;
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }

        return null;
    }

    public VAdminProto.RebalanceStateChangeResponse handleRebalanceStateChange(VAdminProto.RebalanceStateChangeRequest request) {

        VAdminProto.RebalanceStateChangeResponse.Builder response = VAdminProto.RebalanceStateChangeResponse.newBuilder();

        try {
            // Retrieve all values first
            List<RebalancePartitionsInfo> rebalancePartitionsInfo = Lists.newArrayList();
            for(RebalancePartitionInfoMap map: request.getRebalancePartitionInfoListList()) {
                rebalancePartitionsInfo.add(ProtoUtils.decodeRebalancePartitionInfoMap(map));
            }

            Cluster cluster = new ClusterMapper().readCluster(new StringReader(request.getClusterString()));

            boolean swapRO = request.getSwapRo();
            boolean changeClusterMetadata = request.getChangeClusterMetadata();
            boolean changeRebalanceState = request.getChangeRebalanceState();
            boolean rollback = request.getRollback();

            rebalancer.rebalanceStateChange(cluster,
                                            rebalancePartitionsInfo,
                                            swapRO,
                                            changeClusterMetadata,
                                            changeRebalanceState,
                                            rollback);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleRebalanceStateChange failed for request(" + request.toString()
                         + ")", e);
        }
        return response.build();
    }

    public VAdminProto.AsyncOperationStatusResponse handleRebalanceNode(VAdminProto.InitiateRebalanceNodeRequest request) {
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder();
        try {
            if(!voldemortConfig.isEnableRebalanceService())
                throw new VoldemortException("Rebalance service is not enabled for node: "
                                             + metadataStore.getNodeId());

            // We should be in rebalancing state to run this function
            if(!metadataStore.getServerState()
                             .equals(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)) {
                response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                         new VoldemortException("Voldemort server "
                                                                                + metadataStore.getNodeId()
                                                                                + " not in rebalancing state")));
                return response.build();
            }

            RebalancePartitionsInfo rebalanceStealInfo = ProtoUtils.decodeRebalancePartitionInfoMap(request.getRebalancePartitionInfo());

            int requestId = rebalancer.rebalanceNode(rebalanceStealInfo);

            response.setRequestId(requestId)
                    .setDescription(rebalanceStealInfo.toString())
                    .setStatus("Started rebalancing")
                    .setComplete(false);

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleRebalanceNode failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.GetROCurrentVersionDirResponse handleGetROCurrentVersionDir(VAdminProto.GetROCurrentVersionDirRequest request) {

        final List<String> storeNames = request.getStoreNameList();
        VAdminProto.GetROCurrentVersionDirResponse.Builder response = VAdminProto.GetROCurrentVersionDirResponse.newBuilder();

        try {
            for(String storeName: storeNames) {

                ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                       storeRepository,
                                                                       storeName);
                VAdminProto.ROStoreVersionDirMap storeResponse = VAdminProto.ROStoreVersionDirMap.newBuilder()
                                                                                                 .setStoreName(storeName)
                                                                                                 .setStoreDir(store.getCurrentDirPath())
                                                                                                 .build();
                response.addRoStoreVersions(storeResponse);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetROCurrentVersion failed for request(" + request.toString() + ")",
                         e);
        }
        return response.build();
    }

    public VAdminProto.GetROMaxVersionDirResponse handleGetROMaxVersionDir(VAdminProto.GetROMaxVersionDirRequest request) {
        final List<String> storeNames = request.getStoreNameList();
        VAdminProto.GetROMaxVersionDirResponse.Builder response = VAdminProto.GetROMaxVersionDirResponse.newBuilder();

        try {
            for(String storeName: storeNames) {

                ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                       storeRepository,
                                                                       storeName);
                File storeDirPath = new File(store.getStoreDirPath());

                if(!storeDirPath.exists())
                    throw new VoldemortException("Unable to locate the directory of the read-only store "
                                                 + storeName);

                File[] versionDirs = ReadOnlyUtils.getVersionDirs(storeDirPath);
                File[] kthDir = ReadOnlyUtils.findKthVersionedDir(versionDirs,
                                                                  versionDirs.length - 1,
                                                                  versionDirs.length - 1);

                VAdminProto.ROStoreVersionDirMap storeResponse = VAdminProto.ROStoreVersionDirMap.newBuilder()
                                                                                                 .setStoreName(storeName)
                                                                                                 .setStoreDir(kthDir[0].getAbsolutePath())
                                                                                                 .build();

                response.addRoStoreVersions(storeResponse);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetROMaxVersion failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    public VAdminProto.GetROStorageFormatResponse handleGetROStorageFormat(VAdminProto.GetROStorageFormatRequest request) {
        final List<String> storeNames = request.getStoreNameList();
        VAdminProto.GetROStorageFormatResponse.Builder response = VAdminProto.GetROStorageFormatResponse.newBuilder();

        try {
            for(String storeName: storeNames) {

                ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                       storeRepository,
                                                                       storeName);
                VAdminProto.ROStoreVersionDirMap storeResponse = VAdminProto.ROStoreVersionDirMap.newBuilder()
                                                                                                 .setStoreName(storeName)
                                                                                                 .setStoreDir(store.getReadOnlyStorageFormat()
                                                                                                                   .getCode())
                                                                                                 .build();

                response.addRoStoreVersions(storeResponse);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetROStorageFormat failed for request(" + request.toString() + ")",
                         e);
        }
        return response.build();
    }

    public VAdminProto.FailedFetchStoreResponse handleFailedFetch(VAdminProto.FailedFetchStoreRequest request) {
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
            return new FetchEntriesStreamRequestHandler(request,
                                                        metadataStore,
                                                        errorCodeMapper,
                                                        voldemortConfig,
                                                        storeRepository,
                                                        networkClassLoader,
                                                        stats);
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

    public VAdminProto.AsyncOperationListResponse handleAsyncOperationList(VAdminProto.AsyncOperationListRequest request) {
        VAdminProto.AsyncOperationListResponse.Builder response = VAdminProto.AsyncOperationListResponse.newBuilder();
        boolean showComplete = request.getShowComplete();
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
                                                     new VoldemortException("Voldemort server "
                                                                            + metadataStore.getNodeId()
                                                                            + " not in normal state while swapping store "
                                                                            + storeName
                                                                            + " with directory "
                                                                            + dir)));
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
                                                 + store.getCurrentVersionId() + " for store "
                                                 + storeName + " on node "
                                                 + metadataStore.getNodeId());
            } else {
                // Find the max version
                long maxVersion;
                File[] storeDirList = ReadOnlyUtils.getVersionDirs(new File(store.getStoreDirPath()));
                if(storeDirList == null || storeDirList.length == 0) {
                    throw new VoldemortException("Push version required since no version folders exist for store "
                                                 + storeName
                                                 + " on node "
                                                 + metadataStore.getNodeId());
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
        final HashMap<Integer, List<Integer>> replicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());
        final VoldemortFilter filter = request.hasFilter() ? getFilterFromRequest(request.getFilter(),
                                                                                  voldemortConfig,
                                                                                  networkClassLoader)
                                                          : new DefaultVoldemortFilter();
        final String storeName = request.getStore();

        final boolean optimize = request.hasOptimize() ? request.getOptimize() : false;

        final Cluster initialCluster = request.hasInitialCluster() ? new ClusterMapper().readCluster(new StringReader(request.getInitialCluster()))
                                                                  : null;

        int requestId = asyncService.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Fetch and update")
                                                                                                            .setStatus("Started");
        final StoreDefinition storeDef = metadataStore.getStoreDef(storeName);
        final boolean isReadOnlyStore = storeDef.getType()
                                                .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

        final RoutingStrategy oldStrategy = initialCluster != null ? new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                                        initialCluster)
                                                                  : null;

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
                                                                                   voldemortConfig.getClientMaxConnectionsPerNode());
                    try {
                        StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(storeRepository,
                                                                                                  storeName);

                        EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxWriteBytesPerSec());

                        if(isReadOnlyStore) {
                            ReadOnlyStorageEngine readOnlyStorageEngine = ((ReadOnlyStorageEngine) storageEngine);
                            String destinationDir = readOnlyStorageEngine.getCurrentDirPath();
                            logger.info("Fetching files for RO store '" + storeName
                                        + "' from node " + nodeId + " ( " + replicaToPartitionList
                                        + " )");
                            updateStatus("Fetching files for RO store '" + storeName
                                         + "' from node " + nodeId + " ( " + replicaToPartitionList
                                         + " )");

                            // TODO: Optimization to get rid of redundant
                            // copying of data which already exists on this
                            // node. This should simply copy and rename the
                            // existing replica file locally if they exist.
                            // Should not do rename only because then we won't
                            // be able to rollback

                            adminClient.fetchPartitionFiles(nodeId,
                                                            storeName,
                                                            replicaToPartitionList,
                                                            destinationDir,
                                                            readOnlyStorageEngine.getChunkedFileSet()
                                                                                 .getChunkIdToNumChunks()
                                                                                 .keySet());

                        } else {
                            logger.info("Fetching entries for RW store '" + storeName
                                        + "' from node " + nodeId + " ( " + replicaToPartitionList
                                        + " )");
                            updateStatus("Fetching entries for RW store '" + storeName
                                         + "' from node " + nodeId + " ( " + replicaToPartitionList
                                         + " ) ");

                            // Optimization to get rid of redundant copying of
                            // data which already exists on this node
                            HashMap<Integer, List<Integer>> optimizedReplicaToPartitionList = Maps.newHashMap();
                            if(oldStrategy != null && optimize && !storageEngine.isPartitionAware()
                               && voldemortConfig.getRebalancingOptimization()) {

                                for(Entry<Integer, List<Integer>> tuple: replicaToPartitionList.entrySet()) {
                                    List<Integer> partitionList = Lists.newArrayList();
                                    for(int partition: tuple.getValue()) {
                                        List<Integer> preferenceList = oldStrategy.getReplicatingPartitionList(partition);

                                        // If this node was already in the
                                        // preference list before, a copy of the
                                        // data will already exist - Don't copy
                                        // it!
                                        if(!RebalanceUtils.containsPreferenceList(initialCluster,
                                                                                  preferenceList,
                                                                                  metadataStore.getNodeId())) {
                                            partitionList.add(partition);
                                        }
                                    }

                                    if(partitionList.size() > 0) {
                                        optimizedReplicaToPartitionList.put(tuple.getKey(),
                                                                            partitionList);
                                    }
                                }

                                logger.info("After running RW level optimization - Fetching entries for RW store '"
                                            + storeName
                                            + "' from node "
                                            + nodeId
                                            + " ( "
                                            + optimizedReplicaToPartitionList + " )");
                                updateStatus("After running RW level optimization - Fetching entries for RW store '"
                                             + storeName
                                             + "' from node "
                                             + nodeId
                                             + " ( "
                                             + optimizedReplicaToPartitionList + " )");
                            } else {
                                optimizedReplicaToPartitionList.putAll(replicaToPartitionList);
                            }

                            if(optimizedReplicaToPartitionList.size() > 0) {
                                Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                                        storeName,
                                                                                                                        optimizedReplicaToPartitionList,
                                                                                                                        filter,
                                                                                                                        false,
                                                                                                                        initialCluster,
                                                                                                                        0);
                                long numTuples = 0;
                                while(running.get() && entriesIterator.hasNext()) {
                                    Pair<ByteArray, Versioned<byte[]>> entry = entriesIterator.next();

                                    ByteArray key = entry.getFirst();
                                    Versioned<byte[]> value = entry.getSecond();
                                    try {
                                        storageEngine.put(key, value, null);
                                    } catch(ObsoleteVersionException e) {
                                        // log and ignore
                                        logger.debug("Fetch and update threw Obsolete version exception. Ignoring");
                                    }

                                    throttler.maybeThrottle(key.length() + valueSize(value));
                                    if((numTuples % 10000) == 0 && numTuples > 0) {
                                        logger.info(numTuples + " entries copied from node "
                                                    + nodeId + " for store '" + storeName + "'");
                                        updateStatus(numTuples + " entries copied from node "
                                                     + nodeId + " for store '" + storeName + "'");
                                    }
                                    numTuples++;
                                }

                                logger.info("Completed fetching " + numTuples
                                            + " entries from node " + nodeId + " for store '"
                                            + storeName + "'");
                            } else {
                                logger.info("No entries to fetch from node " + nodeId
                                            + " for store '" + storeName + "'");
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
            final HashMap<Integer, List<Integer>> replicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());

            final boolean isReadWriteStore = metadataStore.getStoreDef(storeName)
                                                          .getType()
                                                          .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) != 0;

            if(!isReadWriteStore) {
                throw new VoldemortException("Cannot delete partitions for store " + storeName
                                             + " on node " + metadataStore.getNodeId()
                                             + " since it is not a RW store");
            }

            StorageEngine<ByteArray, byte[], byte[]> storageEngine = getStorageEngine(storeRepository,
                                                                                      storeName);
            VoldemortFilter filter = (request.hasFilter()) ? getFilterFromRequest(request.getFilter(),
                                                                                  voldemortConfig,
                                                                                  networkClassLoader)
                                                          : new DefaultVoldemortFilter();
            EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
            iterator = storageEngine.entries();
            long deleteSuccess = 0;
            logger.info("Deleting entries for RW store " + storeName + " from node "
                        + metadataStore.getNodeId() + " ( " + replicaToPartitionList + " )");

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                ByteArray key = entry.getFirst();
                Versioned<byte[]> value = entry.getSecond();
                throttler.maybeThrottle(key.length() + valueSize(value));
                if(RebalanceUtils.checkKeyBelongsToPartition(metadataStore.getNodeId(),
                                                             key.get(),
                                                             replicaToPartitionList,
                                                             request.hasInitialCluster() ? new ClusterMapper().readCluster(new StringReader(request.getInitialCluster()))
                                                                                        : metadataStore.getCluster(),
                                                             metadataStore.getStoreDef(storeName))
                   && filter.accept(key, value)) {
                    if(storageEngine.delete(key, value.getVersion())) {
                        deleteSuccess++;
                        if((deleteSuccess % 1000) == 0) {
                            logger.info(deleteSuccess + " entries deleted from node "
                                        + metadataStore.getNodeId() + " for store " + storeName);
                        }
                    }
                }
            }

            logger.info("Completed deletion of entries for RW store " + storeName + " from node "
                        + metadataStore.getNodeId() + " ( " + replicaToPartitionList + " )");

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
                throw new VoldemortException("Metadata Key passed '" + keyString
                                             + "' is not handled yet");
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
                        storageService.unregisterEngine(storeRepository.getStorageEngine(storeName),
                                                        false,
                                                        "slop");
                    } else {
                        // update stores list in metadata store
                        List<StoreDefinition> oldStoreDefList = metadataStore.getStoreDefList();
                        List<StoreDefinition> newStoreDefList = new ArrayList<StoreDefinition>();

                        for(StoreDefinition storeDef: oldStoreDefList) {
                            boolean isReadOnly = storeDef.getType()
                                                         .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
                            if(storeDef.isView()) {
                                if(storeDef.getViewTargetStoreName().compareTo(storeName) != 0) {
                                    newStoreDefList.add(storeDef);
                                } else {
                                    logger.info("Deleting view '" + storeDef.getName() + "'");
                                    storageService.unregisterEngine(storeRepository.getStorageEngine(storeDef.getName()),
                                                                    isReadOnly,
                                                                    storeDef.getType());
                                    logger.info("Successfully deleted view '" + storeDef.getName()
                                                + "'");
                                }
                            } else {
                                if(storeDef.getName().compareTo(storeName) != 0) {
                                    newStoreDefList.add(storeDef);
                                } else {
                                    logger.info("Deleting store '" + storeDef.getName() + "'");
                                    storageService.unregisterEngine(storeRepository.getStorageEngine(storeDef.getName()),
                                                                    isReadOnly,
                                                                    storeDef.getType());
                                    logger.info("Successfully deleted store '" + storeDef.getName()
                                                + "'");
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
                    logger.info("Adding new store '" + def.getName() + "'");
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
                    logger.info("Successfully added new store '" + def.getName() + "'");
                } else {
                    logger.error("Failure to add a store with the same name '" + def.getName()
                                 + "'");
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

}

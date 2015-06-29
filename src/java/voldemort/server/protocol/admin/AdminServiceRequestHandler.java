/*
 * 
 * Copyright 2008-2013 LinkedIn, Inc
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

import com.google.protobuf.Message;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.RebalanceTaskInfoMap;
import voldemort.client.protocol.pb.VAdminProto.VoldemortAdminRequest;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.common.nio.ByteBufferBackedInputStream;
import voldemort.common.nio.ByteBufferContainer;
import voldemort.common.service.SchedulerService;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.scheduler.slop.SlopPurgeJob;
import voldemort.server.storage.StorageService;
import voldemort.server.storage.prunejob.VersionedPutPruneJob;
import voldemort.server.storage.repairjob.RepairJob;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.StoreOperationFailureException;
import voldemort.store.backup.NativeBackupable;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.mysql.MysqlStorageEngine;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.StoreVersionManager;
import voldemort.store.readonly.chunk.ChunkedFileSet;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.stats.StreamingStats;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
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
    private final SchedulerService scheduler;
    private final Rebalancer rebalancer;
    private final VoldemortServer server;
    private FileFetcher fileFetcher;

    public AdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                      StorageService storageService,
                                      StoreRepository storeRepository,
                                      MetadataStore metadataStore,
                                      VoldemortConfig voldemortConfig,
                                      AsyncOperationService asyncService,
                                      SchedulerService scheduler,
                                      Rebalancer rebalancer,
                                      VoldemortServer server) {
        this.errorCodeMapper = errorCodeMapper;
        this.storageService = storageService;
        this.metadataStore = metadataStore;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.server = server;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
        this.asyncService = asyncService;
        this.scheduler = scheduler;
        this.rebalancer = rebalancer;
        setFetcherClass(voldemortConfig);
    }

    private void setFetcherClass(VoldemortConfig voldemortConfig) {
        if(voldemortConfig != null) {
            String className = voldemortConfig.getFileFetcherClass();
            if(className == null || className.trim().length() == 0) {
                this.fileFetcher = null;
            } else {
                try {
                    logger.info("Loading fetcher " + className);
                    Class<?> cls = Class.forName(className.trim());
                    this.fileFetcher = (FileFetcher) ReflectUtils.callConstructor(cls,
                                                                                  new Class<?>[] {
                                                                                          VoldemortConfig.class,
                                                                                          storageService.getDynThrottleLimit()
                                                                                                        .getClass() },
                                                                                  new Object[] {
                                                                                          voldemortConfig,
                                                                                          storageService.getDynThrottleLimit() });
                } catch(Exception e) {
                    throw new VoldemortException("Error loading file fetcher class " + className, e);
                }
            }
        } else {
            this.fileFetcher = null;
        }
    }

    @Override
    public StreamRequestHandler handleRequest(final DataInputStream inputStream,
                                              final DataOutputStream outputStream)
            throws IOException {
        return handleRequest(inputStream, outputStream, null);
    }

    @Override
    public StreamRequestHandler handleRequest(final DataInputStream inputStream,
                                              final DataOutputStream outputStream,
                                              final ByteBufferContainer container)
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
                                        handleSetMetadata(request.getUpdateMetadata()));
                break;
            case UPDATE_STORE_DEFINITIONS:
                ProtoUtils.writeMessage(outputStream,
                                        handleUpdateStoreDefinitions(request.getUpdateMetadata()));
                break;
            case UPDATE_METADATA_PAIR:
                ProtoUtils.writeMessage(outputStream,
                                        handleUpdateMetadataPair(request.getUpdateMetadataPair()));
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
            case LIST_SCHEDULED_JOBS:
                ProtoUtils.writeMessage(outputStream,
                                        handleListScheduledJobs(request.getListScheduledJobs()));
                break;
            case GET_SCHEDULED_JOB_STATUS:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetScheduledJobStatus(request.getGetScheduledJobStatus()));
                break;
            case STOP_SCHEDULED_JOB:
                ProtoUtils.writeMessage(outputStream,
                                        handleStopScheduledJob(request.getStopScheduledJob()));
                break;
            case ENABLE_SCHEDULED_JOB:
                ProtoUtils.writeMessage(outputStream,
                                        handleEnableScheduledJob(request.getEnableScheduledJob()));
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
                ProtoUtils.writeMessage(outputStream, handleFetchROStore(request.getFetchStore()));
                break;
            case SWAP_STORE:
                ProtoUtils.writeMessage(outputStream, handleSwapROStore(request.getSwapStore()));
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
            case GET_RO_STORAGE_FILE_LIST:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROStorageFileList(request.getGetRoStorageFileList()));
                break;
            case GET_RO_COMPRESSION_CODEC_LIST:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetROCompressionCodecList(request.getGetRoCompressionCodecList()));
                break;
            case FETCH_PARTITION_FILES:
                return handleFetchROPartitionFiles(request.getFetchPartitionFiles());
            case UPDATE_SLOP_ENTRIES:
                return handleUpdateSlopEntries(request.getUpdateSlopEntries());
            case FAILED_FETCH_STORE:
                ProtoUtils.writeMessage(outputStream,
                                        handleFailedROFetch(request.getFailedFetchStore()));
                break;
            case REBALANCE_STATE_CHANGE:
                ProtoUtils.writeMessage(outputStream,
                                        handleRebalanceStateChange(request.getRebalanceStateChange()));
                break;
            case DELETE_STORE_REBALANCE_STATE:
                ProtoUtils.writeMessage(outputStream,
                                        handleDeleteStoreRebalanceState(request.getDeleteStoreRebalanceState()));
                break;
            case SET_OFFLINE_STATE:
                ProtoUtils.writeMessage(outputStream,
                                        handleSetOfflineState(request.getSetOfflineState()));
                break;
            case REPAIR_JOB:
                ProtoUtils.writeMessage(outputStream, handleRepairJob(request.getRepairJob()));
                break;
            case PRUNE_JOB:
                ProtoUtils.writeMessage(outputStream, handlePruneJob(request.getPruneJob()));
                break;
            case SLOP_PURGE_JOB:
                ProtoUtils.writeMessage(outputStream, handleSlopPurgeJob(request.getSlopPurgeJob()));
                break;
            case NATIVE_BACKUP:
                ProtoUtils.writeMessage(outputStream, handleNativeBackup(request.getNativeBackup()));
                break;
            case RESERVE_MEMORY:
                ProtoUtils.writeMessage(outputStream,
                                        handleReserveMemory(request.getReserveMemory()));
                break;
            case GET_HA_SETTINGS:
                ProtoUtils.writeMessage(outputStream,
                                        handleGetHighAvailabilitySettings(request.getGetHaSettings()));
                break;
            case DISABLE_STORE_VERSION:
                ProtoUtils.writeMessage(outputStream,
                                        handleDisableStoreVersion(request.getDisableStoreVersion()));
                break;
            default:
                throw new VoldemortException("Unknown operation: " + request.getType());
        }

        return null;
    }

    private VAdminProto.DeleteStoreRebalanceStateResponse handleDeleteStoreRebalanceState(VAdminProto.DeleteStoreRebalanceStateRequest request) {
        VAdminProto.DeleteStoreRebalanceStateResponse.Builder response = VAdminProto.DeleteStoreRebalanceStateResponse.newBuilder();
        synchronized(rebalancer) {
            try {

                int nodeId = request.getNodeId();
                String storeName = request.getStoreName();

                logger.info("Removing rebalancing state for donor node " + nodeId + " and store "
                            + storeName + " from stealer node " + metadataStore.getNodeId());
                RebalanceTaskInfo info = metadataStore.getRebalancerState().find(nodeId);
                if(info == null) {
                    throw new VoldemortException("Could not find state for donor node " + nodeId);
                }

                List<Integer> partitionIds = info.getPartitionIds(storeName);
                if(partitionIds.size() == 0) {
                    throw new VoldemortException("Could not find state for donor node " + nodeId
                                                 + " and store " + storeName);
                }

                info.removeStore(storeName);
                logger.info("Removed rebalancing state for donor node " + nodeId + " and store "
                            + storeName + " from stealer node " + metadataStore.getNodeId());

                if(info.getPartitionStores().isEmpty()) {
                    metadataStore.deleteRebalancingState(info);
                    logger.info("Removed entire rebalancing state for donor node " + nodeId
                                + " from stealer node " + metadataStore.getNodeId());
                }
            } catch(VoldemortException e) {
                response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
                logger.error("handleDeleteStoreRebalanceState failed for request("
                                     + request.toString() + ")",
                             e);
            }
        }
        return response.build();
    }

    public VAdminProto.SetOfflineStateResponse handleSetOfflineState(VAdminProto.SetOfflineStateRequest request) {
        VAdminProto.SetOfflineStateResponse.Builder response = VAdminProto.SetOfflineStateResponse.newBuilder();

        try {
            Boolean setToOffline = request.getOfflineMode();
            logger.info("Setting OFFLINE_SERVER state to " + setToOffline.toString());

            if(setToOffline) {
                server.goOffline();
            } else {
                server.goOnline();
            }
            // TODO: deal with slop pushing here
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleSetOfflineState failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.RebalanceStateChangeResponse handleRebalanceStateChange(VAdminProto.RebalanceStateChangeRequest request) {
        VAdminProto.RebalanceStateChangeResponse.Builder response = VAdminProto.RebalanceStateChangeResponse.newBuilder();

        synchronized(rebalancer) {
            try {
                // Retrieve all values first
                List<RebalanceTaskInfo> rebalanceTaskInfo = Lists.newArrayList();
                for(RebalanceTaskInfoMap map: request.getRebalanceTaskListList()) {
                    rebalanceTaskInfo.add(ProtoUtils.decodeRebalanceTaskInfoMap(map));
                }

                Cluster cluster = new ClusterMapper().readCluster(new StringReader(request.getClusterString()));

                List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(request.getStoresString()));
                boolean swapRO = request.getSwapRo();
                boolean changeClusterMetadata = request.getChangeClusterMetadata();
                boolean changeRebalanceState = request.getChangeRebalanceState();
                boolean rollback = request.getRollback();

                rebalancer.rebalanceStateChange(cluster,
                                                storeDefs,
                                                rebalanceTaskInfo,
                                                swapRO,
                                                changeClusterMetadata,
                                                changeRebalanceState,
                                                rollback);
            } catch(VoldemortException e) {
                response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
                logger.error("handleRebalanceStateChange failed for request(" + request.toString()
                             + ")", e);
            }
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
            if(!metadataStore.getServerStateUnlocked()
                             .equals(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)) {
                response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                         new VoldemortException("Voldemort server "
                                                                                + metadataStore.getNodeId()
                                                                                + " not in rebalancing state")));
                return response.build();
            }

            RebalanceTaskInfo rebalanceStealInfo = ProtoUtils.decodeRebalanceTaskInfoMap(request.getRebalanceTaskInfo());

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

    public VAdminProto.FailedFetchStoreResponse handleFailedROFetch(VAdminProto.FailedFetchStoreRequest request) {
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

            logger.info("Deleting data from failed fetch for RO store '" + storeName
                        + "' and directory '" + storeDir + "'");
            // Lets delete the folder
            Utils.rm(new File(storeDir));
            logger.info("Successfully deleted data from failed fetch for RO store '" + storeName
                        + "' and directory '" + storeDir + "'");

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleFailedFetch failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    public VAdminProto.GetROStorageFileListResponse handleGetROStorageFileList(VAdminProto.GetROStorageFileListRequest request) {
        String storeName = request.getStoreName();
        VAdminProto.GetROStorageFileListResponse.Builder response = VAdminProto.GetROStorageFileListResponse.newBuilder();

        try {
            ReadOnlyStorageEngine store = getReadOnlyStorageEngine(metadataStore,
                                                                   storeRepository,
                                                                   storeName);
            ChunkedFileSet chunkedFileSet = store.getChunkedFileSet();
            response.addAllFileName(chunkedFileSet.getFileNames());
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetROStorageFileList failed for request(" + request.toString()
                         + ")", e);
        }
        return response.build();
    }

    public VAdminProto.GetROStorageCompressionCodecListResponse handleGetROCompressionCodecList(VAdminProto.GetROStorageCompressionCodecListRequest request) {
        logger.info("Received request for supported compression codecs");
        VAdminProto.GetROStorageCompressionCodecListResponse.Builder response = VAdminProto.GetROStorageCompressionCodecListResponse.newBuilder();

        ArrayList<String> supportedCodecs = new ArrayList<String>();
        supportedCodecs.add(this.voldemortConfig.getReadOnlyCompressionCodec());
        response.addAllCompressionCodecs(supportedCodecs);

        return response.build();
    }

    public StreamRequestHandler handleFetchROPartitionFiles(VAdminProto.FetchPartitionFilesRequest request) {
        return new FetchPartitionFileStreamRequestHandler(request,
                                                          metadataStore,
                                                          voldemortConfig,
                                                          storeRepository);
    }

    public StreamRequestHandler handleUpdateSlopEntries(VAdminProto.UpdateSlopEntriesRequest request) {
        return new UpdateSlopEntriesRequestHandler(request,
                                                   errorCodeMapper,
                                                   storeRepository,
                                                   voldemortConfig,
                                                   metadataStore);
    }

    public StreamRequestHandler handleFetchPartitionEntries(VAdminProto.FetchPartitionEntriesRequest request) {
        boolean fetchValues = request.hasFetchValues() && request.getFetchValues();
        boolean fetchOrphaned = request.hasFetchOrphaned() && request.getFetchOrphaned();
        StorageEngine<ByteArray, byte[], byte[]> storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                                                             request.getStore());

        if(fetchValues) {
            if(storageEngine.isPartitionScanSupported() && !fetchOrphaned)
                return new PartitionScanFetchEntriesRequestHandler(request,
                                                                   metadataStore,
                                                                   errorCodeMapper,
                                                                   voldemortConfig,
                                                                   storeRepository,
                                                                   networkClassLoader);
            else
                return new FullScanFetchEntriesRequestHandler(request,
                                                              metadataStore,
                                                              errorCodeMapper,
                                                              voldemortConfig,
                                                              storeRepository,
                                                              networkClassLoader);
        } else {
            if(storageEngine.isPartitionScanSupported() && !fetchOrphaned)
                return new PartitionScanFetchKeysRequestHandler(request,
                                                                metadataStore,
                                                                errorCodeMapper,
                                                                voldemortConfig,
                                                                storeRepository,
                                                                networkClassLoader);
            else
                return new FullScanFetchKeysRequestHandler(request,
                                                           metadataStore,
                                                           errorCodeMapper,
                                                           voldemortConfig,
                                                           storeRepository,
                                                           networkClassLoader);
        }
    }

    public StreamRequestHandler handleUpdatePartitionEntries(VAdminProto.UpdatePartitionEntriesRequest request) {
        StorageEngine<ByteArray, byte[], byte[]> storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                                                             request.getStore());

        if(request.hasOverwriteIfLatestTs() && request.getOverwriteIfLatestTs()) {
            // Resolve based on timestamp if specified.
            return new TimeBasedUpdatePartitionEntriesStreamRequestHandler(request,
                                                                           errorCodeMapper,
                                                                           voldemortConfig,
                                                                           storageEngine,
                                                                           storeRepository,
                                                                           networkClassLoader,
                                                                           metadataStore);
        } else {
            // else resort to vector clock based resolving..
            if(doesStorageEngineSupportMultiVersionPuts(storageEngine)) {
                return new BufferedUpdatePartitionEntriesStreamRequestHandler(request,
                                                                              errorCodeMapper,
                                                                              voldemortConfig,
                                                                              storageEngine,
                                                                              storeRepository,
                                                                              networkClassLoader,
                                                                              metadataStore);

            } else {
                return new UpdatePartitionEntriesStreamRequestHandler(request,
                                                                      errorCodeMapper,
                                                                      voldemortConfig,
                                                                      storageEngine,
                                                                      storeRepository,
                                                                      networkClassLoader,
                                                                      metadataStore);
            }
        }
    }

    private boolean doesStorageEngineSupportMultiVersionPuts(StorageEngine<ByteArray, byte[], byte[]> storageEngine) {
        if(!voldemortConfig.getMultiVersionStreamingPutsEnabled()
           || storageEngine instanceof MysqlStorageEngine
           || storageEngine instanceof SlopStorageEngine) {
            return false;
        }

        return true;
    }

    public VAdminProto.AsyncOperationListResponse handleAsyncOperationList(VAdminProto.AsyncOperationListRequest request) {

        VAdminProto.AsyncOperationListResponse.Builder response = VAdminProto.AsyncOperationListResponse.newBuilder();
        boolean showComplete = request.getShowComplete();
        try {
            logger.info("Retrieving list of async operations "
                        + ((showComplete) ? " [ including completed ids ]" : ""));
            List<Integer> asyncIds = asyncService.getAsyncOperationList(showComplete);
            logger.info("Retrieved list of async operations - " + asyncIds);
            response.addAllRequestIds(asyncIds);
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
            logger.info("Stopping async id " + requestId);
            asyncService.stopOperation(requestId);
            logger.info("Successfully stopped async id " + requestId);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAsyncOperationStop failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.ListScheduledJobsResponse handleListScheduledJobs(VAdminProto.ListScheduledJobsRequest request) {

        VAdminProto.ListScheduledJobsResponse.Builder response = VAdminProto.ListScheduledJobsResponse.newBuilder();
        try {
            logger.info("Retrieving list of scheduled jobs");
            List<String> jobIds = scheduler.getAllJobs();
            logger.info("Retrieved list of scheduled jobs - " + jobIds);
            response.addAllJobIds(jobIds);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleListScheduledJobs failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.GetScheduledJobStatusResponse handleGetScheduledJobStatus(VAdminProto.GetScheduledJobStatusRequest request) {

        VAdminProto.GetScheduledJobStatusResponse.Builder response = VAdminProto.GetScheduledJobStatusResponse.newBuilder();
        try {
            String jobId = request.getJobId();
            logger.info("Retrieving scheduled job status");
            boolean enabled = scheduler.getJobEnabled(jobId);
            logger.info("Retrieved scheduled job status - " + jobId);
            response.setEnabled(enabled);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleGetScheduledJobStatus failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.StopScheduledJobResponse handleStopScheduledJob(VAdminProto.StopScheduledJobRequest request) {

        VAdminProto.StopScheduledJobResponse.Builder response = VAdminProto.StopScheduledJobResponse.newBuilder();
        String jobId = request.getJobId();
        try {
            logger.info("Stopping job id " + jobId);
            scheduler.terminate(jobId);
            logger.info("Successfully stopped job id " + jobId);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleStopScheduledJob failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.EnableScheduledJobResponse handleEnableScheduledJob(VAdminProto.EnableScheduledJobRequest request) {

        VAdminProto.EnableScheduledJobResponse.Builder response = VAdminProto.EnableScheduledJobResponse.newBuilder();
        String jobId = request.getJobId();
        try {
            logger.info("Enabling job id " + jobId);
            scheduler.enable(jobId);
            logger.info("Successfully enabled job id " + jobId);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleEnableScheduledJob failed for request(" + request.toString() + ")",
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

            logger.info("Rolling back data for RO store '" + storeName + "' to version directory '"
                        + rollbackVersionDir + "'");
            store.rollback(rollbackVersionDir);
            logger.info("Successfully rolled back data for RO store '" + storeName
                        + "' to version directory '" + rollbackVersionDir + "'");
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleRollbackStore failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    public VAdminProto.RepairJobResponse handleRepairJob(VAdminProto.RepairJobRequest request) {
        VAdminProto.RepairJobResponse.Builder response = VAdminProto.RepairJobResponse.newBuilder();
        try {
            int requestId = asyncService.getUniqueRequestId();
            asyncService.submitOperation(requestId, new AsyncOperation(requestId, "Repair Job") {

                @Override
                public void operate() {
                    RepairJob job = storeRepository.getRepairJob();
                    if(job != null) {
                        if(job.getIsRunning().get()) {
                            logger.info("Repair job already running .. backing off.. ");
                            return;
                        }
                        logger.info("Starting the repair job now on ID : "
                                    + metadataStore.getNodeId());
                        job.run();
                    } else
                        logger.error("RepairJob is not initialized.");
                }

                @Override
                public void stop() {
                    status.setException(new VoldemortException("Repair job interrupted"));
                }
            });
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("Repair job failed for request : " + request.toString() + ")", e);
        }
        return response.build();
    }

    public VAdminProto.PruneJobResponse handlePruneJob(VAdminProto.PruneJobRequest request) {
        VAdminProto.PruneJobResponse.Builder response = VAdminProto.PruneJobResponse.newBuilder();
        try {
            int requestId = asyncService.getUniqueRequestId();
            final String storeName = request.getStoreName();
            asyncService.submitOperation(requestId, new AsyncOperation(requestId, "Prune Job-"
                                                                                  + storeName) {

                @Override
                public void operate() {
                    VersionedPutPruneJob job = storeRepository.getPruneJob();

                    if(job != null) {
                        if(job.getIsRunning().get()) {
                            logger.info("Prune job already running .. backing off.. ");
                            return;
                        }
                        job.setStoreName(storeName);
                        logger.info("Starting the prune job now on ID : "
                                    + metadataStore.getNodeId() + " for store " + storeName);
                        job.run();
                    } else {
                        logger.error("PruneJob is not initialized.");
                    }
                }

                @Override
                public void stop() {
                    status.setException(new VoldemortException("Prune job interrupted"));
                }
            });
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("Prune job failed for request : " + request.toString() + ")", e);
        }
        return response.build();
    }

    public VAdminProto.SlopPurgeJobResponse handleSlopPurgeJob(final VAdminProto.SlopPurgeJobRequest request) {
        VAdminProto.SlopPurgeJobResponse.Builder response = VAdminProto.SlopPurgeJobResponse.newBuilder();
        try {
            int requestId = asyncService.getUniqueRequestId();

            asyncService.submitOperation(requestId, new AsyncOperation(requestId, "SlopPurgeJob") {

                @Override
                public void operate() {
                    SlopPurgeJob job = storeRepository.getSlopPurgeJob();

                    if(job != null) {
                        if(job.getIsRunning().get()) {
                            logger.info(job.getJobName() + " already running .. backing off.. ");
                            return;
                        }
                        logger.info("Starting the " + job.getJobName() + " now on node ID : "
                                    + metadataStore.getNodeId());

                        job.setFilter(request.getFilterNodeIdsList(),
                                      request.hasFilterZoneId() ? request.getFilterZoneId()
                                                               : Zone.UNSET_ZONE_ID,
                                      request.getFilterStoreNamesList());
                        job.run();
                    } else {
                        logger.error("SlopPurgeJob is not initialized.");
                    }
                }

                @Override
                public void stop() {
                    status.setException(new VoldemortException("SlopPurgeJob interrupted"));
                }
            });
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("Slop Purge Job failed for request : " + request.toString() + ")", e);
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

        logger.info("Swapping RO store '" + storeName + "' to version directory '" + directory
                    + "'");
        store.swapFiles(directory);
        logger.info("Swapping swapped RO store '" + storeName + "' to version directory '"
                    + directory + "'");

        return currentDirPath;
    }

    public VAdminProto.SwapStoreResponse handleSwapROStore(VAdminProto.SwapStoreRequest request) {
        final String dir = request.getStoreDir();
        final String storeName = request.getStoreName();
        VAdminProto.SwapStoreResponse.Builder response = VAdminProto.SwapStoreResponse.newBuilder();

        if(!metadataStore.getServerStateUnlocked()
                         .equals(MetadataStore.VoldemortState.NORMAL_SERVER)
           && !metadataStore.getServerStateUnlocked()
                            .equals(MetadataStore.VoldemortState.OFFLINE_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server "
                                                                            + metadataStore.getNodeId()
                                                                            + " is neither in normal state nor in offline state while swapping store "
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

    public VAdminProto.AsyncOperationStatusResponse handleFetchROStore(VAdminProto.FetchStoreRequest request) {
        final String fetchUrl = request.getStoreDir();
        final String storeName = request.getStoreName();

        int requestId = asyncService.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Fetch store")
                                                                                                            .setStatus("started");
        try {
            if(!metadataStore.getReadOnlyFetchEnabledUnlocked()) {
                throw new VoldemortException("Read-only fetcher is disabled in "
                                             + metadataStore.getServerStateUnlocked()
                                             + " state on node " + metadataStore.getNodeId());
            }
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

                        logger.info("Started executing fetch of " + fetchUrl + " for RO store '"
                                    + storeName + "'");
                        updateStatus("0 MB copied at 0 MB/sec - 0 % complete");
                        try {
                            fileFetcher.setAsyncOperationStatus(status);
                            fetchDir = fileFetcher.fetch(fetchUrl, store.getStoreDirPath()
                                                                   + File.separator + "version-"
                                                                   + Long.toString(pushVersion));
                            if(fetchDir == null) {
                                String errorMessage = "File fetcher failed for "
                                                      + fetchUrl
                                                      + " and store '"
                                                      + storeName
                                                      + "' due to incorrect input path/checksum error";
                                updateStatus(errorMessage);
                                logger.error(errorMessage);
                                throw new VoldemortException(errorMessage);
                            } else {
                                String message = "Successfully executed fetch of " + fetchUrl
                                                 + " for RO store '" + storeName + "'";
                                updateStatus(message);
                                logger.info(message);
                            }
                        } catch(VoldemortException ve) {
                            String errorMessage = "File fetcher failed for " + fetchUrl
                                                  + " and store '" + storeName + "' Reason: \n"
                                                  + ve.getMessage();
                            updateStatus(errorMessage);
                            logger.error(errorMessage);
                            throw new VoldemortException(errorMessage);
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
        final List<Integer> partitionIds = request.getPartitionIdsList();
        final VoldemortFilter filter = request.hasFilter() ? getFilterFromRequest(request.getFilter(),
                                                                                  voldemortConfig,
                                                                                  networkClassLoader)
                                                          : new DefaultVoldemortFilter();
        final String storeName = request.getStore();

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
        final StreamingStats streamingStats = voldemortConfig.isJmxEnabled() ? storeRepository.getStreamingStats(storeName)
                                                                            : null;

        try {
            asyncService.submitOperation(requestId, new AsyncOperation(requestId,
                                                                       "Fetch and Update") {

                private final AtomicBoolean running = new AtomicBoolean(true);

                @Override
                public void stop() {
                    running.set(false);
                    logger.info("Stopping fetch and update for store " + storeName + " from node "
                                + nodeId + "( " + partitionIds + " )");
                }

                @Override
                public void operate() {
                    AdminClient adminClient = AdminClient.createTempAdminClient(voldemortConfig,
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
                                        + "' from node " + nodeId + " ( " + partitionIds + " )");
                            updateStatus("Fetching files for RO store '" + storeName
                                         + "' from node " + nodeId + " ( " + partitionIds + " )");

                            adminClient.readonlyOps.fetchPartitionFiles(nodeId,
                                                                        storeName,
                                                                        partitionIds,
                                                                        destinationDir,
                                                                        readOnlyStorageEngine.getChunkedFileSet()
                                                                                             .getChunkIdToNumChunks()
                                                                                             .keySet(),
                                                                        running);

                        } else {
                            logger.info("Fetching entries for RW store '" + storeName
                                        + "' from node " + nodeId + " ( " + partitionIds + " )");
                            updateStatus("Fetching entries for RW store '" + storeName
                                         + "' from node " + nodeId + " ( " + partitionIds + " ) ");

                            if(partitionIds.size() > 0) {
                                Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.bulkFetchOps.fetchEntries(nodeId,
                                                                                                                                     storeName,
                                                                                                                                     partitionIds,
                                                                                                                                     filter,
                                                                                                                                     false,
                                                                                                                                     initialCluster,
                                                                                                                                     0);
                                long numTuples = 0;
                                long startTime = System.currentTimeMillis();
                                long startNs = System.nanoTime();
                                while(running.get() && entriesIterator.hasNext()) {

                                    Pair<ByteArray, Versioned<byte[]>> entry = entriesIterator.next();
                                    if(streamingStats != null) {
                                        streamingStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                                         Utils.elapsedTimeNs(startNs,
                                                                                             System.nanoTime()));
                                    }
                                    ByteArray key = entry.getFirst();
                                    Versioned<byte[]> value = entry.getSecond();
                                    startNs = System.nanoTime();
                                    try {
                                        /**
                                         * TODO This also needs to be fixed to
                                         * use the atomic multi version puts
                                         */
                                        storageEngine.put(key, value, null);
                                    } catch(ObsoleteVersionException e) {
                                        // log and ignore
                                        if(logger.isDebugEnabled()) {
                                            logger.debug("Fetch and update threw Obsolete version exception. Ignoring");
                                        }
                                    } finally {
                                        if(streamingStats != null) {
                                            streamingStats.reportStreamingPut(Operation.UPDATE_ENTRIES);
                                            streamingStats.reportStorageTime(Operation.UPDATE_ENTRIES,
                                                                             Utils.elapsedTimeNs(startNs,
                                                                                                 System.nanoTime()));
                                        }
                                    }

                                    long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                                    throttler.maybeThrottle(key.length() + valueSize(value));
                                    if((numTuples % 100000) == 0 && numTuples > 0) {
                                        logger.info(numTuples + " entries copied from node "
                                                    + nodeId + " for store '" + storeName + "' in "
                                                    + totalTime + " seconds");
                                        updateStatus(numTuples + " entries copied from node "
                                                     + nodeId + " for store '" + storeName
                                                     + "' in " + totalTime + " seconds");
                                    }
                                    numTuples++;
                                }

                                long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                                if(running.get()) {
                                    logger.info("Completed fetching " + numTuples
                                                + " entries from node " + nodeId + " for store '"
                                                + storeName + "' in " + totalTime + " seconds");
                                } else {
                                    logger.info("Fetch and update stopped after fetching "
                                                + numTuples + " entries for node " + nodeId
                                                + " for store '" + storeName + "' in " + totalTime
                                                + " seconds");
                                }
                            } else {
                                logger.info("No entries to fetch from node " + nodeId
                                            + " for store '" + storeName + "'");
                            }
                        }

                    } finally {
                        adminClient.close();
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

    // TODO : Add ability to use partition scans
    public VAdminProto.DeletePartitionEntriesResponse handleDeletePartitionEntries(VAdminProto.DeletePartitionEntriesRequest request) {

        VAdminProto.DeletePartitionEntriesResponse.Builder response = VAdminProto.DeletePartitionEntriesResponse.newBuilder();
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;
        try {
            String storeName = request.getStore();
            final List<Integer> partitionsIds = request.getPartitionIdsList();

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
                        + metadataStore.getNodeId() + " ( " + storeName + " )");

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                ByteArray key = entry.getFirst();
                Versioned<byte[]> value = entry.getSecond();
                throttler.maybeThrottle(key.length() + valueSize(value));
                if(StoreRoutingPlan.checkKeyBelongsToNode(key.get(),
                                                          metadataStore.getNodeId(),
                                                          request.hasInitialCluster() ? new ClusterMapper().readCluster(new StringReader(request.getInitialCluster()))
                                                                                     : metadataStore.getCluster(),
                                                          metadataStore.getStoreDef(storeName))
                   && filter.accept(key, value)) {
                    if(storageEngine.delete(key, value.getVersion())) {
                        deleteSuccess++;
                        if((deleteSuccess % 10000) == 0) {
                            logger.info(deleteSuccess + " entries deleted from node "
                                        + metadataStore.getNodeId() + " for store " + storeName);
                        }
                    }
                }
            }

            logger.info("Completed deletion of entries for RW store " + storeName + " from node "
                        + metadataStore.getNodeId() + " ( " + partitionsIds + " )");

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

    public VAdminProto.UpdateMetadataResponse handleSetMetadata(VAdminProto.UpdateMetadataRequest request) {
        VAdminProto.UpdateMetadataResponse.Builder response = VAdminProto.UpdateMetadataResponse.newBuilder();

        try {
            ByteArray requestKey = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(requestKey.get(), "UTF-8");
            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                Versioned<byte[]> versionedValue = ProtoUtils.decodeVersioned(request.getVersioned());

                logger.info("Updating metadata for key '" + keyString + "'");
                ByteArray key = new ByteArray(ByteUtils.getBytes(keyString, "UTF-8"));
                metadataStore.validate(key,
                                  versionedValue,
                                  null);
                metadataStore.put(key,
                                  versionedValue,
                                  null);
                logger.info("Successfully updated metadata for key '" + keyString + "'");
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleUpdateMetadata failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.UpdateMetadataResponse handleUpdateStoreDefinitions(VAdminProto.UpdateMetadataRequest request) {
        VAdminProto.UpdateMetadataResponse.Builder response = VAdminProto.UpdateMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");
            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                Versioned<byte[]> versionedValue = ProtoUtils.decodeVersioned(request.getVersioned());

                // If updating stores.xml, go through each store entry and do a
                // corresponding put
                if(keyString.equals(MetadataStore.STORES_KEY)) {
                    metadataStore.updateStoreDefinitions(versionedValue);
                }
                logger.info("Successfully updated metadata for key '" + keyString + "'");
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleUpdateMetadata failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.UpdateMetadataPairResponse handleUpdateMetadataPair(VAdminProto.UpdateMetadataPairRequest request) {
        VAdminProto.UpdateMetadataPairResponse.Builder response = VAdminProto.UpdateMetadataPairResponse.newBuilder();
        try {
            ByteArray clusterKey = ProtoUtils.decodeBytes(request.getClusterKey());
            ByteArray storesKey = ProtoUtils.decodeBytes(request.getStoresKey());
            String clusterKeyString = ByteUtils.getString(clusterKey.get(), "UTF-8");
            String storesKeyString = ByteUtils.getString(storesKey.get(), "UTF-8");

            if(MetadataStore.METADATA_KEYS.contains(clusterKeyString)
               && MetadataStore.METADATA_KEYS.contains(storesKeyString)) {

                Versioned<byte[]> clusterVersionedValue = ProtoUtils.decodeVersioned(request.getClusterValue());
                Versioned<byte[]> storesVersionedValue = ProtoUtils.decodeVersioned(request.getStoresValue());

                metadataStore.writeLock.lock();
                try {
                    logger.info("Updating metadata for keys '" + clusterKeyString + "'" + " and '"
                                + storesKeyString + "'");
                    metadataStore.put(clusterKey, clusterVersionedValue, null);

                    // replace this with put
                    metadataStore.put(storesKey, storesVersionedValue, null);
                    // metadataStore.updateStoreDefinitions(storesVersionedValue);
                    logger.info("Successfully updated metadata for keys '" + clusterKeyString + "'"
                                + " and '" + storesKeyString + "'");
                } finally {
                    metadataStore.writeLock.unlock();
                }
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleUpdateMetadataPair failed for request(" + request.toString() + ")",
                         e);
        }
        return response.build();
    }

    public VAdminProto.GetMetadataResponse handleGetMetadata(VAdminProto.GetMetadataRequest request) {
        VAdminProto.GetMetadataResponse.Builder response = VAdminProto.GetMetadataResponse.newBuilder();

        try {
            ByteArray key = ProtoUtils.decodeBytes(request.getKey());
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            /**
             * GET can be done on any of the standard metadata keys
             * ('cluster.xml', 'server.state', 'node.id', ...) or any of the
             * store names.
             */
            if(MetadataStore.METADATA_KEYS.contains(keyString)
               || metadataStore.isValidStore(keyString)) {
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
            logger.info("Truncating data for store '" + storeName + "'");
            storageEngine.truncate();
            logger.info("Successfully truncated data for store '" + storeName + "'");
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleTruncateEntries failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.DeleteStoreResponse handleDeleteStore(VAdminProto.DeleteStoreRequest request) {
        VAdminProto.DeleteStoreResponse.Builder response = VAdminProto.DeleteStoreResponse.newBuilder();

        // don't try to delete a store in the middle of rebalancing
        if(!metadataStore.getServerStateUnlocked()
                         .equals(MetadataStore.VoldemortState.NORMAL_SERVER)
           && !metadataStore.getServerStateUnlocked()
                            .equals(MetadataStore.VoldemortState.OFFLINE_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server is neither in normal state nor in offline state")));
            return response.build();
        }

        try {
            String storeName = request.getStoreName();

            synchronized(lock) {

                if(storeRepository.hasLocalStore(storeName)) {
                    if(storeName.compareTo(SlopStorageEngine.SLOP_STORE_NAME) == 0) {
                        storageService.removeEngine(storeRepository.getStorageEngine(storeName),
                                                    false,
                                                    "slop",
                                                    true);
                    } else {
                        List<StoreDefinition> oldStoreDefList = metadataStore.getStoreDefList();

                        for(StoreDefinition storeDef: oldStoreDefList) {
                            boolean isReadOnly = storeDef.getType()
                                                         .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;
                            if(storeDef.isView()) {
                                if(storeDef.getViewTargetStoreName().compareTo(storeName) == 0) {
                                    logger.info("Deleting view '" + storeDef.getName() + "'");
                                    storageService.removeEngine(storeRepository.getStorageEngine(storeDef.getName()),
                                                                isReadOnly,
                                                                storeDef.getType(),
                                                                false);
                                    logger.info("Successfully deleted view '" + storeDef.getName()
                                                + "'");
                                }
                            } else {
                                if(storeDef.getName().compareTo(storeName) == 0) {
                                    logger.info("Deleting store '" + storeDef.getName() + "'");
                                    storageService.removeEngine(storeRepository.getStorageEngine(storeDef.getName()),
                                                                isReadOnly,
                                                                storeDef.getType(),
                                                                true);
                                    logger.info("Successfully deleted store '" + storeDef.getName()
                                                + "'");
                                }
                            }
                        }

                        try {
                            // Update the metadata
                            metadataStore.deleteStoreDefinition(storeName);
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

        // don't try to add a store when not in normal or offline state
        if(!metadataStore.getServerStateUnlocked()
                         .equals(MetadataStore.VoldemortState.NORMAL_SERVER)
           && !metadataStore.getServerStateUnlocked()
                            .equals(MetadataStore.VoldemortState.OFFLINE_SERVER)) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                     new VoldemortException("Voldemort server is neither in normal state nor in offline state")));
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
                    if(def.getReplicationFactor() > metadataStore.getCluster().getNumberOfNodes()) {
                        throw new StoreOperationFailureException("Cannot add a store whose replication factor ( "
                                                                 + def.getReplicationFactor()
                                                                 + " ) is greater than the number of nodes ( "
                                                                 + metadataStore.getCluster()
                                                                                .getNumberOfNodes()
                                                                 + " )");
                    }

                    logger.info("Adding new store '" + def.getName() + "'");
                    // open the store
                    StorageEngine<ByteArray, byte[], byte[]> engine = storageService.openStore(def);

                    // update stores list in metadata store (this also has the
                    // effect of updating the stores.xml file)
                    try {
                        metadataStore.addStoreDefinition(def);
                    } catch(Exception e) {
                        // rollback open store operation
                        boolean isReadOnly = ReadOnlyStorageConfiguration.TYPE_NAME.equals(def.getType());
                        storageService.removeEngine(engine, isReadOnly, def.getType(), true);
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
    @Override
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

    public VAdminProto.AsyncOperationStatusResponse handleNativeBackup(VAdminProto.NativeBackupRequest request) {
        final File backupDir = new File(request.getBackupDir());
        final boolean isIncremental = request.getIncremental();
        final boolean verifyFiles = request.getVerifyFiles();
        final String storeName = request.getStoreName();
        int requestId = asyncService.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Native backup")
                                                                                                            .setStatus("started");
        try {
            final StorageEngine storageEngine = getStorageEngine(storeRepository, storeName);
            final long start = System.currentTimeMillis();
            if(storageEngine instanceof NativeBackupable) {

                asyncService.submitOperation(requestId, new AsyncOperation(requestId,
                                                                           "Native backup") {

                    @Override
                    public void markComplete() {
                        long end = System.currentTimeMillis();
                        status.setStatus("Native backup completed in " + (end - start) + "ms");
                        status.setComplete(true);
                    }

                    @Override
                    public void operate() {
                        ((NativeBackupable) storageEngine).nativeBackup(backupDir,
                                                                        verifyFiles,
                                                                        isIncremental,
                                                                        status);
                    }

                    @Override
                    public void stop() {
                        status.setException(new VoldemortException("Fetcher interrupted"));
                    }
                });
            } else {
                response.setError(ProtoUtils.encodeError(errorCodeMapper,
                                                         new VoldemortException("Selected store is not native backupable")));
            }

        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleFetchStore failed for request(" + request.toString() + ")", e);
        }

        return response.build();
    }

    public VAdminProto.ReserveMemoryResponse handleReserveMemory(VAdminProto.ReserveMemoryRequest request) {
        VAdminProto.ReserveMemoryResponse.Builder response = VAdminProto.ReserveMemoryResponse.newBuilder();

        try {
            String storeName = request.getStoreName();
            long reserveMB = request.getSizeInMb();

            synchronized(lock) {
                if(storeRepository.hasLocalStore(storeName)) {

                    logger.info("Setting memory foot print of store '" + storeName + "' to "
                                + reserveMB + " MB");

                    // update store's metadata (this also has the effect of
                    // updating the stores.xml file)
                    List<StoreDefinition> storeDefList = metadataStore.getStoreDefList();

                    for(int i = 0; i < storeDefList.size(); i++) {
                        StoreDefinition storeDef = storeDefList.get(i);
                        if(!storeDef.isView() && storeDef.getName().equals(storeName)) {
                            StoreDefinition newStoreDef = new StoreDefinitionBuilder().setName(storeDef.getName())
                                                                                      .setType(storeDef.getType())
                                                                                      .setDescription(storeDef.getDescription())
                                                                                      .setOwners(storeDef.getOwners())
                                                                                      .setKeySerializer(storeDef.getKeySerializer())
                                                                                      .setValueSerializer(storeDef.getValueSerializer())
                                                                                      .setRoutingPolicy(storeDef.getRoutingPolicy())
                                                                                      .setRoutingStrategyType(storeDef.getRoutingStrategyType())
                                                                                      .setReplicationFactor(storeDef.getReplicationFactor())
                                                                                      .setPreferredReads(storeDef.getPreferredReads())
                                                                                      .setRequiredReads(storeDef.getRequiredReads())
                                                                                      .setPreferredWrites(storeDef.getPreferredWrites())
                                                                                      .setRequiredWrites(storeDef.getRequiredWrites())
                                                                                      .setRetentionPeriodDays(storeDef.getRetentionDays())
                                                                                      .setRetentionScanThrottleRate(storeDef.getRetentionScanThrottleRate())
                                                                                      .setZoneReplicationFactor(storeDef.getZoneReplicationFactor())
                                                                                      .setZoneCountReads(storeDef.getZoneCountReads())
                                                                                      .setZoneCountWrites(storeDef.getZoneCountWrites())
                                                                                      .setHintedHandoffStrategy(storeDef.getHintedHandoffStrategyType())
                                                                                      .setHintPrefListSize(storeDef.getHintPrefListSize())
                                                                                      .setMemoryFootprintMB(reserveMB)
                                                                                      .build();

                            storeDefList.set(i, newStoreDef);
                            storageService.updateStore(newStoreDef);
                            break;
                        }
                    }

                    // save the changes
                    try {
                        metadataStore.put(MetadataStore.STORES_KEY, storeDefList);
                    } catch(Exception e) {
                        throw new VoldemortException(e);
                    }

                } else {
                    logger.error("Failure to reserve memory. Store '" + storeName
                                 + "' does not exist");
                    throw new StoreOperationFailureException(String.format("Store '%s' does not exist on this server",
                                                                           storeName));
                }
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleReserveMemory failed for request(" + request.toString() + ")", e);
        }
        return response.build();
    }

    private Message handleDisableStoreVersion(VAdminProto.DisableStoreVersionRequest disableStoreVersion) {
        logger.info("Received DisableStoreVersionRequest: " + disableStoreVersion.toString());

        VAdminProto.DisableStoreVersionResponse.Builder response = VAdminProto.DisableStoreVersionResponse.newBuilder();

        String storeName = disableStoreVersion.getStoreName();
        Long version = disableStoreVersion.getPushVersion();

        try {
            StorageEngine storeToDisable = storeRepository.getStorageEngine(storeName);

            StoreVersionManager storeVersionManager = (StoreVersionManager)
                    storeToDisable.getCapability(StoreCapabilityType.DISABLE_STORE_VERSION);

            storeVersionManager.disableStoreVersion(version);
            response.setDisableSuccess(true)
                    .setDisablePersistenceSuccess(true)
                    .setInfo("The store '" + storeName + "' version " + version + " was successfully disabled.");
        } catch (PersistenceFailureException e) {
            response.setDisableSuccess(true)
                    .setDisablePersistenceSuccess(false)
                    .setInfo("The store '" + storeName + "' version " + version + " was disabled" +
                            " but the change could not be persisted and will thus remain in effect only" +
                            " until the next server restart. This is likely caused by the IO subsystem" +
                            " becoming read-only.");
        } catch (NoSuchCapabilityException e) {
            response.setDisableSuccess(false)
                    .setInfo("The store '" + storeName + "' does not support disabling versions!");
        } catch (NullPointerException e) {
            response.setDisableSuccess(false)
                    .setInfo("The store '" + storeName + "' does not exist!");
        } catch (Exception e) {
            logger.error("Got an unexpected exception while trying to disable store '" +
                    storeName + "' version " + version + ".", e);
            response.setDisableSuccess(false)
                    .setInfo("The store '" + storeName + "' version " + version +
                            " was not disabled because of an unexpected exception.");
        }

        logger.info("handleDisableStoreVersion returning response: " + response.getInfo());

        if (response.getDisableSuccess()) {
            // Then we also want to put the server in offline mode
            VAdminProto.SetOfflineStateRequest offlineStateRequest =
                    VAdminProto.SetOfflineStateRequest.newBuilder().setOfflineMode(true).build();
            handleSetOfflineState(offlineStateRequest);
        }

        return response.build();
    }

    private Message handleGetHighAvailabilitySettings(VAdminProto.GetHighAvailabilitySettingsRequest getHaSettings) {
        logger.info("Received GetHighAvailabilitySettingsRequest");

        VAdminProto.GetHighAvailabilitySettingsResponse.Builder response =
                VAdminProto.GetHighAvailabilitySettingsResponse.newBuilder();

        boolean highAvailabilityPushEnabled = voldemortConfig.isHighAvailabilityPushEnabled();
        response.setEnabled(highAvailabilityPushEnabled);
        if (highAvailabilityPushEnabled) {
            response.setClusterId(voldemortConfig.getHighAvailabilityPushClusterId());
            response.setMaxNodeFailure(voldemortConfig.getHighAvailabilityPushMaxNodeFailures());
            response.setLockPath(voldemortConfig.getHighAvailabilityPushLockPath());
            response.setLockImplementation(voldemortConfig.getHighAvailabilityPushLockImplementation());
        }

        return response.build();
    }

}
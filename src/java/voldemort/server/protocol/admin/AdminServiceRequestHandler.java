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
import java.io.IOException;
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
import voldemort.server.rebalance.Rebalancer;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * Protocol buffers implementation of a {@link RequestHandler}
 * 
 * @author afeinberg
 */
public class AdminServiceRequestHandler implements RequestHandler {

    private final static Logger logger = Logger.getLogger(AdminServiceRequestHandler.class);

    private final ErrorCodeMapper errorCodeMapper;
    private final MetadataStore metadataStore;
    private final StoreRepository storeRepository;
    private final NetworkClassLoader networkClassLoader;
    private final VoldemortConfig voldemortConfig;
    private final AsyncOperationRunner asyncRunner;
    private final Rebalancer rebalancer;

    public AdminServiceRequestHandler(ErrorCodeMapper errorCodeMapper,
                                      StoreRepository storeRepository,
                                      MetadataStore metadataStore,
                                      VoldemortConfig voldemortConfig,
                                      AsyncOperationRunner asyncRunner,
                                      Rebalancer rebalancer) {
        this.errorCodeMapper = errorCodeMapper;
        this.metadataStore = metadataStore;
        this.storeRepository = storeRepository;
        this.voldemortConfig = voldemortConfig;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
        this.asyncRunner = asyncRunner;
        this.rebalancer = rebalancer;
    }

    public void handleRequest(final DataInputStream inputStream, final DataOutputStream outputStream)
            throws IOException {
        // Another protocol buffers bug here, temp. work around
        VoldemortAdminRequest.Builder request = VoldemortAdminRequest.newBuilder();
        int size = inputStream.readInt();
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
                handleFetchPartitionEntries(request.getFetchPartitionEntries(), outputStream);
                break;
            case UPDATE_PARTITION_ENTRIES:
                handleUpdatePartitionEntries(request.getUpdatePartitionEntries(),
                                             inputStream,
                                             outputStream);
                break;
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
            default:
                throw new VoldemortException("Unkown operation " + request.getType());
        }

    }

    public void handleFetchPartitionEntries(VAdminProto.FetchPartitionEntriesRequest request,
                                            DataOutputStream outputStream) throws IOException {
        try {
            String storeName = request.getStore();
            StorageEngine<ByteArray, byte[]> storageEngine = getStorageEngine(storeName);
            RoutingStrategy routingStrategy = metadataStore.getRoutingStrategy(storageEngine.getName());
            EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
            List<Integer> partitionList = request.getPartitionsList();
            VoldemortFilter filter = (request.hasFilter()) ? getFilterFromRequest(request.getFilter())
                                                          : new DefaultVoldemortFilter();

            // boolean switch to fetchEntries/fetchKeys Only
            boolean fetchValues = request.hasFetchValues() && request.getFetchValues();

            if(fetchValues)
                fetchEntries(storageEngine,
                             outputStream,
                             partitionList,
                             routingStrategy,
                             filter,
                             throttler);
            else
                fetchKeys(storageEngine,
                          outputStream,
                          partitionList,
                          routingStrategy,
                          filter,
                          throttler);

            ProtoUtils.writeEndOfStream(outputStream);
        } catch(VoldemortException e) {
            VAdminProto.FetchPartitionEntriesResponse response = VAdminProto.FetchPartitionEntriesResponse.newBuilder()
                                                                                                          .setError(ProtoUtils.encodeError(errorCodeMapper,
                                                                                                                                           e))
                                                                                                          .build();

            ProtoUtils.writeMessage(outputStream, response);
            logger.error("handleFetchPartitionEntries failed for request(" + request.toString()
                         + ")", e);
        }
    }

    /**
     * FetchEntries fetches and return key/value entry.
     * <p>
     * For performance reason use storageEngine.keys() iterator to filter out
     * unwanted keys and then call storageEngine.get() for valid keys.
     * <p>
     * 
     * @param storageEngine
     * @param outputStream
     * @param partitionList
     * @param routingStrategy
     * @param filter
     * @param throttler
     * @throws IOException
     */
    private void fetchEntries(StorageEngine<ByteArray, byte[]> storageEngine,
                              DataOutputStream outputStream,
                              List<Integer> partitionList,
                              RoutingStrategy routingStrategy,
                              VoldemortFilter filter,
                              EventThrottler throttler) throws IOException {
        ClosableIterator<ByteArray> keyIterator = null;
        try {
            int counter = 0;
            int fetched = 0;
            long startTime = System.currentTimeMillis();
            keyIterator = storageEngine.keys();
            while(keyIterator.hasNext()) {
                ByteArray key = keyIterator.next();
                if(validPartition(key.get(), partitionList, routingStrategy)) {
                    for(Versioned<byte[]> value: storageEngine.get(key)) {
                        if(filter.accept(key, value)) {
                            fetched++;
                            VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();

                            VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                                  .setKey(ProtoUtils.encodeBytes(key))
                                                                                                  .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                                  .build();
                            response.setPartitionEntry(partitionEntry);

                            Message message = response.build();
                            ProtoUtils.writeMessage(outputStream, message);

                            if(throttler != null) {
                                throttler.maybeThrottle(entrySize(key, value));
                            }
                        }
                    }
                }
                // log progress
                counter++;
                if(0 == counter % 100000) {
                    long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                    if(logger.isDebugEnabled())
                        logger.debug("fetchEntries() scanned " + counter + " entries, fetched "
                                     + fetched + " entries for store:" + storageEngine.getName()
                                     + " partition:" + partitionList + " in " + totalTime + " s");
                }
            }
        } finally {
            if(null != keyIterator)
                keyIterator.close();
        }
    }

    private void fetchKeys(StorageEngine<ByteArray, byte[]> storageEngine,
                           DataOutputStream outputStream,
                           List<Integer> partitionList,
                           RoutingStrategy routingStrategy,
                           VoldemortFilter filter,
                           EventThrottler throttler) throws IOException {
        ClosableIterator<ByteArray> iterator = null;
        try {
            int counter = 0;
            int fetched = 0;
            long startTime = System.currentTimeMillis();
            iterator = storageEngine.keys();
            while(iterator.hasNext()) {
                ByteArray key = iterator.next();

                if(validPartition(key.get(), partitionList, routingStrategy)
                   && filter.accept(key, null)) {
                    VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
                    response.setKey(ProtoUtils.encodeBytes(key));

                    fetched++;
                    Message message = response.build();
                    ProtoUtils.writeMessage(outputStream, message);

                    if(throttler != null) {
                        throttler.maybeThrottle(key.length());
                    }
                }
                // log progress
                counter++;
                if(0 == counter % 100000) {
                    long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                    if(logger.isDebugEnabled())
                        logger.debug("fetchKeys() scanned " + counter + " keys, fetched " + fetched
                                     + " keys for store:" + storageEngine.getName() + " partition:"
                                     + partitionList + " in " + totalTime + " s");
                }
            }
        } finally {
            if(null != iterator)
                iterator.close();
        }
    }

    public void handleUpdatePartitionEntries(VAdminProto.UpdatePartitionEntriesRequest originalRequest,
                                             DataInputStream inputStream,
                                             DataOutputStream outputStream) throws IOException {
        VAdminProto.UpdatePartitionEntriesRequest request = originalRequest;
        VAdminProto.UpdatePartitionEntriesResponse.Builder response = VAdminProto.UpdatePartitionEntriesResponse.newBuilder();
        boolean continueReading = true;

        try {
            String storeName = request.getStore();
            StorageEngine<ByteArray, byte[]> storageEngine = getStorageEngine(storeName);
            VoldemortFilter filter = (request.hasFilter()) ? getFilterFromRequest(request.getFilter())
                                                          : new DefaultVoldemortFilter();

            int counter = 0;
            long startTime = System.currentTimeMillis();
            EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxWriteBytesPerSec());
            while(continueReading) {
                VAdminProto.PartitionEntry partitionEntry = request.getPartitionEntry();
                ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
                Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());

                if(filter.accept(key, value)) {
                    try {
                        storageEngine.put(key, value);

                    } catch(ObsoleteVersionException e) {
                        // log and ignore
                        logger.debug("updateEntries (Streaming put) threw ObsoleteVersionException, Ignoring.");
                    }

                    if(throttler != null) {
                        throttler.maybeThrottle(entrySize(key, value));
                    }
                }
                // log progress
                counter++;
                if(0 == counter % 100000) {
                    long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                    if(logger.isDebugEnabled())
                        logger.debug("updateEntries() updated " + counter + " entries for store:"
                                     + storageEngine.getName() + " in " + totalTime + " s");
                }

                int size = inputStream.readInt();
                if(size == -1)
                    continueReading = false;
                else {
                    byte[] input = new byte[size];
                    ByteUtils.read(inputStream, input);
                    VAdminProto.UpdatePartitionEntriesRequest.Builder builder = VAdminProto.UpdatePartitionEntriesRequest.newBuilder();
                    builder.mergeFrom(input);
                    request = builder.build();
                }
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleUpdatePartitionEntries failed for request(" + request.toString()
                         + ")", e);
        } finally {
            ProtoUtils.writeMessage(outputStream, response.build());
        }
    }

    public VAdminProto.AsyncOperationStatusResponse handleRebalanceNode(VAdminProto.InitiateRebalanceNodeRequest request) {
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder();
        try {
            if(!voldemortConfig.isEnableRebalanceService())
                throw new VoldemortException("Rebalance service is not enabled for node:"
                                             + metadataStore.getNodeId());

            RebalancePartitionsInfo rebalanceStealInfo = new RebalancePartitionsInfo(request.getStealerId(),
                                                                                     request.getDonorId(),
                                                                                     request.getPartitionsList(),
                                                                                     request.getDeletePartitionsList(),
                                                                                     request.getUnbalancedStoreList(),
                                                                                     request.getAttempt());

            int requestId = rebalancer.rebalanceLocalNode(rebalanceStealInfo);

            response.setRequestId(requestId)
                    .setDescription(rebalanceStealInfo.toString())
                    .setStatus("started")
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
            response.addAllRequestIds(asyncRunner.getAsyncOperationList(showComplete));
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
            asyncRunner.stopOperation(requestId);
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleAsyncOperationStop failed for request(" + request.toString() + ")",
                         e);
        }

        return response.build();
    }

    public VAdminProto.AsyncOperationStatusResponse handleFetchAndUpdate(VAdminProto.InitiateFetchAndUpdateRequest request) {
        final int nodeId = request.getNodeId();
        final List<Integer> partitions = request.getPartitionsList();
        final VoldemortFilter filter = request.hasFilter() ? getFilterFromRequest(request.getFilter())
                                                          : new DefaultVoldemortFilter();
        final String storeName = request.getStore();

        int requestId = asyncRunner.getUniqueRequestId();
        VAdminProto.AsyncOperationStatusResponse.Builder response = VAdminProto.AsyncOperationStatusResponse.newBuilder()
                                                                                                            .setRequestId(requestId)
                                                                                                            .setComplete(false)
                                                                                                            .setDescription("Fetch and update")
                                                                                                            .setStatus("started");

        try {
            asyncRunner.submitOperation(requestId,
                                        new AsyncOperation(requestId, "Fetch and Update") {

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
                                                    StorageEngine<ByteArray, byte[]> storageEngine = getStorageEngine(storeName);
                                                    Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                                                            storeName,
                                                                                                                                            partitions,
                                                                                                                                            filter);
                                                    updateStatus("Initated fetchPartitionEntries");
                                                    EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxWriteBytesPerSec());
                                                    for(long i = 0; running.get()
                                                                    && entriesIterator.hasNext(); i++) {
                                                        Pair<ByteArray, Versioned<byte[]>> entry = entriesIterator.next();

                                                        try {
                                                            storageEngine.put(entry.getFirst(),
                                                                              entry.getSecond());
                                                        } catch(ObsoleteVersionException e) {
                                                            // log and ignore
                                                            logger.debug("migratePartition threw ObsoleteVersionException, Ignoring.");
                                                        }

                                                        throttler.maybeThrottle(entrySize(entry.getFirst(),
                                                                                          entry.getSecond()));

                                                        if((i % 1000) == 0) {
                                                            updateStatus(i + " entries processed");
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
            AsyncOperationStatus operationStatus = asyncRunner.getOperationStatus(requestId);
            boolean requestComplete = asyncRunner.isComplete(requestId);
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
            List<Integer> partitions = request.getPartitionsList();
            StorageEngine<ByteArray, byte[]> storageEngine = getStorageEngine(storeName);
            VoldemortFilter filter = (request.hasFilter()) ? getFilterFromRequest(request.getFilter())
                                                          : new DefaultVoldemortFilter();
            RoutingStrategy routingStrategy = metadataStore.getRoutingStrategy(storageEngine.getName());

            EventThrottler throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
            iterator = storageEngine.entries();
            int deleteSuccess = 0;

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if(checkKeyBelongsToDeletePartition(entry.getFirst().get(),
                                                    partitions,
                                                    routingStrategy)
                   && filter.accept(entry.getFirst(), entry.getSecond())) {
                    if(storageEngine.delete(entry.getFirst(), entry.getSecond().getVersion()))
                        deleteSuccess++;
                    if(throttler != null)
                        throttler.maybeThrottle(entrySize(entry.getFirst(), entry.getSecond()));
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
                                  versionedValue);
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
                List<Versioned<byte[]>> versionedList = metadataStore.get(key);
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
            StorageEngine<ByteArray, byte[]> storageEngine = getStorageEngine(storeName);

            storageEngine.truncate();
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(errorCodeMapper, e));
            logger.error("handleTruncateEntries failed for request(" + request.toString() + ")", e);
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
        throw new VoldemortException("Non-blocking server not supported for AdminServiceRequestHandler");
    }

    /* Private helper methods */
    private VoldemortFilter getFilterFromRequest(VAdminProto.VoldemortFilter request) {
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

    private static int entrySize(ByteArray key, Versioned<byte[]> value) {
        return key.get().length + value.getValue().length
               + ((VectorClock) value.getVersion()).sizeInBytes() + 1;
    }

    private StorageEngine<ByteArray, byte[]> getStorageEngine(String storeName) {
        StorageEngine<ByteArray, byte[]> storageEngine = storeRepository.getStorageEngine(storeName);

        if(storageEngine == null) {
            throw new VoldemortException("No store named '" + storeName + "'.");
        }

        return storageEngine;
    }

    protected boolean validPartition(byte[] key,
                                     List<Integer> partitionList,
                                     RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);

        for(int p: partitionList) {
            if(keyPartitions.contains(p)) {
                return true;
            }
        }

        return false;
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

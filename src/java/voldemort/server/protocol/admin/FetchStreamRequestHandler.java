/*
 * Copyright 2013 LinkedIn, Inc
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.cluster.Cluster;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.StoreInstance;
import voldemort.utils.Time;
import voldemort.xml.ClusterMapper;

import com.google.protobuf.Message;

/**
 * Base class for all key/entry stream fetching handlers.
 * 
 */
public abstract class FetchStreamRequestHandler implements StreamRequestHandler {

    protected final VAdminProto.FetchPartitionEntriesRequest request;

    protected final ErrorCodeMapper errorCodeMapper;

    protected final Cluster initialCluster;

    protected final EventThrottler throttler;

    protected HashMap<Integer, List<Integer>> replicaToPartitionList;

    protected final VoldemortFilter filter;

    protected final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    protected final StreamingStats streamStats;

    protected boolean isJmxEnabled;

    protected final StreamingStats.Operation operation;

    protected long scanned; // Read from disk.

    protected long fetched; // Returned to caller.

    protected final long recordsPerPartition;

    protected final long startTimeMs;

    protected final Logger logger = Logger.getLogger(getClass());

    protected int nodeId;

    protected final StoreDefinition storeDef;

    protected boolean fetchOrphaned;

    protected final StoreInstance storeInstance;

    protected FetchStreamRequestHandler(VAdminProto.FetchPartitionEntriesRequest request,
                                        MetadataStore metadataStore,
                                        ErrorCodeMapper errorCodeMapper,
                                        VoldemortConfig voldemortConfig,
                                        StoreRepository storeRepository,
                                        NetworkClassLoader networkClassLoader,
                                        StreamingStats.Operation operation) {
        this.nodeId = metadataStore.getNodeId();
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        if(request.getReplicaToPartitionList() != null)
            this.replicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());
        this.storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                         request.getStore());
        if(voldemortConfig.isJmxEnabled()) {
            this.streamStats = storeRepository.getStreamingStats(this.storageEngine.getName());
        } else {
            this.streamStats = null;
        }

        this.operation = operation;
        this.storeDef = getStoreDef(request.getStore(), metadataStore);
        if(request.hasInitialCluster()) {
            this.initialCluster = new ClusterMapper().readCluster(new StringReader(request.getInitialCluster()));
        } else {
            this.initialCluster = metadataStore.getCluster();
        }
        this.storeInstance = new StoreInstance(this.initialCluster, this.storeDef);

        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        if(request.hasFilter()) {
            this.filter = AdminServiceRequestHandler.getFilterFromRequest(request.getFilter(),
                                                                          voldemortConfig,
                                                                          networkClassLoader);
        } else {
            this.filter = new DefaultVoldemortFilter();
        }
        this.startTimeMs = System.currentTimeMillis();
        this.scanned = 0;

        if(request.hasRecordsPerPartition() && request.getRecordsPerPartition() > 0) {
            this.recordsPerPartition = request.getRecordsPerPartition();
        } else {
            this.recordsPerPartition = 0;
        }
        this.fetchOrphaned = request.hasFetchOrphaned() && request.getFetchOrphaned();
    }

    private StoreDefinition getStoreDef(String store, MetadataStore metadataStore) {
        StoreDefinition def = null;
        if(SystemStoreConstants.isSystemStore(store)) {
            def = SystemStoreConstants.getSystemStoreDef(store);
        } else {
            def = metadataStore.getStoreDef(request.getStore());
        }
        return def;
    }

    @Override
    public final StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    @Override
    public void close(DataOutputStream outputStream) throws IOException {
        logger.info("Successfully scanned " + scanned + " tuples, fetched " + fetched
                    + " tuples for store '" + storageEngine.getName() + "' in "
                    + ((System.currentTimeMillis() - startTimeMs) / 1000) + " s");

        ProtoUtils.writeEndOfStream(outputStream);
    }

    @Override
    public final void handleError(DataOutputStream outputStream, VoldemortException e)
            throws IOException {
        VAdminProto.FetchPartitionEntriesResponse response = VAdminProto.FetchPartitionEntriesResponse.newBuilder()
                                                                                                      .setError(ProtoUtils.encodeError(errorCodeMapper,
                                                                                                                                       e))
                                                                                                      .build();

        ProtoUtils.writeMessage(outputStream, response);
        logger.error("handleFetchPartitionEntries failed for request(" + request.toString() + ")",
                     e);
    }

    /**
     * Progress info message
     * 
     * @param tag Message that precedes progress info. Indicate 'keys' or
     *        'entries'.
     */
    protected void progressInfoMessage(final String tag) {
        if(logger.isInfoEnabled()) {
            long totalTimeS = (System.currentTimeMillis() - startTimeMs) / Time.MS_PER_SECOND;

            logger.info(tag + " : scanned " + scanned + " and fetched " + fetched + " for store '"
                        + storageEngine.getName() + "' replicaToPartitionList:"
                        + replicaToPartitionList + " in " + totalTimeS + " s");
        }
    }

    /**
     * Account for item being scanned.
     * 
     * @param itemTag mad libs style string to insert into progress message.
     * 
     */
    protected void accountForScanProgress(String itemTag) {
        scanned++;
        if(0 == scanned % STAT_RECORDS_INTERVAL) {
            progressInfoMessage("Fetch " + itemTag + " (progress)");
        }
    }

    /**
     * Helper method to send message on outputStream and account for network
     * time stats.
     * 
     * @param outputStream
     * @param message
     * @throws IOException
     */
    protected void sendMessage(DataOutputStream outputStream, Message message) throws IOException {
        long startNs = System.nanoTime();
        ProtoUtils.writeMessage(outputStream, message);
        if(streamStats != null) {
            streamStats.reportNetworkTime(operation, System.nanoTime() - startNs);
        }
    }

    /**
     * Helper method to track storage operations & time via StreamingStats.
     * 
     * @param startNs
     */
    protected void reportStorageOpTime(long startNs) {
        if(streamStats != null) {
            streamStats.reportStreamingScan(operation);
            streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
        }
    }

}

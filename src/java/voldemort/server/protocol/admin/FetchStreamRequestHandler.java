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
import voldemort.xml.ClusterMapper;

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

    protected long counter;

    protected long skipRecords;

    protected int fetched;

    protected final long startTime;

    protected final Logger logger = Logger.getLogger(getClass());

    protected int nodeId;

    protected StoreDefinition storeDef;

    protected boolean fetchOrphaned;

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
        this.throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        if(request.hasFilter()) {
            this.filter = AdminServiceRequestHandler.getFilterFromRequest(request.getFilter(),
                                                                          voldemortConfig,
                                                                          networkClassLoader);
        } else {
            this.filter = new DefaultVoldemortFilter();
        }
        this.startTime = System.currentTimeMillis();
        this.counter = 0;

        this.skipRecords = 1;
        if(request.hasSkipRecords() && request.getSkipRecords() >= 0) {
            this.skipRecords = request.getSkipRecords() + 1;
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

    public final StreamRequestDirection getDirection() {
        return StreamRequestDirection.WRITING;
    }

    public void close(DataOutputStream outputStream) throws IOException {
        logger.info("Successfully scanned " + counter + " tuples, fetched " + fetched
                    + " tuples for store '" + storageEngine.getName() + "' in "
                    + ((System.currentTimeMillis() - startTime) / 1000) + " s");

        ProtoUtils.writeEndOfStream(outputStream);
    }

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

}

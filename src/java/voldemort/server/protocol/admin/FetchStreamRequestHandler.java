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
import voldemort.store.stats.StreamStats;
import voldemort.store.stats.StreamStats.Handle;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.xml.ClusterMapper;

public abstract class FetchStreamRequestHandler implements StreamRequestHandler {

    protected final VAdminProto.FetchPartitionEntriesRequest request;

    protected final ErrorCodeMapper errorCodeMapper;

    protected final Cluster initialCluster;

    protected final EventThrottler throttler;

    protected final HashMap<Integer, List<Integer>> replicaToPartitionList;

    protected final VoldemortFilter filter;

    protected final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    protected long counter;

    protected long skipRecords;

    protected int fetched;

    protected final long startTime;

    protected final Handle handle;

    protected final StreamStats stats;

    protected final Logger logger = Logger.getLogger(getClass());

    protected int nodeId;

    protected StoreDefinition storeDef;

    protected FetchStreamRequestHandler(VAdminProto.FetchPartitionEntriesRequest request,
                                        MetadataStore metadataStore,
                                        ErrorCodeMapper errorCodeMapper,
                                        VoldemortConfig voldemortConfig,
                                        StoreRepository storeRepository,
                                        NetworkClassLoader networkClassLoader,
                                        StreamStats stats,
                                        StreamStats.Operation operation) {
        this.nodeId = metadataStore.getNodeId();
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        this.replicaToPartitionList = ProtoUtils.decodePartitionTuple(request.getReplicaToPartitionList());
        this.stats = stats;
        this.handle = stats.makeHandle(operation, replicaToPartitionList);
        this.storageEngine = AdminServiceRequestHandler.getStorageEngine(storeRepository,
                                                                         request.getStore());
        this.storeDef = metadataStore.getStoreDef(request.getStore());
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

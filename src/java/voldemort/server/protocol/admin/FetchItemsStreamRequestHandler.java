package voldemort.server.protocol.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.StoreInstance;
import voldemort.utils.Utils;

/**
 * Base class for key/entry stream fetching handlers that do not rely on PID
 * layout.
 * 
 */
public abstract class FetchItemsStreamRequestHandler extends FetchStreamRequestHandler {

    protected final ClosableIterator<ByteArray> keyIterator;

    // PartitionId to count of fetches on that partition.
    protected Map<Integer, Long> partitionFetches;

    public FetchItemsStreamRequestHandler(FetchPartitionEntriesRequest request,
                                          MetadataStore metadataStore,
                                          ErrorCodeMapper errorCodeMapper,
                                          VoldemortConfig voldemortConfig,
                                          StoreRepository storeRepository,
                                          NetworkClassLoader networkClassLoader,
                                          StreamingStats.Operation operation) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader,
              operation);

        this.keyIterator = storageEngine.keys();

        this.partitionFetches = new HashMap<Integer, Long>();
        for(Integer replicaType: replicaToPartitionList.keySet()) {
            if(replicaToPartitionList.get(replicaType) != null) {
                for(Integer partitionId: replicaToPartitionList.get(replicaType)) {
                    this.partitionFetches.put(partitionId, new Long(0));
                }
            }
        }
    }

    /**
     * Given the key, figures out which partition on the local node hosts the
     * key based on contents of the "replica to partition list" data structure.
     * 
     * @param key
     * @return
     */
    private Integer getKeyPartitionId(byte[] key) {
        StoreInstance storeInstance = new StoreInstance(initialCluster, storeDef);
        Integer keyPartitionId = null;
        for(Integer partitionId: storeInstance.getReplicationPartitionList(key)) {
            for(Map.Entry<Integer, List<Integer>> rtps: replicaToPartitionList.entrySet()) {
                if(rtps.getValue().contains(partitionId)) {
                    keyPartitionId = partitionId;
                    break;
                }
            }
        }
        Utils.notNull(keyPartitionId);
        return keyPartitionId;
    }

    /**
     * Determines if the key is needed. To be 'needed', a key must (i) belong to
     * a partition being requested and (ii) be necessary to meet
     * recordsPerPartition constraint, if any.
     * 
     * @param nodeId
     * @param key
     * @param replicaToPartitionList
     * @param cluster
     * @param storeDef
     * @return true iff key is needed.
     */
    protected boolean keyIsNeeded(byte[] key) {
        if(!StoreInstance.checkKeyBelongsToPartition(nodeId,
                                                     key,
                                                     replicaToPartitionList,
                                                     initialCluster,
                                                     storeDef)) {
            return false;
        }

        if(recordsPerPartition <= 0) {
            return true;
        }

        Integer keyPartitionId = getKeyPartitionId(key);
        Long partitionFetch = partitionFetches.get(keyPartitionId);
        Utils.notNull(partitionFetch);

        if(partitionFetch >= recordsPerPartition) {
            return false;
        }

        return true;
    }

    /**
     * Account for key being fetched.
     * 
     * @param key
     */
    protected void keyFetched(byte[] key) {
        fetched++;
        if(streamStats != null) {
            streamStats.reportStreamingFetch(operation);
        }

        if(recordsPerPartition <= 0) {
            return;
        }

        Integer keyPartitionId = getKeyPartitionId(key);
        Long partitionFetch = partitionFetches.get(keyPartitionId);
        Utils.notNull(partitionFetch);

        partitionFetches.put(keyPartitionId, partitionFetch + 1);
    }

    /**
     * True iff enough partitions have been fetched relative to
     * recordsPerPartition value.
     * 
     * @param partitionFetched Records fetched for current partition
     * @return
     */
    protected boolean fetchedEnough() {
        if(recordsPerPartition <= 0) {
            return false;
        }

        for(Map.Entry<Integer, Long> partitionFetch: partitionFetches.entrySet()) {
            if(partitionFetch.getValue() < recordsPerPartition) {
                return false;
            }
        }
        return true;
    }
}

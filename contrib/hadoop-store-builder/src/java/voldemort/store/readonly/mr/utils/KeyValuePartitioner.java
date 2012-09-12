package voldemort.store.readonly.mr.utils;

import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

public class KeyValuePartitioner {

    public int getPartition(byte[] keyBytes,
                            byte[] valueBytes,
                            boolean saveKeys,
                            boolean reducerPerBucket,
                            StoreDefinition storeDef,
                            int numChunks,
                            int numReduceTasks) {
        int partitionId = ByteUtils.readInt(valueBytes, ByteUtils.SIZE_OF_INT);
        int chunkId = ReadOnlyUtils.chunk(keyBytes, numChunks);
        if(saveKeys) {
            int replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                        2 * ByteUtils.SIZE_OF_INT,
                                                        ByteUtils.SIZE_OF_BYTE);
            if(reducerPerBucket) {
                return (partitionId * storeDef.getReplicationFactor() + replicaType)
                       % numReduceTasks;
            } else {
                return ((partitionId * storeDef.getReplicationFactor() * numChunks)
                        + (replicaType * numChunks) + chunkId)
                       % numReduceTasks;
            }
        } else {
            if(reducerPerBucket) {
                return partitionId % numReduceTasks;
            } else {
                return (partitionId * numChunks + chunkId) % numReduceTasks;
            }

        }
    }

}

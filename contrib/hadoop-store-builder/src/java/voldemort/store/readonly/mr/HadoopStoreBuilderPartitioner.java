package voldemort.store.readonly.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Partitioner;

import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce
 * 
 * @author bbansal, jay
 * 
 */
public class HadoopStoreBuilderPartitioner extends HadoopStoreBuilderBase implements
        Partitioner<BytesWritable, BytesWritable> {

    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int nodeId = ByteUtils.readInt(value.get(), 0);
        int chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());
        return (nodeId * getNumChunks() + chunkId % numReduceTasks);
    }

}

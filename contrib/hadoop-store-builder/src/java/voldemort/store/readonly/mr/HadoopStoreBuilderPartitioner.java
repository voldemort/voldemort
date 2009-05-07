package voldemort.store.readonly.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import voldemort.utils.ByteUtils;

/**
 * A Partitioner that pulls the voldemort node id from the first four bytes of
 * the value
 * 
 * @author bbansal
 * 
 */
public class HadoopStoreBuilderPartitioner extends HashPartitioner<BytesWritable, BytesWritable> {

    @Override
    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int nodeId = ByteUtils.readInt(value.get(), 0);
        return (nodeId) % numReduceTasks;
    }
}

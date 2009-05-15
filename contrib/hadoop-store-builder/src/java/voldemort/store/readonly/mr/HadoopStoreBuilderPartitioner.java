package voldemort.store.readonly.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce
 * 
 * @author bbansal, jay
 * 
 */
public class HadoopStoreBuilderPartitioner implements Partitioner<BytesWritable, BytesWritable> {

    private int numChunks;

    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int nodeId = ByteUtils.readInt(value.get(), 0);
        int chunkId = ReadOnlyUtils.chunk(key.get(), numChunks);
        System.out.println("nodeId = " + nodeId + ", chunkId = " + chunkId + ", partition = "
                           + (chunkId * numChunks + nodeId) % numReduceTasks
                           + ", numReduceTasks = " + numReduceTasks);
        return (chunkId * numChunks + nodeId) % numReduceTasks;
    }

    public void configure(JobConf job) {
        this.numChunks = job.getInt("num.chunks", -1);
        if(this.numChunks < 1)
            throw new VoldemortException("num.chunks not specified in the job conf.");
    }

}

package voldemort.contrib.batchindexer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class ReadOnlyBatchIndexPartitioner extends HashPartitioner<BytesWritable, BytesWritable> {

    @Override
    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        // The partition id is first 4 bytes in the value.
        DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(value.get()));
        int nodeId = -2;
        try {
            nodeId = buffer.readInt();
        } catch(IOException e) {
            throw new RuntimeException("Failed to parse nodeId from buffer.", e);
        }
        return (nodeId) % numReduceTasks;
    }
}

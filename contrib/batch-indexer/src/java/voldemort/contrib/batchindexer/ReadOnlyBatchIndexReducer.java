package voldemort.contrib.batchindexer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.utils.ByteUtils;

public class ReadOnlyBatchIndexReducer implements Reducer<BytesWritable, BytesWritable, Text, Text> {

    private DataOutputStream indexFileStream = null;
    private DataOutputStream valueFileStream = null;

    private long position = 0;

    private JobConf conf = null;
    private String taskId = null;
    private int nodeId = -1;

    Path taskIndexFileName;
    Path taskValueFileName;

    /**
     * Reduce should get sorted MD5 keys here with a single value (appended in
     * begining with 4 bits of nodeId)
     */
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        byte[] keyBytes = ByteUtils.copy(key.get(), 0, key.getSize());

        while(values.hasNext()) {
            BytesWritable value = values.next();
            byte[] valBytes = ByteUtils.copy(value.get(), 0, value.getSize());

            if(nodeId == -1) {
                DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(valBytes));
                nodeId = buffer.readInt();
            }
            // strip first 4 bytes as node_id
            byte[] value1 = ByteUtils.copy(valBytes, 4, valBytes.length);

            // Write Index Key/ position
            indexFileStream.write(keyBytes);
            indexFileStream.writeLong(position);
            valueFileStream.writeInt(value1.length);
            valueFileStream.write(value1);
            position += value1.length + 4;

            if(position < 0) {
                throw new RuntimeException("Position bigger than Integer size, split input files.");
            }
        }

    }

    public void configure(JobConf job) {
        try {
            position = 0;
            conf = job;

            taskId = job.get("mapred.task.id");

            taskIndexFileName = new Path(FileOutputFormat.getOutputPath(conf),
                                         conf.get("voldemort.index.filename") + "_" + taskId);
            taskValueFileName = new Path(FileOutputFormat.getOutputPath(conf),
                                         conf.get("voldemort.data.filename") + "_" + taskId);

            FileSystem fs = taskIndexFileName.getFileSystem(job);

            indexFileStream = fs.create(taskIndexFileName, (short) 1);
            valueFileStream = fs.create(taskValueFileName, (short) 1);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    public void close() throws IOException {

        indexFileStream.close();
        valueFileStream.close();

        Path hdfsIndexFile = new Path(FileOutputFormat.getOutputPath(conf), nodeId + ".index");
        Path hdfsValueFile = new Path(FileOutputFormat.getOutputPath(conf), nodeId + ".data");

        FileSystem fs = hdfsIndexFile.getFileSystem(conf);
        fs.rename(taskIndexFileName, hdfsIndexFile);
        fs.rename(taskValueFileName, hdfsValueFile);
    }
}

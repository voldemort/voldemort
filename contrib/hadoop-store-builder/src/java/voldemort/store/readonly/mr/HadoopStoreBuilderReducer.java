package voldemort.store.readonly.mr;

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
import org.apache.log4j.Logger;

import voldemort.utils.ByteUtils;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 * 
 * @author bbansal, jay
 * 
 */
public class HadoopStoreBuilderReducer extends HadoopStoreBuilderBase implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilderReducer.class);

    private DataOutputStream indexFileStream = null;
    private DataOutputStream valueFileStream = null;
    private long position = 0;
    private String taskId = null;
    private int nodeId = -1;
    private Path taskIndexFileName;
    private Path taskValueFileName;
    private String outputDir;
    private JobConf conf;

    /**
     * Reduce should get sorted MD5 keys here with a single value (appended in
     * beginning with 4 bits of nodeId)
     */
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> values,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {
        // copy out only the valid bytes
        byte[] keyBytes = ByteUtils.copy(key.get(), 0, key.getSize());

        while(values.hasNext()) {
            BytesWritable writable = values.next();
            byte[] valueBytes = writable.get();

            if(nodeId == -1)
                nodeId = ByteUtils.readInt(valueBytes, 0);

            // read all but the first 4 bytes, which contain the node id
            byte[] value = ByteUtils.copy(valueBytes, 4, writable.getSize());

            // Write Index Key/ position
            indexFileStream.write(keyBytes);
            indexFileStream.writeLong(position);
            valueFileStream.writeInt(value.length);
            valueFileStream.write(value);
            position += 4 + value.length;
        }

    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        try {
            conf = job;
            position = 0;
            outputDir = job.get("final.output.dir");
            taskId = job.get("mapred.task.id");

            taskIndexFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName() + "."
                                                                              + taskId + ".index");
            taskValueFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName() + "."
                                                                              + taskId + ".data");
            int replicationFactor = job.getInt("store.output.replication.factor", 2);

            logger.info("Opening " + taskIndexFileName + " and " + taskValueFileName
                        + " for writing.");
            FileSystem fs = taskIndexFileName.getFileSystem(job);
            indexFileStream = fs.create(taskIndexFileName, (short) replicationFactor);
            valueFileStream = fs.create(taskValueFileName, (short) replicationFactor);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    @Override
    public void close() throws IOException {
        indexFileStream.close();
        valueFileStream.close();

        Path indexFile = new Path(outputDir, nodeId + ".index");
        Path valueFile = new Path(outputDir, nodeId + ".data");

        // create output directory
        FileSystem fs = indexFile.getFileSystem(conf);
        fs.mkdirs(indexFile.getParent());

        logger.info("Moving " + taskIndexFileName + " to " + indexFile + ".");
        fs.rename(taskIndexFileName, indexFile);
        logger.info("Moving " + taskValueFileName + " to " + valueFile + ".");
        fs.rename(taskValueFileName, valueFile);
    }
}

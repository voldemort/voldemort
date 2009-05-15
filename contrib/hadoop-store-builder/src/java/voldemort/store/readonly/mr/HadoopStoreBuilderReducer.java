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

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyUtils;
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
    private int position = 0;
    private String taskId = null;
    private int numChunks = -1;
    private int nodeId = -1;
    private int chunkId = -1;
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

            if(this.nodeId == -1)
                this.nodeId = ByteUtils.readInt(valueBytes, 0);
            if(this.chunkId == -1)
                this.chunkId = ReadOnlyUtils.chunk(keyBytes, this.numChunks);

            // read all but the first 4 bytes, which contain the node id
            byte[] value = ByteUtils.copy(valueBytes, 4, writable.getSize());

            // Write key and position
            this.indexFileStream.write(keyBytes);
            this.indexFileStream.writeInt(this.position);

            // Write length and value
            this.valueFileStream.writeInt(value.length);
            this.valueFileStream.write(value);

            this.position += 4 + value.length;
            if(this.position < 0)
                throw new VoldemortException("Chunk overflow exception: chunk " + chunkId
                                             + " has exceeded " + Integer.MAX_VALUE + " bytes.");
        }

    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        try {
            this.conf = job;
            this.position = 0;
            this.numChunks = job.getInt("num.chunks", -1);
            this.outputDir = job.get("final.output.dir");
            this.taskId = job.get("mapred.task.id");

            this.taskIndexFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName()
                                                                                   + "."
                                                                                   + this.taskId
                                                                                   + ".index");
            this.taskValueFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName()
                                                                                   + "."
                                                                                   + this.taskId
                                                                                   + ".data");
            int replicationFactor = job.getInt("store.output.replication.factor", 2);

            logger.info("Opening " + this.taskIndexFileName + " and " + this.taskValueFileName
                        + " for writing.");
            FileSystem fs = this.taskIndexFileName.getFileSystem(job);
            this.indexFileStream = fs.create(this.taskIndexFileName, (short) replicationFactor);
            this.valueFileStream = fs.create(this.taskValueFileName, (short) replicationFactor);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.indexFileStream.close();
        this.valueFileStream.close();

        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);
        Path indexFile = new Path(nodeDir, this.chunkId + ".index");
        Path valueFile = new Path(nodeDir, this.chunkId + ".data");

        // create output directory
        FileSystem fs = indexFile.getFileSystem(this.conf);
        fs.mkdirs(nodeDir);

        logger.info("Moving " + this.taskIndexFileName + " to " + indexFile + ".");
        fs.rename(taskIndexFileName, indexFile);
        logger.info("Moving " + this.taskValueFileName + " to " + valueFile + ".");
        fs.rename(this.taskValueFileName, valueFile);
    }
}

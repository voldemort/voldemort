package voldemort.contrib.batchindexer;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.contrib.utils.ContribUtils;

/**
 * Creates a simple Read-Only Voldemort store for easy batch update.
 * <p>
 * Creates two files
 * <ul>
 * <li><strong>Index</strong> File: Keeps the position Index for each key
 * sorted by MD5(key) Tuple: <KEY_HASH_SIZE(16 bytes)><POSITION_SIZE(8 bytes)>
 * </li>
 * <li><strong>Values</strong> file: Keeps the variable length values Tuple:
 * <SIZE_OF_VALUE(4 bytes)><VALUE(byte[])> </li>
 * <ul>
 * <p>
 * Required Properties
 * <ul>
 * <li>job.name</li>
 * <li>voldemort.cluster.local.filePath</li>
 * <li>voldemort.store.local.filePath</li>
 * <li>voldemort.store.name</li>
 * <li>voldemort.store.version</li>
 * <li>input.data.check.percent</li>
 * </ul>
 * 
 * @author bbansal
 */
public abstract class ReadOnlyBatchIndexer extends Configured implements Tool, JobConfigurable {

    private static Logger logger = Logger.getLogger(ReadOnlyBatchIndexer.class);

    public int run(String[] args) throws Exception {

        JobConf conf = new JobConf(getConf(), ReadOnlyBatchIndexer.class);
        configure(conf);

        try {
            // get the voldemort cluster definition
            Cluster cluster = ContribUtils.getVoldemortClusterDetails(conf.get("voldemort.cluster.local.filePath"));
            conf.setNumReduceTasks(cluster.getNumberOfNodes());
        } catch(Exception e) {
            logger.error("Failed to read Voldemort cluster details", e);
            throw new RuntimeException("", e);
        }

        // set the partitioner
        conf.setPartitionerClass(ReadOnlyBatchIndexPartitioner.class);

        // set mapper Outputclasses
        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(BytesWritable.class);

        // set reducer classes
        conf.setReducerClass(ReadOnlyBatchIndexReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);

        // get the store information
        String storeName = conf.get("voldemort.store.name");
        conf.setStrings("voldemort.index.filename", storeName + ".index");
        conf.setStrings("voldemort.data.filename", storeName + ".data");

        // get Local config files.
        Path clusterFile = new Path(conf.get("voldemort.cluster.local.filePath"));
        Path storeFile = new Path(conf.get("voldemort.store.local.filePath"));

        // move files to HDFS if Hadoop run is not local
        if(!conf.get("mapred.job.tracker").equals("local")) {

            // set temp HDFS paths
            Path clusterHdfs = new Path("/tmp/" + conf.getJobName() + "/" + "cluster.xml");
            Path storeHdfs = new Path("/tmp/" + conf.getJobName() + "/" + "store.xml");

            // get FileSystem & copy files to HDFS
            // TODO LOW: Distributed cache should take care of this
            FileSystem fs = clusterFile.getFileSystem(conf);
            fs.copyFromLocalFile(clusterFile, clusterHdfs);
            fs.copyFromLocalFile(storeFile, storeHdfs);

            // add HDFS files to distributed cache
            DistributedCache.addCacheFile(new URI(clusterHdfs.toString() + "#cluster.xml"), conf);
            DistributedCache.addCacheFile(new URI(storeHdfs.toString() + "#store.xml"), conf);
        } else {
            // Add local files to distributed cache
            DistributedCache.addCacheFile(new URI(clusterFile.toString() + "#cluster.xml"), conf);
            DistributedCache.addCacheFile(new URI(storeFile.toString() + "#store.xml"), conf);
        }

        // run(conf);
        JobClient.runJob(conf);
        return 0;
    }

    /**
     * <strong>configure must set:</strong>
     * <ul>
     * <li>Input Path List</li>
     * <li>Output Path</li>
     * <li>Mapper class <? extends {@link ReadOnlyBatchIndexMapper}></li>
     * <li>Input Format class</li>
     * </ul>
     * <p>
     * <strong>configure must set these properties.</strong>
     * <ul>
     * <li>job.name: String</li>
     * <li>voldemort.cluster.local.filePath: String</li>
     * <li>voldemort.store.local.filePath: String</li>
     * <li>voldemort.store.name: String</li>
     * </ul>
     * 
     * 
     * @return
     */
    public abstract void configure(JobConf conf);
}

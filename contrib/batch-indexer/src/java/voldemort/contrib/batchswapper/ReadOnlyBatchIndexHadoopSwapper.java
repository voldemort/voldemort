package voldemort.contrib.batchswapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.utils.ContribUtils;

/**
 * voldemort swap index job. rsync and swap index/data files for Read-Only store
 * in voldemort cluster. Reads from local filesystem and writes to remote node.
 * <p>
 * {@link ReadOnlyBatchIndexSwapper} have few problems
 * <ul>
 * <li> Bringing all files to one nodes at one time is not practical</li>
 * <li> Hadoop mappers can use hdfs data locality (mapper is smart)</li>
 * </ul>
 * <p>
 * We will use customize inputformat to only give is single line input
 * {@link NonSplitableDummyFileInputFormat}
 * 
 * @author bbansal
 *         <p>
 *         Required Properties
 *         <ul>
 *         <li>voldemort.cluster.local.filePath: String</li>
 *         <li>voldemort.store.local.filePath: String</li>
 *         <li>voldemort.store.name: String</li>
 *         <li>source.path: String HDFS Path</li>
 *         <li>destination.path: String Remote machine temp directory</li>
 *         </ul>
 */
public abstract class ReadOnlyBatchIndexHadoopSwapper extends Configured implements Tool,
        JobConfigurable {

    private static Logger logger = Logger.getLogger(ReadOnlyBatchIndexHadoopSwapper.class);

    public int run(String[] args) {
        JobConf conf = new JobConf(ReadOnlyBatchIndexHadoopSwapper.class);
        configure(conf);

        Cluster cluster = null;
        try {
            // get the voldemort cluster definition
            cluster = ContribUtils.getVoldemortClusterDetails(conf.get("voldemort.cluster.local.filePath"));

            // No Reducer needed for this Job
            conf.setNumReduceTasks(0);

            // set FileInputFormat
            conf.setMapperClass(getSwapperMapperClass());
            conf.setInputFormat(NonSplitableDummyFileInputFormat.class);

            // get the store information
            String storeName = conf.get("voldemort.store.name");

            // move files to Distributed Cache for mapper to use
            moveMetaDatatoHDFS(conf, new Path(conf.get("voldemort.cluster.local.filePath")));

            // set Input Output Path
            FileInputFormat.setInputPaths(conf, new Path(conf.get("source.path")));

            // Set Output File to a dummy temp dir
            String tempHadoopDir = conf.get("hadoop.tmp.dir") + File.separatorChar
                                   + (int) (Math.random() * 1000000);
            FileOutputFormat.setOutputPath(conf, new Path(tempHadoopDir));

            // run(conf);
            JobClient.runJob(conf);

            // delete tempHdoopDir
            new Path(tempHadoopDir).getFileSystem(conf).delete(new Path(tempHadoopDir), true);

            // lets try to swap only the successful nodes
            for(Node node: cluster.getNodes()) {
                SwapperUtils.doSwap(storeName, node, conf.get("destination.path"));
            }
        } catch(Exception e) {
            throw new RuntimeException("Swap Job Failed", e);
        }
        return 0;
    }

    private void moveMetaDatatoHDFS(JobConf conf, Path clusterFile) throws IOException,
            URISyntaxException {
        if(!conf.get("mapred.job.tracker").equals("local")) {
            // make temp hdfs path and add to distributed cache
            Path clusterHdfs = new Path("/tmp/" + conf.getJobName() + "/" + "cluster.xml");
            FileSystem fs = clusterFile.getFileSystem(conf);
            fs.copyFromLocalFile(clusterFile, clusterHdfs);
            DistributedCache.addCacheFile(new URI(clusterHdfs.toString() + "#cluster.xml"), conf);
        } else {
            // Add local files to distributed cache
            DistributedCache.addCacheFile(new URI(clusterFile.toString() + "#cluster.xml"), conf);
        }
    }

    /**
     * <strong>configure must set these properties.</strong>
     * <ul>
     * <li>voldemort.cluster.local.filePath: String</li>
     * <li>voldemort.store.local.filePath: String</li>
     * <li>voldemort.store.name: String</li>
     * <li>source.path: String HDFS Path</li>
     * <li>destination.path: String Remote machine temp directory</li>
     * </ul>
     * 
     * 
     * @return
     */
    public abstract void configure(JobConf conf);

    public abstract Class<? extends AbstractSwapperMapper> getSwapperMapperClass();
}

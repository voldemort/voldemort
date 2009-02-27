package voldemort.contrib.batchswapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.batchindexer.ReadOnlyBatchIndexer;
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
 *         <li>source.HDFS.path: String</li>
 *         <li>destination.remote.path: String</li>
 *         </ul>
 */
public abstract class ReadOnlyBatchIndexHadoopSwapper extends Configured implements Tool,
        JobConfigurable {

    private static Logger logger = Logger.getLogger(ReadOnlyBatchIndexHadoopSwapper.class);

    public void run() throws Throwable {
        JobConf conf = new JobConf(getConf(), ReadOnlyBatchIndexer.class);
        configure(conf);

        Cluster cluster = null;
        try {
            // get the voldemort cluster definition
            cluster = ContribUtils.getVoldemortClusterDetails(conf.get("voldemort.cluster.local.filePath"));
        } catch(Exception e) {
            logger.error("Failed to read Voldemort cluster details", e);
            throw new RuntimeException("", e);
        }

        // No Reducer needed for this Job
        conf.setNumReduceTasks(0);

        // set FileInputFormat
        conf.setInputFormat(NonSplitableDummyFileInputFormat.class);

        // get the store information
        String storeName = conf.get("voldemort.store.name");

        // move files to Distributed Cache for mapper to use
        moveMetaDatatoHDFS(conf, new Path(conf.get("voldemort.cluster.local.filePath")));

        // run(conf);
        JobClient.runJob(conf);

        // lets try to swap only the successful nodes
        for(Node node: cluster.getNodes()) {
            SwapperUtils.doSwap(storeName, node, conf.get("destination.remote.path"));
        }
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

    class SwapperMapper<K, V> implements Mapper<K, V, BytesWritable, BytesWritable> {

        JobConf conf;
        Node node;
        String sourcefileName;
        boolean isIndexFile;
        boolean doNothing = false;

        public void map(K key,
                        V value,
                        OutputCollector<BytesWritable, BytesWritable> output,
                        Reporter reporter) throws IOException {

            if(!doNothing) {
                String targetFileName;
                String destinationDir = conf.get("destination.remote.path");

                if(isIndexFile) {
                    targetFileName = SwapperUtils.getIndexDestinationFile(node.getId(),
                                                                          destinationDir);
                } else {
                    targetFileName = SwapperUtils.getDataDestinationFile(node.getId(),
                                                                         destinationDir);
                }

                // copy remote File
                copyRemoteFile(node.getHost(), sourcefileName, targetFileName);
            }
        }

        public void configure(JobConf job) {
            conf = job;
            String mapFileName = conf.get("mapred.input.file");

            if(mapFileName.contains("index") || mapFileName.contains("data")) {
                System.out.println("handling file:" + mapFileName);

                isIndexFile = mapFileName.contains("index");
                String storeName = conf.get("voldemort.store.name");

                String[] tempSplits = mapFileName.split("_");
                int fileNodeId = Integer.parseInt(tempSplits[tempSplits.length - 1]);

                try {
                    // get the voldemort cluster.xml
                    String clusterFilePath = ContribUtils.getFileFromCache(conf, "cluster.xml");

                    if(null == clusterFilePath) {
                        throw new RuntimeException("Mapper expects cluster.xml / stores.xml passed through Distributed cache.");
                    }
                    // create cluster
                    Cluster cluster = ContribUtils.getVoldemortClusterDetails(clusterFilePath);

                    node = cluster.getNodeById(fileNodeId);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void close() throws IOException {}
    }

    /**
     * <strong>configure must set these properties.</strong>
     * <ul>
     * <li>voldemort.cluster.local.filePath: String</li>
     * <li>voldemort.store.local.filePath: String</li>
     * <li>voldemort.store.name: String</li>
     * <li>source.HDFS.path: String</li>
     * <li>destination.remote.path: String</li>
     * </ul>
     * 
     * 
     * @return
     */
    public abstract void configure(JobConf conf);

    /**
     * copy local source file to remote destination
     * 
     * @param hostname
     * @param source
     * @param destination
     * @return
     */
    public abstract boolean copyRemoteFile(String hostname, String source, String destination);
}

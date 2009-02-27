package voldemort.contrib.batchswapper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.utils.ContribUtils;

public abstract class AbstractSwapperMapper implements Mapper<LongWritable, Text, Text, Text> {

    JobConf conf;
    Node node;
    String sourcefileName;
    boolean isIndexFile;
    boolean doNothing = false;

    /**
     * copy local source file to remote destination
     * 
     * @param hostname
     * @param source
     * @param destination
     * @return
     */
    public abstract boolean copyRemoteFile(String hostname, String source, String destination);

    public void map(LongWritable key,
                    Text value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {

        if(!doNothing) {
            String targetFileName;
            String destinationDir = conf.get("destination.path");

            if(isIndexFile) {
                targetFileName = SwapperUtils.getIndexDestinationFile(node.getId(), destinationDir);
            } else {
                targetFileName = SwapperUtils.getDataDestinationFile(node.getId(), destinationDir);
            }

            // copy remote File
            if(!copyRemoteFile(node.getHost(), sourcefileName, targetFileName)) {
                throw new RuntimeException("Failed to copy file host:" + node.getHost()
                                           + " sourcePath:" + sourcefileName + " targetFileName:"
                                           + targetFileName);
            }
        }
    }

    public void configure(JobConf job) {
        conf = job;
        sourcefileName = new Path(conf.get("map.input.file")).toUri().getPath();

        if(sourcefileName.contains("index") || sourcefileName.contains("data")) {
            System.out.println("mapper got file:" + sourcefileName);

            isIndexFile = new Path(sourcefileName).getName().contains("index");
            String storeName = conf.get("voldemort.store.name");

            // split on '.' character names expected are 0.index , 0.data
            String[] tempSplits = new Path(sourcefileName).getName().split("\\.");
            int fileNodeId = Integer.parseInt(tempSplits[0]);

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
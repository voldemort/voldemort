package voldemort.store.readonly.mr;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Creates a simple Read-Only Voldemort store for easy batch update.
 * <p>
 * Creates a read-only store from the specified input data
 * 
 * @author bbansal, jay
 */
public class HadoopStoreBuilder {

    private final Configuration config;
    private final Class<? extends AbstractStoreBuilderMapper<?, ?>> mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final int replicationFactor;
    private final Path inputPath;
    private final Path outputDir;
    private final Path tempDir;

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param replicationFactor
     * @param tempDir
     * @param outputDir
     * @param path
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              int replicationFactor,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath) {
        super();
        this.config = conf;
        this.mapperClass = Utils.notNull(mapperClass);
        this.inputFormatClass = Utils.notNull(inputFormatClass);
        this.inputPath = inputPath;
        this.cluster = Utils.notNull(cluster);
        this.storeDef = Utils.notNull(storeDef);
        this.replicationFactor = replicationFactor;
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
    }

    public void build() {
        JobConf conf = new JobConf(config);
        conf.setInt("io.file.buffer.size", 64 * 1024);
        conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
        conf.set("stores.xml",
                 new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
        conf.setInt("store.output.replication.factor", replicationFactor);
        conf.setNumReduceTasks(cluster.getNumberOfNodes());
        conf.setPartitionerClass(HadoopStoreBuilderPartitioner.class);
        conf.setMapperClass(mapperClass);
        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(BytesWritable.class);
        conf.setReducerClass(HadoopStoreBuilderReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);
        conf.setInputFormat(inputFormatClass);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(conf, inputPath);
        conf.set("final.output.dir", outputDir.toString());
        FileOutputFormat.setOutputPath(conf, tempDir);

        try {
            // delete output dir if it already exists
            FileSystem fs = tempDir.getFileSystem(conf);
            fs.delete(tempDir, true);

            // run job
            JobClient.runJob(conf);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

}

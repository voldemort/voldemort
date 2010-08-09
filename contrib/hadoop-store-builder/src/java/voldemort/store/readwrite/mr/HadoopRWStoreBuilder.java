/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readwrite.mr;

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
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Builds a read-write voldemort store as a hadoop job from the given input
 * data.
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopRWStoreBuilder {

    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    public static final long MIN_CHUNK_SIZE = 1L;
    public static final long MAX_CHUNK_SIZE = (long) (1.9 * 1024 * 1024 * 1024);

    private static final Logger logger = Logger.getLogger(HadoopRWStoreBuilder.class);

    private final Configuration config;
    private final Class<? extends AbstractRWHadoopStoreBuilderMapper<?, ?>> mapperClass;
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final Path inputPath;
    private final Path tempPath;
    private final int reducersPerNode, hadoopNodeId;
    private final long hadoopPushVersion;

    public HadoopRWStoreBuilder(Configuration conf,
                                Class<? extends AbstractRWHadoopStoreBuilderMapper<?, ?>> mapperClass,
                                Class<? extends InputFormat> inputFormatClass,
                                Cluster cluster,
                                StoreDefinition storeDef,
                                int reducersPerNode,
                                Path tempPath,
                                Path inputPath) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             reducersPerNode,
             cluster.getNumberOfNodes(),
             1L,
             tempPath,
             inputPath);
    }

    public HadoopRWStoreBuilder(Configuration conf,
                                Class<? extends AbstractRWHadoopStoreBuilderMapper<?, ?>> mapperClass,
                                Class<? extends InputFormat> inputFormatClass,
                                Cluster cluster,
                                StoreDefinition storeDef,
                                int reducersPerNode,
                                int hadoopNodeId,
                                long hadoopPushVersion,
                                Path tempPath,
                                Path inputPath) {
        this.config = conf;
        this.mapperClass = Utils.notNull(mapperClass);
        this.inputFormatClass = Utils.notNull(inputFormatClass);
        this.inputPath = Utils.notNull(inputPath);
        this.cluster = Utils.notNull(cluster);
        this.storeDef = Utils.notNull(storeDef);
        this.tempPath = Utils.notNull(tempPath);
        this.hadoopNodeId = hadoopNodeId;
        this.hadoopPushVersion = hadoopPushVersion;
        this.reducersPerNode = reducersPerNode;
        if(reducersPerNode < 0)
            throw new VoldemortException("Number of reducers cannot be negative");
    }

    /**
     * Run the job
     */
    public void build() {
        JobConf conf = new JobConf(config);
        conf.setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
        conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
        conf.set("stores.xml",
                 new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
        conf.setInt("hadoop.node.id", this.hadoopNodeId);
        conf.setLong("hadoop.push.version", this.hadoopPushVersion);
        conf.setLong("job.start.time.ms", System.currentTimeMillis());

        conf.setPartitionerClass(HadoopRWStoreBuilderPartitioner.class);

        conf.setInputFormat(inputFormatClass);
        conf.setMapperClass(mapperClass);
        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(BytesWritable.class);
        conf.setReducerClass(HadoopRWStoreBuilderReducer.class);

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);

        conf.setJarByClass(getClass());
        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, tempPath);

        try {
            // delete the temp dir if it exists
            FileSystem tempFs = tempPath.getFileSystem(conf);
            tempFs.delete(tempPath, true);

            conf.setInt("num.chunks", reducersPerNode);
            int numReducers = cluster.getNumberOfNodes() * reducersPerNode;
            logger.info("Replication factor = " + storeDef.getReplicationFactor() + ", numNodes = "
                        + cluster.getNumberOfNodes() + ", reducers per node = " + reducersPerNode
                        + ", numReducers = " + numReducers);
            conf.setNumReduceTasks(numReducers);

            logger.info("Building RW store...");
            JobClient.runJob(conf);

        } catch(Exception e) {
            throw new VoldemortException(e);
        }

    }

}

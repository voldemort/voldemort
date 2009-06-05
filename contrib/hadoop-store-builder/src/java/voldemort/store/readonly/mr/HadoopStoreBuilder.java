/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
 * Builds a read-only voldemort store as a hadoop job from the given input data.
 * 
 * @author bbansal, jay
 */
public class HadoopStoreBuilder {

    public static final long MIN_CHUNK_SIZE = 1L;
    public static final long MAX_CHUNK_SIZE = (long) (1.9 * 1024 * 1024 * 1024);

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilder.class);

    private final Configuration config;
    private final Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final int replicationFactor;
    private final long chunkSizeBytes;
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
     * @param replicationFactor The replication factor to use for storing the
     *        built store.
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              int replicationFactor,
                              long chunkSizeBytes,
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
        this.chunkSizeBytes = chunkSizeBytes;
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
        if(chunkSizeBytes > MAX_CHUNK_SIZE || chunkSizeBytes < MIN_CHUNK_SIZE)
            throw new VoldemortException("Invalid chunk size, chunk size must be in the range "
                                         + MIN_CHUNK_SIZE + "..." + MAX_CHUNK_SIZE);
    }

    /**
     * Run the job
     */
    public void build() {
        JobConf conf = new JobConf(config);
        conf.setInt("io.file.buffer.size", 64 * 1024);
        conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
        conf.set("stores.xml",
                 new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
        conf.setInt("store.output.replication.factor", replicationFactor);
        conf.setPartitionerClass(HadoopStoreBuilderPartitioner.class);
        conf.setMapperClass(mapperClass);
        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(BytesWritable.class);
        conf.setReducerClass(HadoopStoreBuilderReducer.class);
        conf.setInputFormat(inputFormatClass);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);
        conf.setJarByClass(getClass());
        FileInputFormat.setInputPaths(conf, inputPath);
        conf.set("final.output.dir", outputDir.toString());
        FileOutputFormat.setOutputPath(conf, tempDir);

        try {
            // delete output dir if it already exists
            FileSystem fs = tempDir.getFileSystem(conf);
            fs.delete(tempDir, true);

            long size = sizeOfPath(fs, inputPath);
            int numChunks = Math.max((int) (storeDef.getReplicationFactor() * size
                                            / cluster.getNumberOfNodes() / chunkSizeBytes), 1);
            logger.info("Data size = " + size + ", replication factor = "
                        + storeDef.getReplicationFactor() + ", numNodes = "
                        + cluster.getNumberOfNodes() + ", chunk size = " + chunkSizeBytes
                        + ",  num.chunks = " + numChunks);
            conf.setInt("num.chunks", numChunks);
            int numReduces = cluster.getNumberOfNodes() * numChunks;
            conf.setNumReduceTasks(numReduces);
            logger.info("Number of reduces: " + numReduces);

            logger.info("Building store...");
            JobClient.runJob(conf);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private long sizeOfPath(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(fs, status.getPath());
                else
                    size += status.getLen();
            }
        }
        return size;
    }
}

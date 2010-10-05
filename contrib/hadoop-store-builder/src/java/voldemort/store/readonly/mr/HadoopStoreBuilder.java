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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Builds a read-only voldemort store as a hadoop job from the given input data.
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilder {

    public static final long MIN_CHUNK_SIZE = 1L;
    public static final long MAX_CHUNK_SIZE = (long) (1.9 * 1024 * 1024 * 1024);
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilder.class);

    private final Configuration config;
    private final Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final long chunkSizeBytes;
    private final Path inputPath;
    private final Path outputDir;
    private final Path tempDir;
    private CheckSumType checkSumType = CheckSumType.NONE;

    /**
     * Kept for backwards compatibility. We do not use replicationFactor any
     * more since it is derived from the store definition
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param replicationFactor NOT USED
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     */
    @SuppressWarnings("unchecked")
    @Deprecated
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
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
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
        this.chunkSizeBytes = chunkSizeBytes;
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
        if(chunkSizeBytes > MAX_CHUNK_SIZE || chunkSizeBytes < MIN_CHUNK_SIZE)
            throw new VoldemortException("Invalid chunk size, chunk size must be in the range "
                                         + MIN_CHUNK_SIZE + "..." + MAX_CHUNK_SIZE);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath);
        this.checkSumType = checkSumType;

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
        conf.set("checksum.type", CheckSum.toString(checkSumType));
        FileOutputFormat.setOutputPath(conf, tempDir);

        try {

            FileSystem outputFs = outputDir.getFileSystem(conf);
            if(outputFs.exists(outputDir)) {
                throw new IOException("Final output directory already exists.");
            }

            // delete output dir if it already exists
            FileSystem tempFs = tempDir.getFileSystem(conf);
            tempFs.delete(tempDir, true);

            long size = sizeOfPath(tempFs, inputPath);
            int numChunksPerPartition = Math.max((int) (storeDef.getReplicationFactor() * size
                                                        / cluster.getNumberOfPartitions() / chunkSizeBytes),
                                                 1);
            logger.info("Data size = " + size + ", replication factor = "
                        + storeDef.getReplicationFactor() + ", numNodes = "
                        + cluster.getNumberOfNodes() + ", chunk size = " + chunkSizeBytes
                        + ",  num.chunks per partition = " + numChunksPerPartition);
            conf.setInt("num.chunks", numChunksPerPartition);
            int numReduces = cluster.getNumberOfPartitions() * numChunksPerPartition;
            conf.setNumReduceTasks(numReduces);
            logger.info("Number of reduces: " + numReduces);

            logger.info("Building store...");
            JobClient.runJob(conf);

            ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
            metadata.add(ReadOnlyStorageMetadata.FORMAT,
                         ReadOnlyStorageFormat.READONLY_V1.getCode());

            // Check if all folder exists and with format file
            for(Node node: cluster.getNodes()) {
                Path nodePath = new Path(outputDir.toString(), "node-" + node.getId());
                if(!outputFs.exists(nodePath)) {
                    outputFs.mkdirs(nodePath); // Create empty folder
                }

                // Write metadata
                FSDataOutputStream metadataStream = outputFs.create(new Path(nodePath, ".metadata"));
                metadataStream.write(metadata.toJsonString().getBytes());
                metadataStream.flush();
                metadataStream.close();
            }

            if(checkSumType != CheckSumType.NONE) {

                // Generate checksum for every node
                FileStatus[] nodes = outputFs.listStatus(outputDir);

                // Do a CheckSumOfCheckSum - Similar to HDFS
                CheckSum checkSumGenerator = CheckSum.getInstance(this.checkSumType);
                if(checkSumGenerator == null) {
                    throw new VoldemortException("Could not generate checksum digests");
                }

                for(FileStatus node: nodes) {
                    if(node.isDir()) {
                        // Read all check sum files
                        FileStatus[] storeFiles = outputFs.listStatus(node.getPath(),
                                                                      new PathFilter() {

                                                                          public boolean accept(Path arg0) {
                                                                              if(arg0.getName()
                                                                                     .endsWith("checksum")
                                                                                 && !arg0.getName()
                                                                                         .startsWith(".")) {
                                                                                  return true;
                                                                              }
                                                                              return false;
                                                                          }
                                                                      });

                        if(storeFiles != null) {
                            Arrays.sort(storeFiles, new IndexFileLastComparator());
                            for(FileStatus file: storeFiles) {
                                FSDataInputStream input = outputFs.open(file.getPath());
                                byte fileCheckSum[] = new byte[CheckSum.checkSumLength(this.checkSumType)];
                                input.read(fileCheckSum);
                                checkSumGenerator.update(fileCheckSum);
                                outputFs.delete(file.getPath(), true);
                            }
                            FSDataOutputStream checkSumStream = outputFs.create(new Path(node.getPath(),
                                                                                         CheckSum.toString(checkSumType)
                                                                                                 + "checkSum.txt"));
                            checkSumStream.write(checkSumGenerator.getCheckSum());
                            checkSumStream.flush();
                            checkSumStream.close();

                        }
                    }
                }
            }
        } catch(Exception e) {
            throw new VoldemortException(e);
        }

    }

    /**
     * A comparator that sorts index files last. This is required to maintain
     * the order while calculating checksum
     * 
     */
    static class IndexFileLastComparator implements Comparator<FileStatus> {

        public int compare(FileStatus fs1, FileStatus fs2) {
            // directories before files
            if(fs1.isDir())
                return fs2.isDir() ? 0 : -1;
            if(fs2.isDir())
                return fs1.isDir() ? 0 : 1;

            String f1 = fs1.getPath().getName(), f2 = fs2.getPath().getName();

            // if both same, lexicographically
            if((f1.contains(".index") && f2.contains(".index"))
               || (f1.contains(".data") && f2.contains(".data"))) {
                return f1.compareToIgnoreCase(f2);
            }

            if(f1.contains(".index")) {
                return 1;
            } else {
                return -1;
            }
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

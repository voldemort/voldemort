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

package voldemort.store.readonly.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
    private boolean saveKeys = false;

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
                              CheckSumType checkSumType,
                              boolean saveKeys) {
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
        this.checkSumType = checkSumType;
        this.saveKeys = saveKeys;
    }

    /**
     * Run the job
     */
    public void build() {
        try {
            Job job = new Job(config);
            job.getConfiguration().setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
            job.getConfiguration().set("cluster.xml", new ClusterMapper().writeCluster(cluster));
            job.getConfiguration()
               .set("stores.xml",
                    new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
            job.getConfiguration().setBoolean("save.keys", saveKeys);
            job.getConfiguration().set("final.output.dir", outputDir.toString());
            job.getConfiguration().set("checksum.type", CheckSum.toString(checkSumType));
            job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
            job.setPartitionerClass(HadoopStoreBuilderPartitioner.class);
            job.setMapperClass(mapperClass);
            job.setMapOutputKeyClass(BytesWritable.class);
            job.setMapOutputValueClass(BytesWritable.class);
            job.setReducerClass(HadoopStoreBuilderReducer.class);
            job.setInputFormatClass(inputFormatClass);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(BytesWritable.class);
            job.setOutputValueClass(BytesWritable.class);
            job.setJarByClass(getClass());

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, tempDir);

            FileSystem outputFs = outputDir.getFileSystem(job.getConfiguration());
            if(outputFs.exists(outputDir)) {
                throw new IOException("Final output directory already exists.");
            }

            // delete output dir if it already exists
            FileSystem tempFs = tempDir.getFileSystem(job.getConfiguration());
            tempFs.delete(tempDir, true);

            long size = sizeOfPath(tempFs, inputPath);
            int numChunks = Math.max((int) (storeDef.getReplicationFactor() * size
                                            / cluster.getNumberOfNodes() / chunkSizeBytes), 1);
            logger.info("Data size = " + size + ", replication factor = "
                        + storeDef.getReplicationFactor() + ", numNodes = "
                        + cluster.getNumberOfNodes() + ", chunk size = " + chunkSizeBytes
                        + ",  num.chunks = " + numChunks);
            job.getConfiguration().setInt("num.chunks", numChunks);
            int numReduces = cluster.getNumberOfNodes() * numChunks;
            job.setNumReduceTasks(numReduces);
            logger.info("Number of reduces: " + numReduces);

            logger.info("Building store...");
            job.waitForCompletion(true);

            // Do a CheckSumOfCheckSum - Similar to HDFS
            CheckSum checkSumGenerator = CheckSum.getInstance(this.checkSumType);
            if(!this.checkSumType.equals(CheckSumType.NONE) && checkSumGenerator == null) {
                throw new VoldemortException("Could not generate checksum digest for type "
                                             + this.checkSumType);
            }

            // Check if all folder exists and with format file
            for(Node node: cluster.getNodes()) {

                ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();

                if(saveKeys)
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V2.getCode());
                else
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V1.getCode());

                Path nodePath = new Path(outputDir.toString(), "node-" + node.getId());
                if(!outputFs.exists(nodePath)) {
                    outputFs.mkdirs(nodePath); // Create empty folder
                }

                if(checkSumType != CheckSumType.NONE) {

                    FileStatus[] storeFiles = outputFs.listStatus(nodePath, new PathFilter() {

                        public boolean accept(Path arg0) {
                            if(arg0.getName().endsWith("checksum")
                               && !arg0.getName().startsWith(".")) {
                                return true;
                            }
                            return false;
                        }
                    });

                    if(storeFiles != null && storeFiles.length > 0) {
                        Arrays.sort(storeFiles, new IndexFileLastComparator());
                        FSDataInputStream input = null;

                        for(FileStatus file: storeFiles) {
                            try {
                                input = outputFs.open(file.getPath());
                                byte fileCheckSum[] = new byte[CheckSum.checkSumLength(this.checkSumType)];
                                input.read(fileCheckSum);
                                logger.debug("Checksum for file " + file.toString() + " - "
                                             + new String(Hex.encodeHex(fileCheckSum)));
                                checkSumGenerator.update(fileCheckSum);
                            } catch(Exception e) {
                                logger.error("Error while reading checksum file " + e.getMessage(),
                                             e);
                            } finally {
                                if(input != null)
                                    input.close();
                            }
                            outputFs.delete(file.getPath(), false);
                        }

                        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE,
                                     CheckSum.toString(checkSumType));

                        String checkSum = new String(Hex.encodeHex(checkSumGenerator.getCheckSum()));
                        logger.info("Checksum for node " + node.getId() + " - " + checkSum);

                        metadata.add(ReadOnlyStorageMetadata.CHECKSUM, checkSum);
                    }
                }

                // Write metadata
                FSDataOutputStream metadataStream = outputFs.create(new Path(nodePath, ".metadata"));
                metadataStream.write(metadata.toJsonString().getBytes());
                metadataStream.flush();
                metadataStream.close();

            }

        } catch(Exception e) {
            logger.error("Error in Store builder", e);
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

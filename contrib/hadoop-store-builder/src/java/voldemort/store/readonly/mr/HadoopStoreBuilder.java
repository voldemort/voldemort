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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Task;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.checksum.CheckSumMetadata;
import voldemort.store.readonly.disk.KeyValueWriter;
import voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJob;
import voldemort.utils.ByteUtils;
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
    public static final short HADOOP_FILE_PERMISSION = 493;

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilder.class);

    private final Configuration config;
    private final Class mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final long chunkSizeBytes;
    private final Path inputPath;
    private final Path outputDir;
    private final Path tempDir;
    private final CheckSumType checkSumType;
    private final boolean saveKeys;
    private final boolean reducerPerBucket;
    private int numChunksOverride;
    private final boolean isAvro;
    private final Long minNumberOfRecords;
    private final boolean buildPrimaryReplicasOnly;

    /**
     * Create the store builder
     *
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param tempDir The temporary directory to use in hadoop for intermediate
*        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     * @param saveKeys Boolean to signify if we want to save the key as well
     * @param reducerPerBucket Boolean to signify whether we want to have a
*        single reducer for a bucket ( thereby resulting in all chunk files
*        for a bucket being generated in a single reducer )
     * @param chunkSizeBytes Size of each chunks (ignored if numChunksOverride is > 0)
     * @param numChunksOverride Number of chunks per bucket ( partition or partition
*        replica )
     * @param isAvro whether the data format is avro
     * @param minNumberOfRecords if job generates fewer records than this, fail.
     * @param buildPrimaryReplicasOnly if true: build each partition only once,
     *                                 and store the files grouped by partition.
     *                                 if false: build all replicas redundantly,
     *                                 and store the files grouped by node.
     */
    public HadoopStoreBuilder(Configuration conf,
                              Class mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType,
                              boolean saveKeys,
                              boolean reducerPerBucket,
                              long chunkSizeBytes,
                              int numChunksOverride,
                              boolean isAvro,
                              Long minNumberOfRecords,
                              boolean buildPrimaryReplicasOnly) {
        this.config = conf;
        this.mapperClass = Utils.notNull(mapperClass);
        this.inputFormatClass = Utils.notNull(inputFormatClass);
        this.inputPath = inputPath;
        this.cluster = Utils.notNull(cluster);
        this.storeDef = Utils.notNull(storeDef);
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
        this.checkSumType = checkSumType;
        this.saveKeys = saveKeys;
        this.reducerPerBucket = reducerPerBucket;
        this.chunkSizeBytes = chunkSizeBytes;
        this.numChunksOverride = numChunksOverride;
        this.isAvro = isAvro;
        this.minNumberOfRecords = minNumberOfRecords == null ? 1 : minNumberOfRecords;
        this.buildPrimaryReplicasOnly = buildPrimaryReplicasOnly;

        if(numChunksOverride <= 0) {
            logger.info("HadoopStoreBuilder constructed with numChunksOverride <= 0, thus relying chunk size.");
            if(chunkSizeBytes > MAX_CHUNK_SIZE || chunkSizeBytes < MIN_CHUNK_SIZE) {
                throw new VoldemortException("Invalid chunk size, chunk size must be in the range "
                        + MIN_CHUNK_SIZE + "..." + MAX_CHUNK_SIZE);
            }
        } else {
            logger.info("HadoopStoreBuilder constructed with numChunksOverride > 0, thus ignoring chunk size.");
        }
    }


    /**
     * Run the job
     */
    public void build() {
        try {
            JobConf conf = new JobConf(config);
            conf.setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
            conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
            conf.set("stores.xml",
                     new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
            conf.setBoolean(VoldemortBuildAndPushJob.SAVE_KEYS, saveKeys);
            conf.setBoolean(VoldemortBuildAndPushJob.REDUCER_PER_BUCKET, reducerPerBucket);
            conf.setBoolean(VoldemortBuildAndPushJob.BUILD_PRIMARY_REPLICAS_ONLY, buildPrimaryReplicasOnly);
            if(!isAvro) {
                conf.setPartitionerClass(HadoopStoreBuilderPartitioner.class);
                conf.setMapperClass(mapperClass);
                conf.setMapOutputKeyClass(BytesWritable.class);
                conf.setMapOutputValueClass(BytesWritable.class);
                conf.setReducerClass(HadoopStoreBuilderReducer.class);
            }
            conf.setInputFormat(inputFormatClass);
            conf.setOutputFormat(SequenceFileOutputFormat.class);
            conf.setOutputKeyClass(BytesWritable.class);
            conf.setOutputValueClass(BytesWritable.class);
            conf.setJarByClass(getClass());
            conf.setReduceSpeculativeExecution(false);
            FileInputFormat.setInputPaths(conf, inputPath);
            conf.set("final.output.dir", outputDir.toString());
            conf.set("checksum.type", CheckSum.toString(checkSumType));
            conf.set("dfs.umaskmode", "002");
            FileOutputFormat.setOutputPath(conf, tempDir);

            FileSystem outputFs = outputDir.getFileSystem(conf);
            if(outputFs.exists(outputDir)) {
                throw new IOException("Final output directory already exists.");
            }

            // delete output dir if it already exists
            FileSystem tempFs = tempDir.getFileSystem(conf);
            tempFs.delete(tempDir, true);

            long size = sizeOfPath(tempFs, inputPath);
            logger.info("Data size = " + size +
                        ", replication factor = " + storeDef.getReplicationFactor() +
                        ", numNodes = " + cluster.getNumberOfNodes() +
                        ", numPartitions = " + cluster.getNumberOfPartitions() +
                        ", chunk size = " + chunkSizeBytes);

            // Dynamically calculate number of chunks and reducers

            // Base numbers of chunks and reducers, will get modified according to various settings
            int numChunks = (int) (size / cluster.getNumberOfPartitions() / chunkSizeBytes);
            int numReducers = cluster.getNumberOfPartitions();

            // In Save Keys mode, which was introduced with the READ_ONLY_V2 file format, the replicas
            // are shuffled via additional reducers, whereas originally they were shuffled via
            // additional chunks. Whether this distinction actually makes sense or not is an interesting
            // question, but in order to avoid breaking anything we'll just maintain the original behavior.
            if (saveKeys) {
                if (buildPrimaryReplicasOnly) {
                    // The buildPrimaryReplicasOnly mode is supported exclusively in combination with
                    // saveKeys. If enabled, then we don't want to shuffle extra keys redundantly,
                    // hence we don't change the number of reducers.
                } else {
                    // Old behavior, where all keys are redundantly shuffled to redundant reducers.
                    numReducers = numReducers * storeDef.getReplicationFactor();
                }
            } else {
                numChunks = numChunks * storeDef.getReplicationFactor();
            }

            // Ensure at least one chunk
            numChunks = Math.max(numChunks, 1);

            if (reducerPerBucket) {
                // Then all chunks for a given partition/replica combination are shuffled to the same
                // reducer, hence, the number of reducers remains the same as previously defined.
            } else {
                // Otherwise, we want one reducer per chunk, hence we multiply the number of reducers.
                numReducers = numReducers * numChunks;
            }

            if (this.numChunksOverride > 0) {
                logger.info("The " + VoldemortBuildAndPushJob.NUM_CHUNKS + " setting is overridden " +
                            "by config, so we'll use the override (" + this.numChunksOverride + ") " +
                            "and discard the dynamically computed value (" + numChunks + ").");
                numChunks = this.numChunksOverride;
            }

            conf.setInt(VoldemortBuildAndPushJob.NUM_CHUNKS, numChunks);
            conf.setNumReduceTasks(numReducers);

            if(isAvro) {
                conf.setPartitionerClass(AvroStoreBuilderPartitioner.class);
                // conf.setMapperClass(mapperClass);
                conf.setMapOutputKeyClass(ByteBuffer.class);
                conf.setMapOutputValueClass(ByteBuffer.class);

                conf.setInputFormat(inputFormatClass);

                conf.setOutputFormat((Class<? extends OutputFormat>) AvroOutputFormat.class);
                conf.setOutputKeyClass(ByteBuffer.class);
                conf.setOutputValueClass(ByteBuffer.class);

                // AvroJob confs for the avro mapper
                AvroJob.setInputSchema(conf, Schema.parse(config.get("avro.rec.schema")));

                AvroJob.setOutputSchema(conf,
                                        Pair.getPairSchema(Schema.create(Schema.Type.BYTES),
                                                           Schema.create(Schema.Type.BYTES)));

                AvroJob.setMapperClass(conf, mapperClass);
                conf.setReducerClass(AvroStoreBuilderReducer.class);
            }

            logger.info("Number of chunks: " + numChunks + ", number of reducers: " + numReducers
                        + ", save keys: " + saveKeys + ", reducerPerBucket: " + reducerPerBucket
                        + ", buildPrimaryReplicasOnly: " + buildPrimaryReplicasOnly);
            logger.info("Building store...");
            RunningJob job = JobClient.runJob(conf);

            // Once the job has completed log the counter
            Counters counters = job.getCounters();

            long numberOfRecords = counters.getCounter(Task.Counter.REDUCE_INPUT_GROUPS);

            if (numberOfRecords < minNumberOfRecords) {
                throw new VoldemortException("The number of records in the data set (" + numberOfRecords +
                        ") is lower than the minimum required (" + minNumberOfRecords + "). Aborting.");
            }

            if(saveKeys) {
                logger.info("Number of collisions in the job - "
                            + counters.getCounter(KeyValueWriter.CollisionCounter.NUM_COLLISIONS));
                logger.info("Maximum number of collisions for one entry - "
                            + counters.getCounter(KeyValueWriter.CollisionCounter.MAX_COLLISIONS));
            }

            // Do a CheckSumOfCheckSum - Similar to HDFS
            CheckSum checkSumGenerator = CheckSum.getInstance(this.checkSumType);
            if(!this.checkSumType.equals(CheckSumType.NONE) && checkSumGenerator == null) {
                throw new VoldemortException("Could not generate checksum digest for type "
                                             + this.checkSumType);
            }

            List<String> directories = Lists.newArrayList();
            if (buildPrimaryReplicasOnly) {
                // Files are grouped by partitions
                for(int partitionId = 0; partitionId < cluster.getNumberOfPartitions(); partitionId++) {
                    directories.add(ReadOnlyUtils.PARTITION_DIRECTORY_PREFIX + partitionId);
                }
            } else {
                // Files are grouped by node
                for(Node node: cluster.getNodes()) {
                    directories.add(ReadOnlyUtils.NODE_DIRECTORY_PREFIX + node.getId());
                }
            }

            ReadOnlyStorageMetadata fullStoreMetadata = new ReadOnlyStorageMetadata();

            // Check if all folder exists and with format file
            for(String directoryName: directories) {

                ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();

                if(saveKeys) {
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V2.getCode());
                } else {
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V1.getCode());
                }

                Path directoryPath = new Path(outputDir.toString(), directoryName);

                if(!outputFs.exists(directoryPath)) {
                    logger.info("No data generated for " + directoryName
                                        + ". Generating empty folder");
                    outputFs.mkdirs(directoryPath); // Create empty folder
                    outputFs.setPermission(directoryPath, new FsPermission(HADOOP_FILE_PERMISSION));
                    logger.info("Setting permission to 755 for " + directoryPath);
                }

                processCheckSumMetadataFile(directoryName, outputFs, checkSumGenerator, directoryPath, metadata);

                if (buildPrimaryReplicasOnly) {
                    // In buildPrimaryReplicasOnly mode, writing a metadata file for each partitions
                    // takes too long, so we skip it. We will rely on the full-store.metadata file instead.
                } else {
                    // Maintaining the old behavior: we write the node-specific metadata file
                    writeMetadataFile(directoryPath, outputFs, ReadOnlyUtils.METADATA_FILE_EXTENSION, metadata);
                }

                fullStoreMetadata.addNestedMetadata(directoryName, metadata);
            }

            // Write the aggregate metadata file
            writeMetadataFile(outputDir, outputFs, ReadOnlyUtils.FULL_STORE_METADATA_FILE, fullStoreMetadata);

        } catch(Exception e) {
            logger.error("Error in Store builder", e);
            throw new VoldemortException(e);
        }
    }

    /**
     * Persists a *.metadata file to a specific directory in HDFS.
     *
     * @param directoryPath where to write the metadata file.
     * @param outputFs {@link org.apache.hadoop.fs.FileSystem} where to write the file
     * @param metadataFileName name of the file (including extension)
     * @param metadata {@link voldemort.store.readonly.ReadOnlyStorageMetadata} to persist on HDFS
     * @throws IOException if the FileSystem operations fail
     */
    private void writeMetadataFile(Path directoryPath,
                                   FileSystem outputFs,
                                   String metadataFileName,
                                   ReadOnlyStorageMetadata metadata) throws IOException {
        Path metadataPath = new Path(directoryPath, metadataFileName);
        FSDataOutputStream metadataStream = outputFs.create(metadataPath);
        outputFs.setPermission(metadataPath, new FsPermission(HADOOP_FILE_PERMISSION));
        logger.info("Setting permission to 755 for " + metadataPath);
        metadataStream.write(metadata.toJsonString().getBytes());
        metadataStream.flush();
        metadataStream.close();
    }

    /**
     * For the given node, following three actions are done:
     * 
     * 1. Computes checksum of checksums
     * 
     * 2. Computes total data size
     * 
     * 3. Computes total index size
     * 
     * Finally updates the metadata file with those information
     * 
     * @param directoryName
     * @param outputFs
     * @param checkSumGenerator
     * @param nodePath
     * @param metadata
     * @throws IOException
     */
    private void processCheckSumMetadataFile(String directoryName,
                                             FileSystem outputFs,
                                             CheckSum checkSumGenerator,
                                             Path nodePath,
                                             ReadOnlyStorageMetadata metadata) throws IOException {

        long dataSizeInBytes = 0L;
        long indexSizeInBytes = 0L;

        FileStatus[] storeFiles = outputFs.listStatus(nodePath, new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if(arg0.getName().endsWith("checksum") && !arg0.getName().startsWith(".")) {
                    return true;
                }
                return false;
            }
        });

        if(storeFiles != null && storeFiles.length > 0) {
            Arrays.sort(storeFiles, new IndexFileLastComparator());
            FSDataInputStream input = null;
            CheckSumMetadata checksumMetadata;

            for(FileStatus file: storeFiles) {
                try {
                    // HDFS NameNodes can sometimes GC for extended periods
                    // of time, hence the exponential back-off strategy below.
                    // TODO: Refactor all BnP retry code into a pluggable mechanism

                    int totalAttempts = 4;
                    int attemptsRemaining = totalAttempts;
                    while(attemptsRemaining > 0) {
                        try {
                            attemptsRemaining--;
                            input = outputFs.open(file.getPath());
                        } catch(Exception e) {
                            if(attemptsRemaining < 1) {
                                throw e;
                            }

                            // Exponential back-off sleep times: 5s, 25s, 45s.
                            int sleepTime = ((totalAttempts - attemptsRemaining) ^ 2) * 5;
                            logger.error("Error getting checksum file from HDFS. Retries left: "
                                         + attemptsRemaining + ". Back-off until next retry: "
                                         + sleepTime + " seconds.", e);

                            Thread.sleep(sleepTime * 1000);
                        }
                    }
                    checksumMetadata = new CheckSumMetadata(input);
                    if(checkSumType != CheckSumType.NONE) {
                        byte[] fileChecksum = checksumMetadata.getCheckSum();
                        logger.debug("Checksum for file " + file.toString() + " - "
                                     + new String(Hex.encodeHex(fileChecksum)));
                        checkSumGenerator.update(fileChecksum);
                    }
                    /*
                     * if this is a 'data checksum' file, add the data file size
                     * to 'dataSizeIbBytes'
                     */
                    String dataFileSizeInBytes = (String) checksumMetadata.get(CheckSumMetadata.DATA_FILE_SIZE_IN_BYTES);
                    if(dataFileSizeInBytes != null) {
                        dataSizeInBytes += Long.parseLong(dataFileSizeInBytes);
                    }

                    /*
                     * if this is a 'index checksum' file, add the index file
                     * size to 'indexSizeIbBytes'
                     */
                    String indexFileSizeInBytes = (String) checksumMetadata.get(CheckSumMetadata.INDEX_FILE_SIZE_IN_BYTES);
                    if(indexFileSizeInBytes != null) {
                        indexSizeInBytes += Long.parseLong(indexFileSizeInBytes);
                    }
                } catch(Exception e) {
                    logger.error("Error getting checksum file from HDFS", e);
                } finally {
                    if(input != null)
                        input.close();
                }
                outputFs.delete(file.getPath(), false);
            }

            // update metadata
            long diskSizeForNodeInBytes = dataSizeInBytes + indexSizeInBytes;
            logger.info("Estimated disk size for store " + this.storeDef.getName() + " in "
                        + directoryName + " in KB: "
                        + (diskSizeForNodeInBytes / ByteUtils.BYTES_PER_KB));
            metadata.add(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES,
                         Long.toString(diskSizeForNodeInBytes));
            if(checkSumType != CheckSumType.NONE) {
                metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(checkSumType));

                String checkSum = new String(Hex.encodeHex(checkSumGenerator.getCheckSum()));
                logger.info("Checksum for node " + directoryName + " - " + checkSum);

                metadata.add(ReadOnlyStorageMetadata.CHECKSUM, checkSum);
            }
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

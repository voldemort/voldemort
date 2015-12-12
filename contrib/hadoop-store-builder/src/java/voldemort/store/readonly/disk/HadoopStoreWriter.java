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

package voldemort.store.readonly.disk;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.checksum.CheckSumMetadata;
import voldemort.store.readonly.mr.AbstractStoreBuilderConfigurable;
import voldemort.store.readonly.mr.HadoopStoreBuilder;
import voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJob;
import voldemort.utils.ByteUtils;

public class HadoopStoreWriter
        extends AbstractStoreBuilderConfigurable
        implements KeyValueWriter<BytesWritable, BytesWritable> {

    private static final Logger logger = Logger.getLogger(HadoopStoreWriter.class);

    private DataOutputStream[] indexFileStream = null;
    private DataOutputStream[] valueFileStream = null;
    private int[] position;
    private String taskId = null;

    private int nodeId = -1;
    private int partitionId = -1;
    private int replicaType = -1;

    private Path[] taskIndexFileName;
    private Path[] taskValueFileName;

    private JobConf conf;
    private CheckSumType checkSumType;
    private CheckSum[] checkSumDigestIndex;
    private CheckSum[] checkSumDigestValue;

    private String outputDir;

    private FileSystem fs;

    /*
     * This variable is used to figure out the file extension for index and data
     * files. When the server supports compression, this variable's value is
     * typically ".gz" or else it holds and empty string
     */
    private String fileExtension = null;

    /*
     * These variables are used to track the size of the files produced by the
     * reducer
     */
    private long[] indexFileSizeInBytes;
    private long[] valueFileSizeInBytes;

    public HadoopStoreWriter() {

    }

    /**
     * Intended for use by test classes only.
     *
     * @param job
     */
    protected HadoopStoreWriter(JobConf job) {
        this.nodeId = 1;
        this.partitionId = 1;
        this.replicaType = 1;

        conf(job);
    }

    @Override
    public void conf(JobConf job) {
        super.configure(job);

        this.conf = job;
        try {
            this.position = new int[getNumChunks()];
            this.outputDir = job.get("final.output.dir");
            this.taskId = job.get("mapred.task.id");
            this.checkSumType = CheckSum.fromString(job.get(VoldemortBuildAndPushJob.CHECKSUM_TYPE));
            this.checkSumDigestIndex = new CheckSum[getNumChunks()];
            this.checkSumDigestValue = new CheckSum[getNumChunks()];

            initFileStreams(conf.getBoolean(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS, false),
                            conf.get(VoldemortBuildAndPushJob.REDUCER_OUTPUT_COMPRESS_CODEC, NO_COMPRESSION_CODEC));
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    private void initFileStreams(boolean isCompressionEnabled, String compressionCodec)
            throws IOException {
        this.taskIndexFileName = new Path[getNumChunks()];
        this.taskValueFileName = new Path[getNumChunks()];
        this.indexFileStream = new DataOutputStream[getNumChunks()];
        this.valueFileStream = new DataOutputStream[getNumChunks()];
        this.indexFileSizeInBytes = new long[getNumChunks()];
        this.valueFileSizeInBytes = new long[getNumChunks()];

        boolean isValidCompressionEnabled = false;
        if(isCompressionEnabled
                && compressionCodec.toUpperCase(Locale.ENGLISH).equals(this.COMPRESSION_CODEC)) {
            fileExtension = GZIP_FILE_EXTENSION;
            isValidCompressionEnabled = true;
        } else {
            fileExtension = "";
        }

        for(int chunkId = 0; chunkId < getNumChunks(); chunkId++) {

            this.indexFileSizeInBytes[chunkId] = 0L;
            this.valueFileSizeInBytes[chunkId] = 0L;
            this.checkSumDigestIndex[chunkId] = CheckSum.getInstance(checkSumType);
            this.checkSumDigestValue[chunkId] = CheckSum.getInstance(checkSumType);
            this.position[chunkId] = 0;
            this.taskIndexFileName[chunkId] = new Path(FileOutputFormat.getOutputPath(conf),
                                                       getStoreName() + "."
                                                               + Integer.toString(chunkId) + "_"
                                                               + this.taskId + INDEX_FILE_EXTENSION
                                                               + fileExtension);
            this.taskValueFileName[chunkId] = new Path(FileOutputFormat.getOutputPath(conf),
                                                       getStoreName() + "."
                                                               + Integer.toString(chunkId) + "_"
                                                               + this.taskId + DATA_FILE_EXTENSION
                                                               + fileExtension);
            if(this.fs == null)
                this.fs = this.taskIndexFileName[chunkId].getFileSystem(conf);
            if(isValidCompressionEnabled) {
                this.indexFileStream[chunkId] = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(fs.create(this.taskIndexFileName[chunkId]),
                                                                                                                   DEFAULT_BUFFER_SIZE)));
                this.valueFileStream[chunkId] = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(fs.create(this.taskValueFileName[chunkId]),
                                                                                                                   DEFAULT_BUFFER_SIZE)));

            } else {
                this.indexFileStream[chunkId] = fs.create(this.taskIndexFileName[chunkId]);
                this.valueFileStream[chunkId] = fs.create(this.taskValueFileName[chunkId]);

            }
            fs.setPermission(this.taskIndexFileName[chunkId],
                             new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
            logger.info("Setting permission to 755 for " + this.taskIndexFileName[chunkId]);
            fs.setPermission(this.taskValueFileName[chunkId],
                             new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
            logger.info("Setting permission to 755 for " + this.taskValueFileName[chunkId]);

            logger.info("Opening " + this.taskIndexFileName[chunkId] + " and "
                                + this.taskValueFileName[chunkId] + " for writing.");
        }

    }

    @Override
    public void write(BytesWritable key, Iterator<BytesWritable> iterator, Reporter reporter)
            throws IOException {

        // Read chunk id
        int chunkId = ReadOnlyUtils.chunk(key.getBytes(), getNumChunks());

        // Write key and position
        this.indexFileStream[chunkId].write(key.getBytes(), 0, key.getLength());
        this.indexFileSizeInBytes[chunkId] += key.getLength();
        this.indexFileStream[chunkId].writeInt(this.position[chunkId]);
        this.indexFileSizeInBytes[chunkId] += ByteUtils.SIZE_OF_INT;

        // Run key through checksum digest
        if(this.checkSumDigestIndex[chunkId] != null) {
            this.checkSumDigestIndex[chunkId].update(key.getBytes(), 0, key.getLength());
            this.checkSumDigestIndex[chunkId].update(this.position[chunkId]);
        }

        short numTuples = 0;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream valueStream = new DataOutputStream(stream);

        while(iterator.hasNext()) {
            BytesWritable writable = iterator.next();
            byte[] valueBytes = writable.getBytes();
            int offsetTillNow = 0;

            // Read node Id
            if(this.nodeId == -1)
                this.nodeId = ByteUtils.readInt(valueBytes, offsetTillNow);
            offsetTillNow += ByteUtils.SIZE_OF_INT;

            // Read partition id
            if(this.partitionId == -1)
                this.partitionId = ByteUtils.readInt(valueBytes, offsetTillNow);
            offsetTillNow += ByteUtils.SIZE_OF_INT;

            // Read replica type
            if(getSaveKeys()) {
                if(this.replicaType == -1) {
                    this.replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                                 offsetTillNow,
                                                                 ByteUtils.SIZE_OF_BYTE);
                    if (getBuildPrimaryReplicasOnly() && this.replicaType > 0) {
                        throw new IllegalArgumentException("Should not get any replicaType > 0 shuffled to us"
                                                           + " when buildPrimaryReplicasOnly mode is enabled!");
                    }
                }
                offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            }

            int valueLength = writable.getLength() - offsetTillNow;
            if(getSaveKeys()) {
                // Write ( key_length, value_length, key,
                // value )
                valueStream.write(valueBytes, offsetTillNow, valueLength);
            } else {
                // Write (value_length + value)
                valueStream.writeInt(valueLength);
                valueStream.write(valueBytes, offsetTillNow, valueLength);
            }

            numTuples++;

            // If we have multiple values for this md5 that is a collision,
            // throw an exception--either the data itself has duplicates, there
            // are trillions of keys, or someone is attempting something
            // malicious ( We obviously expect collisions when we save keys )
            if(!getSaveKeys() && numTuples > 1)
                throw new VoldemortException("Duplicate keys detected for md5 sum "
                                             + ByteUtils.toHexString(ByteUtils.copy(key.getBytes(),
                                                                                    0,
                                                                                    key.getLength())));

        }

        if(numTuples < 0) {
            // Overflow
            throw new VoldemortException("Found too many collisions: chunk " + chunkId
                                         + " has exceeded " + Short.MAX_VALUE + " collisions.");
        } else if(numTuples > 1) {
            // Update number of collisions + max keys per collision
            reporter.incrCounter(CollisionCounter.NUM_COLLISIONS, 1);

            long numCollisions = reporter.getCounter(CollisionCounter.MAX_COLLISIONS).getCounter();
            if(numTuples > numCollisions) {
                reporter.incrCounter(CollisionCounter.MAX_COLLISIONS, numTuples - numCollisions);
            }
        }

        // Flush the value
        valueStream.flush();
        byte[] value = stream.toByteArray();

        // Start writing to file now
        // First, if save keys flag set the number of keys
        if(getSaveKeys()) {

            this.valueFileStream[chunkId].writeShort(numTuples);
            this.valueFileSizeInBytes[chunkId] += ByteUtils.SIZE_OF_SHORT;
            this.position[chunkId] += ByteUtils.SIZE_OF_SHORT;

            if(this.checkSumDigestValue[chunkId] != null) {
                this.checkSumDigestValue[chunkId].update(numTuples);
            }
        }

        this.valueFileStream[chunkId].write(value);
        this.valueFileSizeInBytes[chunkId] += value.length;
        this.position[chunkId] += value.length;

        if(this.checkSumDigestValue[chunkId] != null) {
            this.checkSumDigestValue[chunkId].update(value);
        }

        if(this.position[chunkId] < 0)
            throw new VoldemortException("Chunk overflow exception: chunk " + chunkId
                                         + " has exceeded " + Integer.MAX_VALUE + " bytes.");

    }

    @Override
    public void close() throws IOException {

        for(int chunkId = 0; chunkId < getNumChunks(); chunkId++) {
            this.indexFileStream[chunkId].close();
            this.valueFileStream[chunkId].close();
        }

        if(this.nodeId == -1 || this.partitionId == -1) {
            // Issue 258 - No data was read in the reduce phase, do not create
            // any output
            return;
        }

        // If the replica type read was not valid, shout out
        if(getSaveKeys() && this.replicaType == -1) {
            throw new RuntimeException("Could not read the replica type correctly for node "
                                       + nodeId + " ( partition - " + this.partitionId + " )");
        }

        String fileNamePrefix = null;
        if(getSaveKeys()) {
            fileNamePrefix = new String(Integer.toString(this.partitionId) + "_"
                                        + Integer.toString(this.replicaType) + "_");
        } else {
            fileNamePrefix = new String(Integer.toString(this.partitionId) + "_");
        }

        // Initialize the output directory
        String outputDirName;
        if (getBuildPrimaryReplicasOnly()) {
            outputDirName = "partition-" + this.partitionId;
        } else {
            outputDirName = "node-" + this.nodeId;
        }
        Path outputDir = new Path(this.outputDir, outputDirName);

        // Create output directory, if it doesn't exist
        FileSystem outputFs = outputDir.getFileSystem(this.conf);
        outputFs.mkdirs(outputDir);
        outputFs.setPermission(outputDir, new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
        logger.info("Setting permission to 755 for " + outputDir);

        // Write the checksum and output files
        for(int chunkId = 0; chunkId < getNumChunks(); chunkId++) {

            String chunkFileName = fileNamePrefix + Integer.toString(chunkId);
            CheckSumMetadata indexCheckSum = new CheckSumMetadata();
            CheckSumMetadata valueCheckSum = new CheckSumMetadata();
            if(this.checkSumType != CheckSumType.NONE) {

                if(this.checkSumDigestIndex[chunkId] != null
                        && this.checkSumDigestValue[chunkId] != null) {
                    indexCheckSum.add(ReadOnlyStorageMetadata.CHECKSUM,
                                      new String(Hex.encodeHex(this.checkSumDigestIndex[chunkId].getCheckSum())));
                    valueCheckSum.add(ReadOnlyStorageMetadata.CHECKSUM,
                                      new String(Hex.encodeHex(this.checkSumDigestValue[chunkId].getCheckSum())));
                } else {
                    throw new RuntimeException("Failed to open checksum digest for node " + nodeId
                                               + " ( partition - " + this.partitionId
                                               + ", chunk - " + chunkId + " )");
                }
            }

            Path checkSumIndexFile = new Path(outputDir, chunkFileName + INDEX_FILE_EXTENSION
                    + CHECKSUM_FILE_EXTENSION);
            Path checkSumValueFile = new Path(outputDir, chunkFileName + DATA_FILE_EXTENSION
                    + CHECKSUM_FILE_EXTENSION);

            if(outputFs.exists(checkSumIndexFile)) {
                outputFs.delete(checkSumIndexFile, true);
            }
            FSDataOutputStream output = outputFs.create(checkSumIndexFile);
            outputFs.setPermission(checkSumIndexFile,
                                   new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
            indexCheckSum.add(CheckSumMetadata.INDEX_FILE_SIZE_IN_BYTES,
                              Long.toString(this.indexFileSizeInBytes[chunkId]));
            output.write(indexCheckSum.toJsonString().getBytes());
            output.close();

            if(outputFs.exists(checkSumValueFile)) {
                outputFs.delete(checkSumValueFile, true);
            }
            output = outputFs.create(checkSumValueFile);
            outputFs.setPermission(checkSumValueFile,
                                   new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
            valueCheckSum.add(CheckSumMetadata.DATA_FILE_SIZE_IN_BYTES,
                              Long.toString(this.valueFileSizeInBytes[chunkId]));
            output.write(valueCheckSum.toJsonString().getBytes());
            output.close();

            // Generate the final chunk files and add file size information
            Path indexFile = new Path(outputDir, chunkFileName + INDEX_FILE_EXTENSION + fileExtension);
            Path valueFile = new Path(outputDir, chunkFileName + DATA_FILE_EXTENSION + fileExtension);

            logger.info("Moving " + this.taskIndexFileName[chunkId] + " to " + indexFile);
            if(outputFs.exists(indexFile)) {
                outputFs.delete(indexFile, true);
            }
            fs.rename(taskIndexFileName[chunkId], indexFile);

            logger.info("Moving " + this.taskValueFileName[chunkId] + " to " + valueFile);
            if(outputFs.exists(valueFile)) {
                outputFs.delete(valueFile, true);
            }
            fs.rename(this.taskValueFileName[chunkId], valueFile);
        }
    }
}
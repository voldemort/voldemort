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
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

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
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.HadoopStoreBuilder;
import voldemort.utils.ByteUtils;
import voldemort.xml.StoreDefinitionsMapper;

// generates index and data files
public class HadoopStoreWriter implements KeyValueWriter<BytesWritable, BytesWritable> {

    private static final Logger logger = Logger.getLogger(HadoopStoreWriter.class);

    private DataOutputStream indexFileStream = null;
    private DataOutputStream valueFileStream = null;
    private int position;
    private String taskId = null;

    private int nodeId = -1;
    private int partitionId = -1;
    private int chunkId = -1;
    private int replicaType = -1;

    private Path taskIndexFileName;
    private Path taskValueFileName;

    private JobConf conf;
    private CheckSumType checkSumType;
    private CheckSum checkSumDigestIndex;
    private CheckSum checkSumDigestValue;

    private String outputDir;

    private FileSystem fs;

    private int numChunks;
    private StoreDefinition storeDef;
    private boolean saveKeys;

    /*
     * This variable is used to figure out the file extension for index and data
     * files. When the server supports compression, this variable's value is
     * typically ".gz" or else it holds and empty string
     */
    private String fileExtension = null;

    public boolean getSaveKeys() {
        return this.saveKeys;
    }

    public String getStoreName() {
        checkNotNull(storeDef);
        return storeDef.getName();
    }

    private final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }

    public int getNumChunks() {
        return this.numChunks;
    }

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
        this.chunkId = 1;
        this.replicaType = 1;

        conf(job);
    }

    @Override
    public void conf(JobConf job) {

        conf = job;
        try {
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
            if(storeDefs.size() != 1)
                throw new IllegalStateException("Expected to find only a single store, but found multiple!");
            this.storeDef = storeDefs.get(0);

            this.numChunks = conf.getInt("num.chunks", -1);
            if(this.numChunks < 1)
                throw new VoldemortException("num.chunks not specified in the job conf.");
            this.saveKeys = conf.getBoolean("save.keys", false);
            this.position = 0;
            this.outputDir = job.get("final.output.dir");
            this.taskId = job.get("mapred.task.id");
            this.checkSumType = CheckSum.fromString(job.get("checksum.type"));
            this.checkSumDigestIndex = CheckSum.getInstance(checkSumType);
            this.checkSumDigestValue = CheckSum.getInstance(checkSumType);

            initFileStreams(conf.getBoolean("reducer.output.compress", false),
                            conf.get("reducer.output.compress.codec", NO_COMPRESSION_CODEC));

        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }

    }

    private void initFileStreams(boolean isCompressionEnabled, String compressionCodec)
            throws IOException {

        fileExtension = (isCompressionEnabled && compressionCodec.toUpperCase(Locale.ENGLISH)
                                                                 .equals(COMPRESSION_CODEC)) ? GZIP_FILE_EXTENSION
                                                                                                 : "";
        this.taskIndexFileName = new Path(FileOutputFormat.getOutputPath(conf), getStoreName()
                                                                                + "." + this.taskId
 + INDEX_FILE_EXTENSION
                                                                                + fileExtension);
        this.taskValueFileName = new Path(FileOutputFormat.getOutputPath(conf), getStoreName()
                                                                                + "." + this.taskId
 + DATA_FILE_EXTENSION
                                                                                + fileExtension);
        if(this.fs == null)
            this.fs = this.taskIndexFileName.getFileSystem(conf);

        if(isCompressionEnabled
           && compressionCodec.toUpperCase(Locale.ENGLISH).equals(COMPRESSION_CODEC)) {
            this.indexFileStream = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(fs.create(this.taskIndexFileName),
                                                                                                      DEFAULT_BUFFER_SIZE)));
            this.valueFileStream = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(fs.create(this.taskValueFileName),
                                                                                                      DEFAULT_BUFFER_SIZE)));

        } else {
            this.indexFileStream = fs.create(this.taskIndexFileName);
            this.valueFileStream = fs.create(this.taskValueFileName);

        }

        fs.setPermission(this.taskIndexFileName,
                         new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
        logger.info("Setting permission to 755 for " + this.taskIndexFileName);
        fs.setPermission(this.taskValueFileName,
                         new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
        logger.info("Setting permission to 755 for " + this.taskValueFileName);

        logger.info("Opening " + this.taskIndexFileName + " and " + this.taskValueFileName
                    + " for writing.");

    }

    @Override
    public void write(BytesWritable key, Iterator<BytesWritable> iterator, Reporter reporter)
            throws IOException {

        // Write key and position
        this.indexFileStream.write(key.get(), 0, key.getSize());
        this.indexFileStream.writeInt(this.position);

        // Run key through checksum digest
        if(this.checkSumDigestIndex != null) {
            this.checkSumDigestIndex.update(key.get(), 0, key.getSize());
            this.checkSumDigestIndex.update(this.position);
        }

        short numTuples = 0;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream valueStream = new DataOutputStream(stream);

        while(iterator.hasNext()) {
            BytesWritable writable = iterator.next();
            byte[] valueBytes = writable.get();
            int offsetTillNow = 0;

            // Read node Id
            if(this.nodeId == -1)
                this.nodeId = ByteUtils.readInt(valueBytes, offsetTillNow);
            offsetTillNow += ByteUtils.SIZE_OF_INT;

            // Read partition id
            if(this.partitionId == -1)
                this.partitionId = ByteUtils.readInt(valueBytes, offsetTillNow);
            offsetTillNow += ByteUtils.SIZE_OF_INT;

            // Read chunk id
            if(this.chunkId == -1)
                this.chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());

            // Read replica type
            if(getSaveKeys()) {
                if(this.replicaType == -1)
                    this.replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                                 offsetTillNow,
                                                                 ByteUtils.SIZE_OF_BYTE);
                offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            }

            int valueLength = writable.getSize() - offsetTillNow;
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
                                             + ByteUtils.toHexString(ByteUtils.copy(key.get(),
                                                                                    0,
                                                                                    key.getSize())));

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

            this.valueFileStream.writeShort(numTuples);
            this.position += ByteUtils.SIZE_OF_SHORT;

            if(this.checkSumDigestValue != null) {
                this.checkSumDigestValue.update(numTuples);
            }
        }

        this.valueFileStream.write(value);
        this.position += value.length;

        if(this.checkSumDigestValue != null) {
            this.checkSumDigestValue.update(value);
        }

        if(this.position < 0)
            throw new VoldemortException("Chunk overflow exception: chunk " + chunkId
                                         + " has exceeded " + Integer.MAX_VALUE + " bytes.");
    }

    @Override
    public void close() throws IOException {

        this.indexFileStream.close();
        this.valueFileStream.close();

        if(this.nodeId == -1 || this.chunkId == -1 || this.partitionId == -1) {
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
                                        + Integer.toString(this.replicaType) + "_"
                                        + Integer.toString(this.chunkId));
        } else {
            fileNamePrefix = new String(Integer.toString(this.partitionId) + "_"
                                        + Integer.toString(this.chunkId));
        }

        // Initialize the node directory
        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);

        // Create output directory, if it doesn't exist
        FileSystem outputFs = nodeDir.getFileSystem(this.conf);
        outputFs.mkdirs(nodeDir);
        outputFs.setPermission(nodeDir, new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
        logger.info("Setting permission to 755 for " + nodeDir);

        // Write the checksum and output files
        if(this.checkSumType != CheckSumType.NONE) {

            if(this.checkSumDigestIndex != null && this.checkSumDigestValue != null) {
                Path checkSumIndexFile = new Path(nodeDir, fileNamePrefix + INDEX_FILE_EXTENSION
                                                           + CHECKSUM_FILE_EXTENSION);
                Path checkSumValueFile = new Path(nodeDir, fileNamePrefix + DATA_FILE_EXTENSION
                                                           + CHECKSUM_FILE_EXTENSION);

                if(outputFs.exists(checkSumIndexFile)) {
                    outputFs.delete(checkSumIndexFile);
                }
                FSDataOutputStream output = outputFs.create(checkSumIndexFile);
                outputFs.setPermission(checkSumIndexFile,
                                       new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
                output.write(this.checkSumDigestIndex.getCheckSum());
                output.close();

                if(outputFs.exists(checkSumValueFile)) {
                    outputFs.delete(checkSumValueFile);
                }
                output = outputFs.create(checkSumValueFile);
                outputFs.setPermission(checkSumValueFile,
                                       new FsPermission(HadoopStoreBuilder.HADOOP_FILE_PERMISSION));
                output.write(this.checkSumDigestValue.getCheckSum());
                output.close();
            } else {
                throw new RuntimeException("Failed to open checksum digest for node " + nodeId
                                           + " ( partition - " + this.partitionId + ", chunk - "
                                           + chunkId + " )");
            }
        }

        // Generate the final chunk files
        Path indexFile = new Path(nodeDir, fileNamePrefix + INDEX_FILE_EXTENSION + fileExtension);
        Path valueFile = new Path(nodeDir, fileNamePrefix + DATA_FILE_EXTENSION + fileExtension);

        logger.info("Moving " + this.taskIndexFileName + " to " + indexFile);
        if(outputFs.exists(indexFile)) {
            outputFs.delete(indexFile);
        }
        outputFs.rename(taskIndexFileName, indexFile);

        logger.info("Moving " + this.taskValueFileName + " to " + valueFile);
        if(outputFs.exists(valueFile)) {
            outputFs.delete(valueFile);
        }
        outputFs.rename(this.taskValueFileName, valueFile);
    }

}

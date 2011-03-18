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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderReducer extends AbstractStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilderReducer.class);

    private DataOutputStream indexFileStream = null;
    private DataOutputStream valueFileStream = null;
    private int position = 0;
    private String taskId = null;

    private int nodeId = -1;
    private int chunkId = -1;
    private int partitionId = -1;
    private int replicaType = -1;

    private Path taskIndexFileName;
    private Path taskValueFileName;
    private String outputDir;
    private JobConf conf;
    private CheckSumType checkSumType;
    private CheckSum checkSumDigestIndex;
    private CheckSum checkSumDigestValue;

    protected static enum CollisionCounter {
        NUM_COLLISIONS,
        MAX_COLLISIONS;
    }

    /**
     * Reduce should get sorted MD5 of Voldemort key ( either 16 bytes if saving
     * keys is disabled, else 8 bytes ) as key and for value (a) node-id,
     * partition-id, value - if saving keys is disabled (b) node-id,
     * partition-id, [key-size, key, value-size, value]* if saving keys is
     * enabled
     */
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> iterator,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {

        // Write key and position
        this.indexFileStream.write(key.get(), 0, key.getSize());
        this.indexFileStream.writeInt(this.position);

        // Run key through checksum digest
        if(this.checkSumDigestIndex != null) {
            this.checkSumDigestIndex.update(key.get(), 0, key.getSize());
            this.checkSumDigestIndex.update(this.position);
        }

        int numKeyValues = 0;
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

            // Read replica type
            if(getSaveKeys()) {
                if(this.replicaType == -1)
                    this.replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                                 offsetTillNow,
                                                                 ByteUtils.SIZE_OF_BYTE);
                offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            }

            // Read chunk id
            if(this.chunkId == -1)
                this.chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());

            int valueLength = writable.getSize() - offsetTillNow;
            if(getSaveKeys()) {
                // Write (key_length + key + value_length + value)
                valueStream.write(valueBytes, offsetTillNow, valueLength);
            } else {
                // Write (value_length + value)
                valueStream.writeInt(valueLength);
                valueStream.write(valueBytes, offsetTillNow, valueLength);
            }

            numKeyValues++;

            // If we have multiple values for this md5 that is a collision,
            // throw an exception--either the data itself has duplicates, there
            // are trillions of keys, or someone is attempting something
            // malicious ( We obviously expect collisions when we save keys )
            if(!getSaveKeys() && numKeyValues > 1)
                throw new VoldemortException("Duplicate keys detected for md5 sum "
                                             + ByteUtils.toHexString(ByteUtils.copy(key.get(),
                                                                                    0,
                                                                                    key.getSize())));

        }

        // Update number of collisions + max keys per collision
        if(numKeyValues > 1) {
            reporter.incrCounter(CollisionCounter.NUM_COLLISIONS, 1);

            long numCollisions = reporter.getCounter(CollisionCounter.MAX_COLLISIONS).getCounter();
            if(numKeyValues > numCollisions) {
                reporter.incrCounter(CollisionCounter.MAX_COLLISIONS, numKeyValues - numCollisions);
            }
        }

        // Start writing to file now
        // First, if save keys then write number of keys

        if(getSaveKeys()) {
            // Write the number of KVs as a single byte
            byte[] numBuf = new byte[ByteUtils.SIZE_OF_BYTE];
            ByteUtils.writeBytes(numBuf, numKeyValues, 0, ByteUtils.SIZE_OF_BYTE);

            this.valueFileStream.write(numBuf);
            this.position += ByteUtils.SIZE_OF_BYTE;

            if(this.checkSumDigestValue != null) {
                this.checkSumDigestValue.update(numBuf);
            }
        }

        // Now, write the actual value to the file
        valueStream.flush();

        byte[] value = stream.toByteArray();
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
    public void configure(JobConf job) {
        super.configure(job);
        try {
            this.conf = job;
            this.position = 0;
            this.outputDir = job.get("final.output.dir");
            this.taskId = job.get("mapred.task.id");
            this.checkSumType = CheckSum.fromString(job.get("checksum.type"));
            this.checkSumDigestIndex = CheckSum.getInstance(checkSumType);
            this.checkSumDigestValue = CheckSum.getInstance(checkSumType);

            this.taskIndexFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName()
                                                                                   + "."
                                                                                   + this.taskId
                                                                                   + ".index");
            this.taskValueFileName = new Path(FileOutputFormat.getOutputPath(job), getStoreName()
                                                                                   + "."
                                                                                   + this.taskId
                                                                                   + ".data");

            logger.info("Opening " + this.taskIndexFileName + " and " + this.taskValueFileName
                        + " for writing.");
            FileSystem fs = this.taskIndexFileName.getFileSystem(job);
            this.indexFileStream = fs.create(this.taskIndexFileName);
            this.valueFileStream = fs.create(this.taskValueFileName);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
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
                                       + nodeId + " ( partition - " + this.partitionId
                                       + ", chunk - " + this.chunkId + " )");
        }

        String fileName = null;
        if(getSaveKeys()) {
            fileName = new String(Integer.toString(this.partitionId) + "_"
                                  + Integer.toString(this.replicaType) + "_"
                                  + Integer.toString(this.chunkId));
        } else {
            fileName = new String(Integer.toString(this.partitionId) + "_"
                                  + Integer.toString(this.chunkId));
        }

        // Initialize the final files
        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);
        Path indexFile = new Path(nodeDir, fileName + ".index");
        Path valueFile = new Path(nodeDir, fileName + ".data");

        // Create output directory [ if it doesn't exist ]
        FileSystem fs = indexFile.getFileSystem(this.conf);
        fs.mkdirs(nodeDir);

        if(this.checkSumType != CheckSumType.NONE) {
            if(this.checkSumDigestIndex != null && this.checkSumDigestValue != null) {
                Path checkSumIndexFile = new Path(nodeDir, fileName + ".index.checksum");
                Path checkSumValueFile = new Path(nodeDir, fileName + ".data.checksum");

                FSDataOutputStream output = fs.create(checkSumIndexFile);
                output.write(this.checkSumDigestIndex.getCheckSum());
                output.close();

                output = fs.create(checkSumValueFile);
                output.write(this.checkSumDigestValue.getCheckSum());
                output.close();
            } else {
                throw new RuntimeException("Failed to open checksum digest for node " + nodeId
                                           + " ( partition - " + this.partitionId + " )");
            }
        }

        logger.info("Moving " + this.taskIndexFileName + " to " + indexFile + ".");
        fs.rename(taskIndexFileName, indexFile);
        logger.info("Moving " + this.taskValueFileName + " to " + valueFile + ".");
        fs.rename(this.taskValueFileName, valueFile);
    }
}

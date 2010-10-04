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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 * 
 * 
 */
public class HadoopStoreBuilderReducer extends Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilderReducer.class);

    private DataOutputStream indexFileStream = null;
    private DataOutputStream valueFileStream = null;
    private int position = 0;
    private String taskId = null;
    private int numChunks = -1;
    private int nodeId = -1;
    private int chunkId = -1;
    private int partitionId = -1;
    private Path taskIndexFileName;
    private Path taskValueFileName;
    private String outputDir;
    private Configuration conf;
    private CheckSumType checkSumType;
    private CheckSum checkSumDigestIndex;
    private CheckSum checkSumDigestValue;

    /**
     * Reduce should get sorted MD5 keys here with a single value (appended in
     * beginning with 4 bits of nodeId)
     */
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<BytesWritable> iterator = values.iterator();
        BytesWritable writable = iterator.next();
        byte[] valueBytes = writable.getBytes();

        if(this.nodeId == -1)
            this.nodeId = ByteUtils.readInt(valueBytes, 0);
        if(this.partitionId == -1)
            this.partitionId = ByteUtils.readInt(valueBytes, 4);
        if(this.chunkId == -1)
            this.chunkId = ReadOnlyUtils.chunk(key.getBytes(), this.numChunks);

        // Write key and position
        this.indexFileStream.write(key.getBytes(), 0, key.getLength());
        this.indexFileStream.writeInt(this.position);
        if(this.checkSumDigestIndex != null) {
            this.checkSumDigestIndex.update(key.getBytes(), 0, key.getLength());
            this.checkSumDigestIndex.update(this.position);
        }

        // Write length and value
        int valueLength = writable.getLength() - 8;
        this.valueFileStream.writeInt(valueLength);
        this.valueFileStream.write(valueBytes, 8, valueLength);
        if(this.checkSumDigestValue != null) {
            this.checkSumDigestValue.update(valueLength);
            this.checkSumDigestValue.update(valueBytes, 8, valueLength);
        }
        this.position += 4 + valueLength;
        if(this.position < 0)
            throw new VoldemortException("Chunk overflow exception: chunk " + chunkId
                                         + " has exceeded " + Integer.MAX_VALUE + " bytes.");

        // if we have multiple values for this md5 that is a collision, throw an
        // exception--either the data itself has duplicates, there are trillions
        // of keys, or someone is attempting something malicious
        if(iterator.hasNext())
            throw new VoldemortException("Duplicate keys detected for md5 sum "
                                         + ByteUtils.toHexString(ByteUtils.copy(key.getBytes(),
                                                                                0,
                                                                                key.getLength())));
    }

    @Override
    public void setup(Context context) {
        try {
            this.conf = context.getConfiguration();
            this.position = 0;
            this.numChunks = conf.getInt("num.chunks", -1);
            this.outputDir = conf.get("final.output.dir");
            this.taskId = conf.get("mapred.task.id");
            this.checkSumType = CheckSum.fromString(conf.get("checksum.type"));
            this.checkSumDigestIndex = CheckSum.getInstance(checkSumType);
            this.checkSumDigestValue = CheckSum.getInstance(checkSumType);

            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
            if(storeDefs.size() != 1)
                throw new IllegalStateException("Expected to find only a single store, but found multiple!");
            String storeName = storeDefs.get(0).getName();

            this.taskIndexFileName = new Path(FileOutputFormat.getOutputPath(context),
                                              storeName + "." + this.taskId + ".index");
            this.taskValueFileName = new Path(FileOutputFormat.getOutputPath(context),
                                              storeName + "." + this.taskId + ".data");

            logger.info("Opening " + this.taskIndexFileName + " and " + this.taskValueFileName
                        + " for writing.");
            FileSystem fs = this.taskIndexFileName.getFileSystem(conf);
            this.indexFileStream = fs.create(this.taskIndexFileName);
            this.valueFileStream = fs.create(this.taskValueFileName);
        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException {
        this.indexFileStream.close();
        this.valueFileStream.close();

        if(this.nodeId == -1 || this.chunkId == -1 || this.partitionId == -1) {
            // No data was read in the reduce phase, do not create any output
            // directory (Also Issue 258)
            return;
        }
        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);
        Path indexFile = new Path(nodeDir, this.partitionId + "_" + this.chunkId + ".index");
        Path valueFile = new Path(nodeDir, this.partitionId + "_" + this.chunkId + ".data");

        // create output directory
        FileSystem fs = indexFile.getFileSystem(this.conf);
        fs.mkdirs(nodeDir);

        if(this.checkSumType != CheckSumType.NONE) {
            if(this.checkSumDigestIndex != null && this.checkSumDigestValue != null) {
                Path checkSumIndexFile = new Path(nodeDir, this.partitionId + "_" + this.chunkId
                                                           + ".index.checksum");
                Path checkSumValueFile = new Path(nodeDir, this.partitionId + "_" + this.chunkId
                                                           + ".data.checksum");

                FSDataOutputStream output = fs.create(checkSumIndexFile);
                output.write(this.checkSumDigestIndex.getCheckSum());
                output.close();

                output = fs.create(checkSumValueFile);
                output.write(this.checkSumDigestValue.getCheckSum());
                output.close();
            } else {
                throw new VoldemortException("Failed to open CheckSum digest");
            }
        }

        logger.info("Moving " + this.taskIndexFileName + " to " + indexFile + ".");
        fs.rename(taskIndexFileName, indexFile);
        logger.info("Moving " + this.taskValueFileName + " to " + valueFile + ".");
        fs.rename(this.taskValueFileName, valueFile);
    }
}

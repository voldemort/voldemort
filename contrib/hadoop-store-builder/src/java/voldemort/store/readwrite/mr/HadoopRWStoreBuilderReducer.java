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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.store.readonly.mr.AbstractStoreBuilderConfigurable;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

@SuppressWarnings("deprecation")
public class HadoopRWStoreBuilderReducer extends AbstractStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private static final Logger logger = Logger.getLogger(HadoopRWStoreBuilderReducer.class);

    private AdminClient client;
    private int nodeId, chunkId;
    private long totalBytes;
    private long transferStartTime;
    private int sizeInt = ByteUtils.SIZE_OF_INT;

    protected static enum RecordCounter {
        RECORDS_STREAMED;
    }

    public void reduce(BytesWritable key,
                       final Iterator<BytesWritable> values,
                       OutputCollector<BytesWritable, BytesWritable> output,
                       final Reporter reporter) throws IOException {
        this.transferStartTime = System.nanoTime();

        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                while(values.hasNext()) {
                    BytesWritable keyValue = values.next();
                    byte[] keyValueBytes = new byte[keyValue.get().length];
                    System.arraycopy(keyValue.get(), 0, keyValueBytes, 0, keyValue.get().length);

                    // Reading key
                    int keyBytesLength = ByteUtils.readInt(keyValueBytes, 0);
                    byte[] keyBytes = new byte[keyBytesLength];
                    System.arraycopy(keyValueBytes, sizeInt, keyBytes, 0, keyBytesLength);

                    // Reading value
                    int valueBytesLength = ByteUtils.readInt(keyValueBytes, sizeInt
                                                                            + keyBytesLength);
                    byte[] valueBytes = new byte[valueBytesLength];
                    System.arraycopy(keyValueBytes,
                                     sizeInt + sizeInt + keyBytesLength,
                                     valueBytes,
                                     0,
                                     valueBytesLength);

                    // Reading vector clock
                    int vectorClockBytesLength = ByteUtils.readInt(keyValueBytes,
                                                                   sizeInt + sizeInt
                                                                           + keyBytesLength
                                                                           + valueBytesLength);
                    byte[] vectorClockBytes = new byte[vectorClockBytesLength];
                    System.arraycopy(keyValueBytes,
                                     sizeInt + sizeInt + sizeInt + keyBytesLength
                                             + valueBytesLength,
                                     vectorClockBytes,
                                     0,
                                     vectorClockBytesLength);
                    VectorClock vectorClock = new VectorClock(vectorClockBytes);

                    totalBytes += (keyBytesLength + valueBytesLength + vectorClockBytesLength);

                    // Generating output
                    ByteArray key = new ByteArray(keyBytes);
                    Versioned<byte[]> versioned = Versioned.value(valueBytes, vectorClock);

                    reporter.incrCounter(RecordCounter.RECORDS_STREAMED, 1);
                    return new Pair<ByteArray, Versioned<byte[]>>(key, versioned);
                }
                return endOfData();
            }
        };
        logger.info("Connecting to admin client on " + this.nodeId + " - chunk id - "
                    + this.chunkId);
        this.client.updateEntries(this.nodeId, getStoreName(), iterator, null);
        logger.info("Completed transfer of chunk id " + this.chunkId + " to node " + this.nodeId);
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        this.totalBytes = 0;
        this.chunkId = job.getInt("mapred.task.partition", -1); // http://www.mail-archive.com/core-user@hadoop.apache.org/msg05749.html

        if(this.chunkId < 0) {
            throw new RuntimeException("Incorrect chunk id ");
        }
        this.nodeId = this.chunkId / getNumChunks();

        this.client = new AdminClient(getCluster(), new AdminClientConfig());
        logger.info("Reducer for Node id - " + this.nodeId + " and chunk id - " + this.chunkId);
    }

    @Override
    public void close() throws IOException {
        this.client.stop();
        logger.info("Finished partition "
                    + this.nodeId
                    + " and chunkId "
                    + this.chunkId
                    + " - "
                    + ((double) (this.totalBytes * 1000.0) / (double) (System.nanoTime() - this.transferStartTime))
                    + " MBps");
    }
}

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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

@SuppressWarnings("deprecation")
public class HadoopRWStoreBuilderReducer extends AbstractStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private static final Logger logger = Logger.getLogger(HadoopRWStoreBuilderReducer.class);

    private AdminClient client;
    private int nodeId, chunkId;
    private Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator;
    private long totalBytes;
    private long transferStartTime;
    private int sizeInt = ByteUtils.SIZE_OF_INT;
    private VectorClock vectorClock;

    protected static enum RecordCounter {
        RECORDS_STREAMED;
    }

    public void reduce(BytesWritable key,
                       final Iterator<BytesWritable> values,
                       OutputCollector<BytesWritable, BytesWritable> output,
                       final Reporter reporter) throws IOException {
        this.transferStartTime = System.nanoTime();

        this.iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                while(values.hasNext()) {
                    BytesWritable keyValue = values.next();
                    byte[] tempKeyValueBytes = new byte[keyValue.get().length];
                    System.arraycopy(keyValue.get(), 0, tempKeyValueBytes, 0, keyValue.get().length);

                    // Reading key
                    int keyBytesLength = ByteUtils.readInt(tempKeyValueBytes, 0);
                    byte[] keyBytes = new byte[keyBytesLength];
                    System.arraycopy(tempKeyValueBytes, sizeInt, keyBytes, 0, keyBytesLength);

                    // Reading value
                    int valueBytesLength = ByteUtils.readInt(tempKeyValueBytes, sizeInt
                                                                                + keyBytesLength);
                    byte[] valueBytes = new byte[valueBytesLength];
                    System.arraycopy(tempKeyValueBytes,
                                     sizeInt + sizeInt + keyBytesLength,
                                     valueBytes,
                                     0,
                                     valueBytesLength);

                    totalBytes += (keyBytesLength + valueBytesLength);
                    ByteArray key = new ByteArray(keyBytes);
                    Versioned<byte[]> versioned = Versioned.value(valueBytes, vectorClock);
                    reporter.incrCounter(RecordCounter.RECORDS_STREAMED, 1);
                    return new Pair<ByteArray, Versioned<byte[]>>(key, versioned);
                }
                return endOfData();
            }
        };
        this.client.updateEntries(this.nodeId, getStoreName(), this.iterator, null);
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        this.totalBytes = 0;
        this.chunkId = job.getInt("mapred.task.partition", -1); // http://www.mail-archive.com/core-user@hadoop.apache.org/msg05749.html
        int hadoopNodeId = job.getInt("hadoop.node.id", -1);
        long hadoopPushVersion = job.getLong("hadoop.push.version", -1L);
        long jobStartTime = job.getLong("job.start.time.ms", -1);
        if(this.chunkId < 0 || hadoopPushVersion < 0 || hadoopNodeId < 0 || jobStartTime < 0) {
            throw new RuntimeException("Incorrect chunk id / hadoop push version / hadoop node id / job start time");
        }
        this.nodeId = this.chunkId / getNumChunks();

        List<ClockEntry> versions = new ArrayList<ClockEntry>();
        versions.add(0, new ClockEntry((short) hadoopNodeId, hadoopPushVersion));
        vectorClock = new VectorClock(versions, jobStartTime);

        logger.info("Working on Node id - " + this.nodeId + " and chunk id - " + this.chunkId);
        this.client = new AdminClient(getCluster(), new AdminClientConfig());
        logger.info("Connected to admin client on " + this.nodeId);
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

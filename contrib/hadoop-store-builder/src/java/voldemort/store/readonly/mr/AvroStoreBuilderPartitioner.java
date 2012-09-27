package voldemort.store.readonly.mr;

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

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce (and hence in the same store chunk)
 */
@SuppressWarnings("deprecation")
public class AvroStoreBuilderPartitioner implements
        Partitioner<AvroKey<ByteBuffer>, AvroValue<ByteBuffer>> {

    @Override
    public int getPartition(AvroKey<ByteBuffer> key, AvroValue<ByteBuffer> value, int numReduceTasks) {

        byte[] keyBytes = null, valueBytes;

        if(!key.datum().hasRemaining()) {
            // System.out.println("empty key");
            return 0;
        }
        keyBytes = new byte[key.datum().remaining()];
        key.datum().get(keyBytes);

        if(!value.datum().hasRemaining()) {
            // System.out.println("empty value");
            return 0;
        }
        valueBytes = new byte[value.datum().remaining()];
        value.datum().get(valueBytes);

        ByteBuffer keyBuffer = null, valueBuffer = null;

        keyBuffer = ByteBuffer.allocate(keyBytes.length);
        keyBuffer.put(keyBytes);
        keyBuffer.rewind();

        valueBuffer = ByteBuffer.allocate(valueBytes.length);
        valueBuffer.put(valueBytes);
        valueBuffer.rewind();

        key.datum(keyBuffer);
        value.datum(valueBuffer);

        int partitionId = ByteUtils.readInt(valueBytes, ByteUtils.SIZE_OF_INT);
        int chunkId = ReadOnlyUtils.chunk(keyBytes, getNumChunks());
        if(getSaveKeys()) {
            int replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                        2 * ByteUtils.SIZE_OF_INT,
                                                        ByteUtils.SIZE_OF_BYTE);
            if(getReducerPerBucket()) {
                return (partitionId * getStoreDef().getReplicationFactor() + replicaType)
                       % numReduceTasks;
            } else {
                return ((partitionId * getStoreDef().getReplicationFactor() * getNumChunks())
                        + (replicaType * getNumChunks()) + chunkId)
                       % numReduceTasks;
            }
        } else {
            if(getReducerPerBucket()) {
                return partitionId % numReduceTasks;
            } else {
                return (partitionId * getNumChunks() + chunkId) % numReduceTasks;
            }

        }
    }

    private int numChunks;
    private Cluster cluster;
    private StoreDefinition storeDef;
    private boolean saveKeys;
    private boolean reducerPerBucket;

    @Override
    public void configure(JobConf conf) {
        this.cluster = new ClusterMapper().readCluster(new StringReader(conf.get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);

        this.numChunks = conf.getInt("num.chunks", -1);
        if(this.numChunks < 1)
            throw new VoldemortException("num.chunks not specified in the job conf.");

        this.saveKeys = conf.getBoolean("save.keys", false);
        this.reducerPerBucket = conf.getBoolean("reducer.per.bucket", false);
    }

    @SuppressWarnings("unused")
    public void close() throws IOException {}

    public Cluster getCluster() {
        checkNotNull(cluster);
        return cluster;
    }

    public boolean getSaveKeys() {
        return this.saveKeys;
    }

    public boolean getReducerPerBucket() {
        return this.reducerPerBucket;
    }

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
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

}

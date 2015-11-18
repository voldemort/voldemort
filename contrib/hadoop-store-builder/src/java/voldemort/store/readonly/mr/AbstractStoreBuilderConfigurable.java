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
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJob;
import voldemort.utils.ByteUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * A base class with basic configuration values and utility functions
 * shared by the mapper, reducer, and partitioner
 */
abstract public class AbstractStoreBuilderConfigurable {

    private int numChunks;
    private Cluster cluster;
    private StoreDefinition storeDef;
    private boolean saveKeys;
    private boolean reducerPerBucket;
    private boolean buildPrimaryReplicasOnly;

    public void configure(JobConf conf) {
        this.cluster = new ClusterMapper().readCluster(new StringReader(conf.get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);

        this.numChunks = conf.getInt(VoldemortBuildAndPushJob.NUM_CHUNKS, -1);
        if(this.numChunks < 1)
            throw new VoldemortException(VoldemortBuildAndPushJob.NUM_CHUNKS + " not specified in the job conf.");

        this.saveKeys = conf.getBoolean(VoldemortBuildAndPushJob.SAVE_KEYS, false);
        this.reducerPerBucket = conf.getBoolean(VoldemortBuildAndPushJob.REDUCER_PER_BUCKET, false);
        this.buildPrimaryReplicasOnly = conf.getBoolean(VoldemortBuildAndPushJob.BUILD_PRIMARY_REPLICAS_ONLY, false);
        if (buildPrimaryReplicasOnly && !saveKeys) {
            throw new IllegalStateException(VoldemortBuildAndPushJob.BUILD_PRIMARY_REPLICAS_ONLY + " can only be true if " +
                                            VoldemortBuildAndPushJob.SAVE_KEYS + " is also true.");
        }
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

    public boolean getBuildPrimaryReplicasOnly() {
        return this.buildPrimaryReplicasOnly;
    }

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
    }

    public String getStoreName() {
        checkNotNull(storeDef);
        return storeDef.getName();
    }

    protected final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }

    public int getNumChunks() {
        return this.numChunks;
    }

    public int getPartition(byte[] key,
                            byte[] value,
                            int numReduceTasks) {
        try {
            int partitionId = ByteUtils.readInt(value, ByteUtils.SIZE_OF_INT);
            int chunkId = ReadOnlyUtils.chunk(key, getNumChunks());
            if(getSaveKeys()) {
                int replicaType = (int) ByteUtils.readBytes(value,
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
        } catch (Exception e) {
            throw new VoldemortException("Caught exception in getPartition()!" +
                                         " key: " + ByteUtils.toHexString(key) +
                                         ", value: " + ByteUtils.toHexString(value) +
                                         ", numReduceTasks: " + numReduceTasks, e);
        }
    }
}

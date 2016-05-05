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

    public static final String NUM_CHUNKS = "num.chunks";

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

        this.numChunks = conf.getInt(NUM_CHUNKS, -1);
        if(this.numChunks < 1) {
            // A bit of defensive code for good measure, but should never happen anymore, now that the config cannot
            // be overridden by the user.
            throw new VoldemortException(NUM_CHUNKS + " not specified in the MapReduce JobConf (should NEVER happen)");
        }

        this.saveKeys = conf.getBoolean(VoldemortBuildAndPushJob.SAVE_KEYS, true);
        this.reducerPerBucket = conf.getBoolean(VoldemortBuildAndPushJob.REDUCER_PER_BUCKET, true);
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

    /**
     * This function computes which reduce task to shuffle a record to.
     */
    public int getPartition(byte[] key,
                            byte[] value,
                            int numReduceTasks) {
        try {
            /**
             * {@link partitionId} is the Voldemort primary partition that this
             * record belongs to.
             */
            int partitionId = ByteUtils.readInt(value, ByteUtils.SIZE_OF_INT);

            /**
             * This is the base number we will ultimately mod by {@link numReduceTasks}
             * to determine which reduce task to shuffle to.
             */
            int magicNumber = partitionId;

            if (getSaveKeys() && !buildPrimaryReplicasOnly) {
                /**
                 * When saveKeys is enabled (which also implies we are generating
                 * READ_ONLY_V2 format files), then we are generating files with
                 * a replica type, with one file per replica.
                 *
                 * Each replica is sent to a different reducer, and thus the
                 * {@link magicNumber} is scaled accordingly.
                 *
                 * The downside to this is that it is pretty wasteful. The files
                 * generated for each replicas are identical to one another, so
                 * there's no point in generating them independently in many
                 * reducers.
                 *
                 * This is one of the reasons why buildPrimaryReplicasOnly was
                 * written. In this mode, we only generate the files for the
                 * primary replica, which means the number of reducers is
                 * minimized and {@link magicNumber} does not need to be scaled.
                 */
                int replicaType = (int) ByteUtils.readBytes(value,
                                                            2 * ByteUtils.SIZE_OF_INT,
                                                            ByteUtils.SIZE_OF_BYTE);
                magicNumber = magicNumber * getStoreDef().getReplicationFactor() + replicaType;
            }

            if (!getReducerPerBucket()) {
                /**
                 * Partition files can be split in many chunks in order to limit the
                 * maximum file size downloaded and handled by Voldemort servers.
                 *
                 * {@link chunkId} represents which chunk of partition then current
                 * record belongs to.
                 */
                int chunkId = ReadOnlyUtils.chunk(key, getNumChunks());

                /**
                 * When reducerPerBucket is disabled, all chunks are sent to a
                 * different reducer. This increases parallelism at the expense
                 * of adding more load on Hadoop.
                 *
                 * {@link magicNumber} is thus scaled accordingly, in order to
                 * leverage the extra reducers available to us.
                 */
                magicNumber = magicNumber * getNumChunks() + chunkId;
            }

            /**
             * Finally, we mod {@link magicNumber} by {@link numReduceTasks},
             * since the MapReduce framework expects the return of this function
             * to be bounded by the number of reduce tasks running in the job.
             */
            return magicNumber % numReduceTasks;
        } catch (Exception e) {
            throw new VoldemortException("Caught exception in getPartition()!" +
                                         " key: " + ByteUtils.toHexString(key) +
                                         ", value: " + ByteUtils.toHexString(value) +
                                         ", numReduceTasks: " + numReduceTasks, e);
        }
    }
}

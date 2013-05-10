/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// TODO: Merge this with what is currently REbalanceNodePlan? No!? This class
// serves to purposes: 1) In RebalanceClusterPlan, these things are created and
// the BatchPlan is expressed as a list of these; 2) In RebalanceNodePlan, the
// work each node does during a batch is expressed as an ordered list of these.
// Are all interfaces /members needed for both use cases? simplification &
// clarification would be really nice.

// TODO: Rename to include PartitionStore in name since the basic unit of work
// tracked within this class is at the level of partition-stores, not
// partitions. Maybe "RebalancePartitionStoreTask" is a good name? Though, this
// class covers all partition-stores that must move between this particular
// donor/stealer.
/**
 * Holds the list of partitions being moved / deleted for a stealer-donor node
 * tuple
 * 
 */
public class RebalancePartitionsInfo {

    private final int stealerId;
    private final int donorId;
    // TODO: (refactor) Unclear what value of the inner Hashmap is. It maps
    // "replica type" to lists of partition IDs. A list of partition IDs (per
    // store) seems sufficient for all purposes. The replica type is a
    // distraction.
    private HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitionList;
    // TODO: (refactor) What value is maxReplica? Seems like it is used in loops
    // internally. No idea why it is a member.
    private int maxReplica;
    // TODO: (refactor) Does the initialCluster have to be a member? See if it
    // can be removed. There is a getInitialCluster method that is called by
    // others. Why do callers need the initial cluster from this particular
    // class? At first glance, all usages of this method are awkward/unclean
    // (i.e., seems like initialCluster could be found through other paths in
    // all cases).
    private Cluster initialCluster;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing for a stealer-donor node tuple
     * 
     * <br>
     * 
     * @param stealerNodeId Stealer node id
     * @param donorId Donor node id
     * @param storeToReplicaToAddPartitionList Map of store name to map of
     *        replica type to partitions to add
     * @param storeToReplicaToDeletePartitionList Map of store name to map of
     *        replica type to partitions to delete
     * @param initialCluster We require the state of the current metadata in
     *        order to determine correct key movement for RW stores. Otherwise
     *        we move keys on the basis of the updated metadata and hell breaks
     *        loose.
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitionList,
                                   Cluster initialCluster) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.storeToReplicaToAddPartitionList = storeToReplicaToAddPartitionList;
        this.maxReplica = 0;

        // Find the max replica number
        findMaxReplicaType(storeToReplicaToAddPartitionList);

        this.initialCluster = Utils.notNull(initialCluster);
    }

    private void findMaxReplicaType(HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToPartitionList) {
        for(Entry<String, HashMap<Integer, List<Integer>>> entry: storeToReplicaToPartitionList.entrySet()) {
            for(Entry<Integer, List<Integer>> replicaToPartitionList: entry.getValue().entrySet()) {
                if(replicaToPartitionList.getKey() > this.maxReplica) {
                    this.maxReplica = replicaToPartitionList.getKey();
                }
            }
        }
    }

    public static RebalancePartitionsInfo create(String line) {
        try {
            JsonReader reader = new JsonReader(new StringReader(line));
            Map<String, ?> map = reader.readObject();
            return create(map);
        } catch(Exception e) {
            throw new VoldemortException("Failed to create partition info from string: " + line, e);
        }
    }

    public static RebalancePartitionsInfo create(Map<?, ?> map) {
        int stealerId = (Integer) map.get("stealerId");
        int donorId = (Integer) map.get("donorId");
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStores"));
        int maxReplicas = (Integer) map.get("maxReplicas");
        Cluster initialCluster = new ClusterMapper().readCluster(new StringReader((String) map.get("initialCluster")));

        HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartition = Maps.newHashMap();
        HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToDeletePartition = Maps.newHashMap();
        for(String unbalancedStore: unbalancedStoreList) {

            HashMap<Integer, List<Integer>> replicaToAddPartition = Maps.newHashMap();
            HashMap<Integer, List<Integer>> replicaToDeletePartitionList = Maps.newHashMap();
            for(int replicaNo = 0; replicaNo <= maxReplicas; replicaNo++) {
                List<Integer> partitionList = Utils.uncheckedCast(map.get(unbalancedStore
                                                                          + "replicaToAddPartitionList"
                                                                          + Integer.toString(replicaNo)));
                // TODO there is a potential NPE hiding here that might fail
                // rebalancing tests
                if(partitionList.size() > 0)
                    replicaToAddPartition.put(replicaNo, partitionList);

                List<Integer> deletePartitionList = Utils.uncheckedCast(map.get(unbalancedStore
                                                                                + "replicaToDeletePartitionList"
                                                                                + Integer.toString(replicaNo)));
                // TODO there is a potential NPE hiding here that might fail
                // rebalancing tests
                if(deletePartitionList.size() > 0)
                    replicaToDeletePartitionList.put(replicaNo, deletePartitionList);
            }

            if(replicaToAddPartition.size() > 0)
                storeToReplicaToAddPartition.put(unbalancedStore, replicaToAddPartition);

            if(replicaToDeletePartitionList.size() > 0)
                storeToReplicaToDeletePartition.put(unbalancedStore, replicaToDeletePartitionList);
        }

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           storeToReplicaToAddPartition,
                                           initialCluster);
    }

    public synchronized ImmutableMap<String, Object> asMap() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();

        builder.put("stealerId", stealerId)
               .put("donorId", donorId)
               .put("unbalancedStores",
                    Lists.newArrayList(storeToReplicaToAddPartitionList.keySet()))
               .put("maxReplicas", maxReplica)
               .put("initialCluster", new ClusterMapper().writeCluster(initialCluster));

        for(String unbalancedStore: storeToReplicaToAddPartitionList.keySet()) {

            HashMap<Integer, List<Integer>> replicaToAddPartition = storeToReplicaToAddPartitionList.get(unbalancedStore);

            for(int replicaNum = 0; replicaNum <= maxReplica; replicaNum++) {

                if(replicaToAddPartition != null && replicaToAddPartition.containsKey(replicaNum)) {
                    builder.put(unbalancedStore + "replicaToAddPartitionList"
                                        + Integer.toString(replicaNum),
                                replicaToAddPartition.get(replicaNum));
                } else {
                    builder.put(unbalancedStore + "replicaToAddPartitionList"
                                        + Integer.toString(replicaNum),
                                Lists.newArrayList());
                }
            }
        }
        return builder.build();
    }

    public synchronized int getDonorId() {
        return donorId;
    }

    public synchronized int getStealerId() {
        return stealerId;
    }

    public synchronized Cluster getInitialCluster() {
        return initialCluster;
    }

    /**
     * Total count of partition-stores moved or deleted in this task.
     * 
     * @return number of partition stores moved in this task.
     */
    public synchronized int getPartitionStoreMoves() {
        int count = 0;

        for(HashMap<Integer, List<Integer>> storeMoves: storeToReplicaToAddPartitionList.values()) {
            for(List<Integer> partitionStoreMoves: storeMoves.values()) {
                count += partitionStoreMoves.size();
            }
        }

        return count;
    }

    /**
     * Returns the stores which have their partitions being added ( The stores
     * with partitions being deleted are a sub-set )
     * 
     * @return Set of store names
     */
    public synchronized Set<String> getUnbalancedStoreList() {
        return storeToReplicaToAddPartitionList.keySet();
    }

    public synchronized HashMap<String, HashMap<Integer, List<Integer>>> getStoreToReplicaToAddPartitionList() {
        return this.storeToReplicaToAddPartitionList;
    }

    public synchronized HashMap<Integer, List<Integer>> getReplicaToAddPartitionList(String storeName) {
        return this.storeToReplicaToAddPartitionList.get(storeName);
    }

    public synchronized void setStoreToReplicaToAddPartitionList(HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitionList) {
        this.storeToReplicaToAddPartitionList = storeToReplicaToAddPartitionList;
    }

    public synchronized void removeStore(String storeName) {
        this.storeToReplicaToAddPartitionList.remove(storeName);
    }

    /**
     * Gives the list of primary partitions being moved across all stores.
     * 
     * @return List of primary partitions
     */
    public synchronized List<Integer> getStealMasterPartitions() {
        Iterator<HashMap<Integer, List<Integer>>> iter = storeToReplicaToAddPartitionList.values()
                                                                                         .iterator();
        List<Integer> primaryPartitionsBeingMoved = Lists.newArrayList();
        while(iter.hasNext()) {
            HashMap<Integer, List<Integer>> partitionTuples = iter.next();
            if(partitionTuples.containsKey(0))
                primaryPartitionsBeingMoved.addAll(partitionTuples.get(0));
        }
        return primaryPartitionsBeingMoved;
    }

    @Override
    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("\nRebalancePartitionsInfo(" + getStealerId() + " ["
                  + initialCluster.getNodeById(getStealerId()).getHost() + "] <--- " + getDonorId()
                  + " [" + initialCluster.getNodeById(getDonorId()).getHost() + "] ");

        for(String unbalancedStore: storeToReplicaToAddPartitionList.keySet()) {

            sb.append("\n\t- Store '" + unbalancedStore + "' move ");
            HashMap<Integer, List<Integer>> replicaToAddPartition = storeToReplicaToAddPartitionList.get(unbalancedStore);

            for(int replicaNum = 0; replicaNum <= maxReplica; replicaNum++) {
                if(replicaToAddPartition != null && replicaToAddPartition.containsKey(replicaNum))
                    sb.append(" - " + replicaToAddPartition.get(replicaNum));
                else
                    sb.append(" - []");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public synchronized String toJsonString() {
        Map<String, Object> map = asMap();

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(map);
        writer.flush();
        return writer.toString();
    }

    @Override
    public synchronized boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        RebalancePartitionsInfo that = (RebalancePartitionsInfo) o;

        if(donorId != that.donorId)
            return false;
        if(stealerId != that.stealerId)
            return false;
        if(!initialCluster.equals(that.initialCluster))
            return false;
        if(storeToReplicaToAddPartitionList != null ? !storeToReplicaToAddPartitionList.equals(that.storeToReplicaToAddPartitionList)
                                                   : that.storeToReplicaToAddPartitionList != null)
            return false;

        return true;
    }

    @Override
    public synchronized int hashCode() {
        int result = stealerId;
        result = 31 * result + donorId;
        result = 31 * result + initialCluster.hashCode();
        result = 31
                 * result
                 + (storeToReplicaToAddPartitionList != null ? storeToReplicaToAddPartitionList.hashCode()
                                                            : 0);
        return result;
    }
}
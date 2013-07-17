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
import java.util.List;
import java.util.Map;
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

public class RebalanceTaskInfo {

    private final int stealerId;

    private final int donorId;
    private HashMap<String, List<Integer>> storeToPartitionIds;
    private Cluster initialCluster;
    
    /**
     * Rebalance Partitions info maintains all information needed for rebalancing for a
     * stealer-donor node tuple
     * 
     * @param stealerNodeId Stealer node id
     * @param donorId Donor node id
     * @param storeToPartitionList Map of store name to partitions
     * @param initialCluster We require the state of the current metadata in order to determine
     *            correct key movement for RW stores. Otherwise we move keys on the basis of the
     *            updated metadata and hell breaks loose.
     */
    public RebalanceTaskInfo(int stealerNodeId, int donorId, HashMap<String,
                                      List<Integer>> storeToPartitionIds, Cluster initialCluster) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.storeToPartitionIds = storeToPartitionIds;
        this.initialCluster = initialCluster;
    }

    public static RebalanceTaskInfo create(String line) {
        try {
            JsonReader reader = new JsonReader(new StringReader(line));
            Map<String, ?> map = reader.readObject();
            return create(map);
        } catch (Exception e) {
            throw new VoldemortException("Failed to create partition info from string: " + line, e);
        }
    }

    // TODO : Need to think through which of these "SerDe" methods are needed for on-the-wire and 
    // which are needed for on-disk. Neither seems like standard "SerDe". And then come up with a 
    // standard way to perform Serialization/De-serialization

    public static RebalanceTaskInfo create(Map<?, ?> map) {
        int stealerId = (Integer)map.get("stealerId");
        int donorId = (Integer)map.get("donorId");
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStores"));
        Cluster initialCluster = new ClusterMapper()
                                     .readCluster(new StringReader((String)map.get("initialCluster")));

        HashMap<String, List<Integer>> storeToPartitionIds = Maps.newHashMap();
        for (String unbalancedStore : unbalancedStoreList) {
            List<Integer> partitionList = Utils.uncheckedCast(map.get(unbalancedStore + "partitionList"));
            if (partitionList.size() > 0)
                storeToPartitionIds.put(unbalancedStore, partitionList);
        }

        return new RebalanceTaskInfo(stealerId, donorId, storeToPartitionIds, initialCluster);
    }

    public synchronized ImmutableMap<String, Object> asMap() {

        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();
        builder.put("stealerId", stealerId)
               .put("donorId", donorId)
               .put("unbalancedStores", Lists.newArrayList(storeToPartitionIds.keySet()))
               .put("initialCluster", new ClusterMapper().writeCluster(initialCluster));

        for (String unbalancedStore : storeToPartitionIds.keySet()) {
            List<Integer> partitionIds = storeToPartitionIds.get(unbalancedStore);
            if (!partitionIds.isEmpty()) 
                builder.put(unbalancedStore + "partitionList", partitionIds);
            else
                builder.put(unbalancedStore + "partitionList" , Lists.newArrayList());
            }

        return builder.build();
    }

    /**
     * @return store to partition ids map
     */
    public synchronized HashMap<String, List<Integer>> getStoreToPartitionIds() {
        return this.storeToPartitionIds;
    }

    /**
     * @return set partition ids for a specific store
     */
    public synchronized void setPartitionIds(String storeName, List<Integer> partitionIds) {
        this.storeToPartitionIds.put(storeName, partitionIds);
    }

    /**
     * @return set store to partitions maps
     */
    public synchronized void setStoreToPartitionList(HashMap<String, List<Integer>> storeToPartitionIds) {
        this.storeToPartitionIds = storeToPartitionIds;
    }

    /**
     * @return initial cluster
     */
    public synchronized Cluster getInitialCluster() {
        return initialCluster;
    }

    /**
     * @return returns donor node id
     */
    public synchronized int getDonorId() {
        return donorId;
    }

    /**
     * @return returns stealer node id
     */
    public synchronized int getStealerId() {
        return stealerId;
    }

    /**
     * Total count of partition-stores moved in this task.
     * 
     * @return number of partition stores moved in this task.
     */
    public synchronized int getPartitionStoreMoves() {
        int count = 0;
        for (List<Integer> entry : storeToPartitionIds.values())
            count += entry.size();
        return count;
    }

    /**
     * Removes the store name from the map.
     * 
     * @param store name to be removed from the map
     */
    public synchronized void removeStore(String storeName) {
        this.storeToPartitionIds.remove(storeName);
    }

    /**
     * Returns the list of partitions ids corresponding to a store.
     * 
     * @param name of the store
     * @return list of partitions ids for the store.
     */
    public synchronized List<Integer> getPartitionIds(String storeName) {
        return this.storeToPartitionIds.get(storeName);
    }

    /**
     * Returns all the store names from the map
     * 
     * @return returns a list of stores in the map
     */
    public synchronized Set<String> getPartitionStores() {
        return this.storeToPartitionIds.keySet();
    }

    /**
     * Returns the total count of partitions across all stores.
     * 
     * @return returns the total count of partitions across all stores.
     */
    public synchronized int getPartitionStoreCount() {
        int count = 0;
        for (String store : storeToPartitionIds.keySet()) {
            count += storeToPartitionIds.get(store).size();
        }
        return count;
    }

    @Override
    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("\nRebalanceTaskInfo(" + getStealerId()
                  + " [" + initialCluster.getNodeById(getStealerId()).getHost() 
                  + "] <--- " + getDonorId() 
                  + " ["
                  + initialCluster.getNodeById(getDonorId()).getHost() 
                  + "] ");
        for (String unbalancedStore : storeToPartitionIds.keySet()) {
            sb.append("\n\t- Store '" + unbalancedStore + "' move ");
            List<Integer> partitionIds = storeToPartitionIds.get(unbalancedStore);
            if (!partitionIds.isEmpty())
                sb.append(" - " + partitionIds);
            else
                sb.append(" - []");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Pretty prints a task list of rebalancing tasks.
     * 
     * @param infos list of rebalancing tasks (RebalancePartitiosnInfo)
     * @return pretty-printed string
     */
    public static String taskListToString(List<RebalanceTaskInfo> infos) {
        StringBuffer sb = new StringBuffer();
        for (RebalanceTaskInfo info : infos) {
            sb.append("\t").append(info.getDonorId()).append(" -> ").append(info.getStealerId()).append(" : [");
            for (String storeName : info.getPartitionStores()) {
                sb.append("{").append(storeName).append(" : ").append(info.getPartitionIds(storeName)).append("}");
            }
            sb.append("]").append(Utils.NEWLINE);
        }
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RebalanceTaskInfo that = (RebalanceTaskInfo)o;
        if (donorId != that.donorId)
            return false;
        if (stealerId != that.stealerId)
            return false;
        if (!initialCluster.equals(that.initialCluster))
            return false;
        if (storeToPartitionIds != null ? !storeToPartitionIds.equals(that.storeToPartitionIds) 
                                        : that.storeToPartitionIds != null)
            return false;

        return true;
    }

    @Override
    public synchronized int hashCode() {
        int result = stealerId;
        result = 31 * result + donorId;
        result = 31 * result + initialCluster.hashCode();
        result = 31 * result + (storeToPartitionIds != null ? storeToPartitionIds.hashCode() : 0);
        return result;
    }
}

package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Holds the list of partitions being moved / deleted for a stealer-donor node
 * tuple
 * 
 */
public class RebalancePartitionsInfo {

    private final int stealerId;
    private final int donorId;
    private List<String> unbalancedStoreList;
    private int attempt;
    private HashMap<Integer, List<Integer>> replicaToPartitionList;
    private List<Integer> allPartitions;
    private List<Integer> deletePartitionsList;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing for a stealer-donor node tuple
     * 
     * <br>
     * 
     * @param stealerNodeId Stealer node id
     * @param donorId Donor node id
     * @param replicaToPartitionList Map of replica type to partition list
     * @param deletePartitionsList List of partitions being deleted
     * @param unbalancedStoreList List of stores which are unbalanced
     * @param attempt Attempt number
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   HashMap<Integer, List<Integer>> replicaToPartitionList,
                                   List<Integer> deletePartitionsList,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.replicaToPartitionList = replicaToPartitionList;
        this.deletePartitionsList = deletePartitionsList;
        this.attempt = attempt;
        this.unbalancedStoreList = unbalancedStoreList;
    }

    public static RebalancePartitionsInfo create(String line) {
        try {
            JsonReader reader = new JsonReader(new StringReader(line));
            Map<String, ?> map = reader.readObject();
            return create(map);
        } catch(Exception e) {
            throw new VoldemortException("Failed to create RebalanceStealInfo from String:" + line,
                                         e);
        }
    }

    public static RebalancePartitionsInfo create(Map<?, ?> map) {
        int stealerId = (Integer) map.get("stealerId");
        int donorId = (Integer) map.get("donorId");
        List<Integer> deletePartitionsList = Utils.uncheckedCast(map.get("deletePartitionsList"));
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStoreList"));
        int attempt = (Integer) map.get("attempt");
        int numReplicas = (Integer) map.get("numReplicas");

        HashMap<Integer, List<Integer>> partitionMap = Maps.newHashMap();
        for(int replicaNo = 0; replicaNo < numReplicas; replicaNo++) {
            List<Integer> partitionList = Utils.uncheckedCast(map.get("replicaToPartitionList"
                                                                      + Integer.toString(replicaNo)));
            partitionMap.put(replicaNo, partitionList);
        }

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           partitionMap,
                                           deletePartitionsList,
                                           unbalancedStoreList,
                                           attempt);
    }

    public ImmutableMap<String, Object> asMap() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();

        builder.put("stealerId", stealerId)
               .put("donorId", donorId)
               .put("deletePartitionsList", deletePartitionsList)
               .put("unbalancedStoreList", unbalancedStoreList)
               .put("attempt", attempt)
               .put("numReplicas", replicaToPartitionList.size());

        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {
            builder.put("replicaToPartitionList" + Integer.toString(entry.getKey()),
                        entry.getValue());
        }

        return builder.build();
    }

    public List<Integer> getDeletePartitionsList() {
        return deletePartitionsList;
    }

    public void setDeletePartitionsList(List<Integer> deletedPartitions) {
        deletePartitionsList = deletedPartitions;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getDonorId() {
        return donorId;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getStealerId() {
        return stealerId;
    }

    public List<String> getUnbalancedStoreList() {
        return unbalancedStoreList;
    }

    public void setUnbalancedStoreList(List<String> storeList) {
        this.unbalancedStoreList = storeList;
    }

    public List<Integer> getStealReplicaPartitions() {
        return stealReplicaPartitions;
    }

    public void setStealReplicaPartitions(List<Integer> stealReplicaPartitions) {
        this.stealReplicaPartitions = stealReplicaPartitions;
    }

    public List<Integer> getStealMasterPartitions() {
        return stealMasterPartitions;
    }

    public void setStealMasterPartitions(List<Integer> stealMasterPartitions) {
        this.stealMasterPartitions = stealMasterPartitions;
    }

    /**
     * Returns list of all partitions being moved ( primary + replicas )
     */
    public List<Integer> getPartitions() {
        return allPartitions;
    }

    @Override
    public String toString() {
        return "RebalancingStealInfo(" + getStealerId() + " <--- " + getDonorId()
               + " master partitions :" + getStealMasterPartitions() + " replica partitions :"
               + getStealReplicaPartitions() + " deleted partition :" + getDeletePartitionsList()
               + " stores:" + getUnbalancedStoreList() + ")";
    }

    public String toJsonString() {
        Map<String, Object> map = asMap();

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(map);
        writer.flush();
        return writer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        RebalancePartitionsInfo that = (RebalancePartitionsInfo) o;

        if(attempt != that.attempt)
            return false;
        if(donorId != that.donorId)
            return false;
        if(stealerId != that.stealerId)
            return false;
        if(stealMasterPartitions != null ? !stealMasterPartitions.equals(that.stealMasterPartitions)
                                        : that.stealMasterPartitions != null)
            return false;
        if(stealReplicaPartitions != null ? !stealReplicaPartitions.equals(that.stealReplicaPartitions)
                                         : that.stealReplicaPartitions != null)
            return false;
        if(deletePartitionsList != null ? !deletePartitionsList.equals(that.deletePartitionsList)
                                       : that.deletePartitionsList != null)
            return false;
        if(unbalancedStoreList != null ? !unbalancedStoreList.equals(that.unbalancedStoreList)
                                      : that.unbalancedStoreList != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = stealerId;
        result = 31 * result + donorId;
        result = 31 * result
                 + (stealMasterPartitions != null ? stealMasterPartitions.hashCode() : 0);
        result = 31 * result
                 + (stealReplicaPartitions != null ? stealReplicaPartitions.hashCode() : 0);
        result = 31 * result + (deletePartitionsList != null ? deletePartitionsList.hashCode() : 0);
        result = 31 * result + (unbalancedStoreList != null ? unbalancedStoreList.hashCode() : 0);
        result = 31 * result + attempt;
        return result;
    }
}
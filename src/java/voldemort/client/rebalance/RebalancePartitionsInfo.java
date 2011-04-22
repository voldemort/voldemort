package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableMap;

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
    private List<Integer> stealMasterPartitions;
    private List<Integer> stealReplicaPartitions;
    private List<Integer> deletePartitionsList;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing for a stealer-donor node tuple
     * 
     * <br>
     * 
     * @param stealerNodeId Stealer node id
     * @param donorId Donor node id
     * @param stealMasterPartitions List of master partitions being moved
     * @param stealReplicaPartitions List of replica partitions being moved
     * @param deletePartitionsList List of partitions being deleted
     * @param unbalancedStoreList List of stores which are unbalanced
     * @param attempt Attempt number
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> stealMasterPartitions,
                                   List<Integer> stealReplicaPartitions,
                                   List<Integer> deletePartitionsList,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.stealMasterPartitions = stealMasterPartitions;
        this.stealReplicaPartitions = stealReplicaPartitions;
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
        List<Integer> stealMasterPartitions = Utils.uncheckedCast(map.get("stealMasterPartitions"));
        List<Integer> stealReplicaPartitions = Utils.uncheckedCast(map.get("stealReplicaPartitions"));
        List<Integer> deletePartitionsList = Utils.uncheckedCast(map.get("deletePartitionsList"));
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStoreList"));
        int attempt = (Integer) map.get("attempt");

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           stealMasterPartitions,
                                           stealReplicaPartitions,
                                           deletePartitionsList,
                                           unbalancedStoreList,
                                           attempt);
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

    public ImmutableMap<String, Object> asMap() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();

        return builder.put("stealerId", stealerId)
                      .put("donorId", donorId)
                      .put("stealMasterPartitions", stealMasterPartitions)
                      .put("stealReplicaPartitions", stealReplicaPartitions)
                      .put("deletePartitionsList", deletePartitionsList)
                      .put("unbalancedStoreList", unbalancedStoreList)
                      .put("attempt", attempt)
                      .build();
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
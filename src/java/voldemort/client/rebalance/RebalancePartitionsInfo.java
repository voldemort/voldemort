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

public class RebalancePartitionsInfo {

    private final int stealerId;
    private final int donorId;
    private final List<Integer> partitionList;
    private final List<Integer> deletePartitionsList;
    private List<String> unbalancedStoreList;
    private int attempt;
    private List<Integer> stealMasterPartitions;
    private Map<String, String> storeToRODir;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing of one stealer node from one donor node.
     * <p>
     * 
     * @param stealerNodeId
     * @param donorId
     * @param partitionList
     * @param deletePartitionsList : selected list of partitions which only
     *        should be deleted
     * @param stealMasterPartitions : partitions for which we should change the
     *        ownership in cluster.
     * @param unbalancedStoreList : list of store names which need rebalancing
     * @param attempt : attempt number
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<Integer> deletePartitionsList,
                                   List<Integer> stealMasterPartitions,
                                   List<String> unbalancedStoreList,
                                   Map<String, String> storeToRODir,
                                   int attempt) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.deletePartitionsList = deletePartitionsList;
        this.unbalancedStoreList = unbalancedStoreList;
        this.stealMasterPartitions = stealMasterPartitions;
        this.storeToRODir = storeToRODir;
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
        List<Integer> partitionList = Utils.uncheckedCast(map.get("partitionList"));
        List<Integer> stealMasterPartitions = Utils.uncheckedCast(map.get("stealMasterPartitions"));
        int attempt = (Integer) map.get("attempt");
        List<Integer> deletePartitionsList = Utils.uncheckedCast(map.get("deletePartitionsList"));
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStoreList"));
        Map<String, String> storeToRODir = Utils.uncheckedCast(map.get("storeToRODir"));

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           partitionList,
                                           deletePartitionsList,
                                           stealMasterPartitions,
                                           unbalancedStoreList,
                                           storeToRODir,
                                           attempt);
    }

    public List<Integer> getDeletePartitionsList() {
        return deletePartitionsList;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getDonorId() {
        return donorId;
    }

    public List<Integer> getPartitionList() {
        return partitionList;
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

    public List<Integer> getStealMasterPartitions() {
        return stealMasterPartitions;
    }

    public void setStealMasterPartitions(List<Integer> stealMasterPartitions) {
        this.stealMasterPartitions = stealMasterPartitions;
    }

    public Map<String, String> getStoreToRODir() {
        return storeToRODir;
    }

    public void setStoreToRODir(Map<String, String> storeToRODir) {
        this.storeToRODir = storeToRODir;
    }

    @Override
    public String toString() {
        return "RebalancingStealInfo(" + getStealerId() + " <--- " + getDonorId() + " partitions:"
               + getPartitionList() + " steal master partitions:" + getStealMasterPartitions()
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
                      .put("partitionList", partitionList)
                      .put("unbalancedStoreList", unbalancedStoreList)
                      .put("stealMasterPartitions", stealMasterPartitions)
                      .put("deletePartitionsList", deletePartitionsList)
                      .put("storeToRODir", storeToRODir)
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
        if(!deletePartitionsList.equals(that.deletePartitionsList))
            return false;
        if(!partitionList.equals(that.partitionList))
            return false;
        if(stealMasterPartitions != null ? !stealMasterPartitions.equals(that.stealMasterPartitions)
                                        : that.stealMasterPartitions != null)
            return false;
        if(unbalancedStoreList != null ? !unbalancedStoreList.equals(that.unbalancedStoreList)
                                      : that.unbalancedStoreList != null)
            return false;
        if(storeToRODir != null ? !storeToRODir.equals(that.storeToRODir)
                               : that.storeToRODir != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = stealerId;
        result = 31 * result + donorId;
        result = 31 * result + partitionList.hashCode();
        result = 31 * result + deletePartitionsList.hashCode();
        result = 31 * result + (unbalancedStoreList != null ? unbalancedStoreList.hashCode() : 0);
        result = 31 * result + attempt;
        result = 31 * result
                 + (stealMasterPartitions != null ? stealMasterPartitions.hashCode() : 0);
        result = 31 * result + (storeToRODir != null ? storeToRODir.hashCode() : 0);
        return result;
    }
}
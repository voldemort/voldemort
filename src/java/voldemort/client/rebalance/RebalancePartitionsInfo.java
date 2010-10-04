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
    private Map<String, String> stealerNodeROStoreToDir, donorNodeROStoreToDir;

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
     * @param stealerNodeStoreToRODir : map of store name to read-only store
     *        directory on stealer node
     * @param donorNodeStoreToRODir : mapping of store name to read-only store
     *        directory on donor node
     * @param attempt : attempt number
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<Integer> deletePartitionsList,
                                   List<Integer> stealMasterPartitions,
                                   List<String> unbalancedStoreList,
                                   Map<String, String> stealerNodeROStoreToDir,
                                   Map<String, String> donorNodeROStoreToDir,
                                   int attempt) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.deletePartitionsList = deletePartitionsList;
        this.unbalancedStoreList = unbalancedStoreList;
        this.stealMasterPartitions = stealMasterPartitions;
        this.stealerNodeROStoreToDir = stealerNodeROStoreToDir;
        this.donorNodeROStoreToDir = donorNodeROStoreToDir;
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
        Map<String, String> stealerNodeROStoreToDir = Utils.uncheckedCast(map.get("stealerNodeROStoreToDir"));
        Map<String, String> donorNodeROStoreToDir = Utils.uncheckedCast(map.get("donorNodeROStoreToDir"));

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           partitionList,
                                           deletePartitionsList,
                                           stealMasterPartitions,
                                           unbalancedStoreList,
                                           stealerNodeROStoreToDir,
                                           donorNodeROStoreToDir,
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

    public Map<String, String> getStealerNodeROStoreToDir() {
        return stealerNodeROStoreToDir;
    }

    public void setStealerNodeROStoreToDir(Map<String, String> stealerNodeROStoreToDir) {
        this.stealerNodeROStoreToDir = stealerNodeROStoreToDir;
    }

    public Map<String, String> getDonorNodeROStoreToDir() {
        return donorNodeROStoreToDir;
    }

    public void setDonorNodeROStoreToDir(Map<String, String> donorNodeROStoreToDir) {
        this.donorNodeROStoreToDir = donorNodeROStoreToDir;
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
                      .put("stealerNodeROStoreToDir", stealerNodeROStoreToDir)
                      .put("donorNodeROStoreToDir", donorNodeROStoreToDir)
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
        if(stealerNodeROStoreToDir != null ? !stealerNodeROStoreToDir.equals(that.stealerNodeROStoreToDir)
                                          : that.stealerNodeROStoreToDir != null)
            return false;
        if(donorNodeROStoreToDir != null ? !donorNodeROStoreToDir.equals(that.donorNodeROStoreToDir)
                                        : that.donorNodeROStoreToDir != null)
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
        result = 31 * result
                 + (stealerNodeROStoreToDir != null ? stealerNodeROStoreToDir.hashCode() : 0);
        result = 31 * result
                 + (donorNodeROStoreToDir != null ? donorNodeROStoreToDir.hashCode() : 0);
        return result;
    }
}
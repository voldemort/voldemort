package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;

import com.google.common.collect.ImmutableMap;

public class RebalancePartitionsInfo {

    private final int stealerId;
    private final int donorId;
    private final List<Integer> partitionList;
    private final List<Integer> deletePartitionsList;
    private List<String> unbalancedStoreList;
    private int attempt;

    /**
     * TODO(MED) : We need to add stealMasterPartitions for issue#210. The core
     * issue is we needed a way to differentiate between master partitions copy
     * and partitions copy needed to satisfy replication constraints. (master
     * partition ownership need to be changed and can be deleted from original
     * node). <br>
     * Currently we are using partitionList, deletePartitionsList and
     * stealMasterPartitions to do it, We can do it in much better ways. This is
     * currently being done to avoid changing wire protocol (admin) and
     * this#toJsonString() code as this would make the new version incompatible
     * with the last one, changing this#toJsonString() means you might need to
     * clear .temp directories to start the server.
     */
    private List<Integer> stealMasterPartitions;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing of one stealer node from one donor node.
     * <p>
     * 
     * @param stealerNodeId
     * @param donorId
     * @param partitionList
     * @param deletePartitionsList : For cases where replication mapping is
     *        changing due to partition migration we only want to copy data and
     *        not delete them from donor node.
     * @param unbalancedStoreList
     * @param attempt
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<Integer> deletePartitionsList,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        super();
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.deletePartitionsList = deletePartitionsList;
        this.unbalancedStoreList = unbalancedStoreList;
    }

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
     * @param unbalancedStoreList
     * @param attempt
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<Integer> deletePartitionsList,
                                   List<Integer> stealMasterPartitions,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        super();
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.deletePartitionsList = deletePartitionsList;
        this.unbalancedStoreList = unbalancedStoreList;
        this.stealMasterPartitions = stealMasterPartitions;
    }

    @SuppressWarnings("unchecked")
    public RebalancePartitionsInfo(String line) {
        try {
            JsonReader reader = new JsonReader(new StringReader(line));
            Map<String, Object> map = (Map<String, Object>) reader.read();
            this.stealerId = (Integer) map.get("stealerId");
            this.donorId = (Integer) map.get("donorId");
            this.partitionList = (List<Integer>) map.get("partitionList");
            this.attempt = (Integer) map.get("attempt");
            this.deletePartitionsList = (List<Integer>) map.get("deletePartitionsList");
            this.unbalancedStoreList = (List<String>) map.get("unbalancedStoreList");
        } catch(Exception e) {
            throw new VoldemortException("Failed to create RebalanceStealInfo from String:" + line,
                                         e);
        }
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

    @Override
    public String toString() {
        return "RebalancingStealInfo(" + getStealerId() + " <--- " + getDonorId() + " partitions:"
               + getPartitionList() + " stores:" + getUnbalancedStoreList() + ")";
    }

    @SuppressWarnings("unchecked")
    public String toJsonString() {
        Map map = ImmutableMap.builder()
                              .put("stealerId", stealerId)
                              .put("donorId", donorId)
                              .put("partitionList", partitionList)
                              .put("unbalancedStoreList", unbalancedStoreList)
                              .put("deletePartitionsList", deletePartitionsList)
                              .put("attempt", attempt)
                              .build();

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(map);
        writer.flush();
        return writer.toString();
    }
}
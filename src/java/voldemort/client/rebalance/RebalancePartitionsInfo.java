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
    private List<String> unbalancedStoreList;

    private int attempt;

    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   List<Integer> partitionList,
                                   List<String> unbalancedStoreList,
                                   int attempt) {
        super();
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.unbalancedStoreList = unbalancedStoreList;
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
            this.unbalancedStoreList = (List<String>) map.get("unbalancedStoreList");
        } catch(Exception e) {
            throw new VoldemortException("Failed to create RebalanceStealInfo from String:" + line,
                                         e);
        }
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
                              .put("attempt", attempt)
                              .build();

        StringWriter writer = new StringWriter();
        new JsonWriter(writer).write(map);
        writer.flush();
        return writer.toString();
    }
}
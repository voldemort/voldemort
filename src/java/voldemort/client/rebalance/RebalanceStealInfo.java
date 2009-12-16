package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;

import com.google.common.collect.ImmutableMap;

public class RebalanceStealInfo {

    private final int stealerId;
    private final int donorId;
    private final List<Integer> partitionList;
    private final List<String> unbalancedStoreList;

    private int attempt;

    public RebalanceStealInfo(int stealerNodeId,
                              int donorId,
                              List<Integer> partitionList,
                              List<String> unbalancedStorList,
                              int attempt) {
        super();
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.unbalancedStoreList = unbalancedStorList;
    }

    public RebalanceStealInfo(String line) {
        JsonReader reader = new JsonReader(new StringReader(line));
        Map<String, Object> map = (Map<String, Object>) reader.read();
        this.stealerId = (Integer) map.get("stealerId");
        this.donorId = (Integer) map.get("donorId");
        this.partitionList = (List<Integer>) map.get("partitionList");
        this.attempt = (Integer) map.get("attempt");
        this.unbalancedStoreList = (List<String>) map.get("unbalancedStoreList");
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
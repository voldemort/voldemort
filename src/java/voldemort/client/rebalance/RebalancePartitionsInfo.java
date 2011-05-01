package voldemort.client.rebalance;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
    private int maxReplica;
    private boolean deletePartitions;
    private Cluster initialCluster;

    /**
     * Rebalance Partitions info maintains all information needed for
     * rebalancing for a stealer-donor node tuple
     * 
     * <br>
     * 
     * @param stealerNodeId Stealer node id
     * @param donorId Donor node id
     * @param replicaToPartitionList Map of replica type to partition list
     * @param unbalancedStoreList List of stores which are unbalanced
     * @param initialCluster We require the state of the current metadata in
     *        order to determine correct key movement for RW stores. Otherwise
     *        we move keys on the basis of the updated metadata and hell breaks
     *        loose.
     * @param deletePartitions Delete partitions after rebalancing
     * @param attempt Attempt number
     */
    public RebalancePartitionsInfo(int stealerNodeId,
                                   int donorId,
                                   HashMap<Integer, List<Integer>> replicaToPartitionList,
                                   List<String> unbalancedStoreList,
                                   Cluster initialCluster,
                                   boolean deletePartitions,
                                   int attempt) {
        this.stealerId = stealerNodeId;
        this.donorId = donorId;
        this.replicaToPartitionList = replicaToPartitionList;
        this.attempt = attempt;
        this.unbalancedStoreList = unbalancedStoreList;
        this.allPartitions = Lists.newArrayList();
        this.maxReplica = 0;
        this.deletePartitions = deletePartitions;
        for(Entry<Integer, List<Integer>> entry: replicaToPartitionList.entrySet()) {
            this.allPartitions.addAll(entry.getValue());
            if(entry.getKey() > maxReplica) {
                maxReplica = entry.getKey();
            }
        }
        this.initialCluster = Utils.notNull(initialCluster);
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
        List<String> unbalancedStoreList = Utils.uncheckedCast(map.get("unbalancedStoreList"));
        int attempt = (Integer) map.get("attempt");
        int maxReplicas = (Integer) map.get("maxReplicas");
        boolean deletePartitions = (Boolean) map.get("deletePartitions");
        Cluster initialCluster = new ClusterMapper().readCluster(new StringReader((String) map.get("initialCluster")));

        HashMap<Integer, List<Integer>> partitionMap = Maps.newHashMap();
        for(int replicaNo = 0; replicaNo <= maxReplicas; replicaNo++) {
            List<Integer> partitionList = Utils.uncheckedCast(map.get("replicaToPartitionList"
                                                                      + Integer.toString(replicaNo)));
            if(partitionList.size() > 0)
                partitionMap.put(replicaNo, partitionList);
        }

        return new RebalancePartitionsInfo(stealerId,
                                           donorId,
                                           partitionMap,
                                           unbalancedStoreList,
                                           initialCluster,
                                           deletePartitions,
                                           attempt);
    }

    public ImmutableMap<String, Object> asMap() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>();

        builder.put("stealerId", stealerId)
               .put("donorId", donorId)
               .put("unbalancedStoreList", unbalancedStoreList)
               .put("attempt", attempt)
               .put("deletePartitions", deletePartitions);

        int maxReplicas = 0;
        for(int replicaNum: replicaToPartitionList.keySet()) {
            if(replicaNum > maxReplicas) {
                maxReplicas = replicaNum;
            }
        }
        builder.put("maxReplicas", maxReplicas);
        builder.put("initialCluster", new ClusterMapper().writeCluster(initialCluster));

        for(int replicaNum = 0; replicaNum <= maxReplicas; replicaNum++) {
            if(replicaToPartitionList.containsKey(replicaNum)) {
                builder.put("replicaToPartitionList" + Integer.toString(replicaNum),
                            replicaToPartitionList.get(replicaNum));
            } else {
                builder.put("replicaToPartitionList" + Integer.toString(replicaNum),
                            Lists.newArrayList());
            }
        }
        return builder.build();
    }

    public boolean getDeletePartitions() {
        return this.deletePartitions;
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

    public Cluster getInitialCluster() {
        return initialCluster;
    }

    public List<String> getUnbalancedStoreList() {
        return unbalancedStoreList;
    }

    public void setUnbalancedStoreList(List<String> storeList) {
        this.unbalancedStoreList = storeList;
    }

    public HashMap<Integer, List<Integer>> getReplicaToPartitionList() {
        return replicaToPartitionList;
    }

    public List<Integer> getStealMasterPartitions() {
        return replicaToPartitionList.get(0);
    }

    /**
     * Returns list of all partitions being moved ( primary + replicas )
     */
    public List<Integer> getPartitions() {
        return allPartitions;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("RebalancingStealInfo(" + getStealerId() + " <--- " + getDonorId()
                  + ", partitions moved ");
        for(int replicaNum = 0; replicaNum <= maxReplica; replicaNum++) {
            if(replicaToPartitionList.containsKey(replicaNum))
                sb.append(" - " + replicaToPartitionList.get(replicaNum));
            else
                sb.append(" - []");
        }
        sb.append(", stores: " + getUnbalancedStoreList() + ", delete - " + deletePartitions + ")");
        return sb.toString();
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
        if(deletePartitions != that.deletePartitions)
            return false;
        if(!initialCluster.equals(that.initialCluster))
            return false;
        if(replicaToPartitionList != null ? !replicaToPartitionList.equals(that.replicaToPartitionList)
                                         : that.replicaToPartitionList != null)
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
        result = 31 * result + initialCluster.hashCode();
        result = 31 * result
                 + (replicaToPartitionList != null ? replicaToPartitionList.hashCode() : 0);
        result = 31 * result + (unbalancedStoreList != null ? unbalancedStoreList.hashCode() : 0);
        result = 31 * result + attempt;
        return result;
    }
}
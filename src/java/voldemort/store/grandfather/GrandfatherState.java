package voldemort.store.grandfather;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Store and manipulate grandfathering state. Also maintains a mapping of
 * partitions being moved to the node ids. Required for fast lookup during
 * grandfathering
 * 
 */
public class GrandfatherState {

    private final HashMultimap<Integer, Integer> partitionToNodeIds = HashMultimap.create();
    // Contains a mapping of every stealer node to their corresponding partition
    // info
    protected final Map<Integer, RebalancePartitionsInfo> stealInfoMap;

    public GrandfatherState(List<RebalancePartitionsInfo> stealInfoList) {
        stealInfoMap = Maps.newHashMapWithExpectedSize(stealInfoList.size());
        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoList)
            stealInfoMap.put(rebalancePartitionsInfo.getStealerId(), rebalancePartitionsInfo);
        for(RebalancePartitionsInfo info: stealInfoList) {
            for(int partition: info.getPartitionList()) {
                partitionToNodeIds.put(partition, info.getStealerId());
            }
        }
    }

    public String toJsonString() {
        List<Map<String, Object>> maps = Lists.newLinkedList();

        for(RebalancePartitionsInfo rebalancePartitionsInfo: stealInfoMap.values())
            maps.add(rebalancePartitionsInfo.asMap());

        StringWriter stringWriter = new StringWriter();
        new JsonWriter(stringWriter).write(maps);
        stringWriter.flush();

        return stringWriter.toString();
    }

    public static GrandfatherState create(String json) {
        List<RebalancePartitionsInfo> stealInfoList = Lists.newLinkedList();
        JsonReader reader = new JsonReader(new StringReader(json));

        for(Object o: reader.readArray()) {
            Map<?, ?> m = (Map<?, ?>) o;
            stealInfoList.add(RebalancePartitionsInfo.create(m));
        }

        return new GrandfatherState(stealInfoList);
    }

    public Set<Integer> findNodeIds(int partition) {
        return partitionToNodeIds.get(partition);
    }

    public Collection<RebalancePartitionsInfo> getAll() {
        return stealInfoMap.values();
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        GrandfatherState that = (GrandfatherState) o;

        if(!stealInfoMap.equals(that.stealInfoMap))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return stealInfoMap.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("GrandfatherState(operations: ");
        sb.append("\n");
        for(RebalancePartitionsInfo info: getAll()) {
            sb.append(info);
            sb.append("\n");
        }
        sb.append(")");

        return sb.toString();
    }
}

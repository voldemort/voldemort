package voldemort.store.grandfather;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.serialization.json.JsonReader;
import voldemort.server.rebalance.RebalancerState;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Same as the RebalancerState but also maintains a mapping of partitions being
 * moved to the node ids. Required for fast lookup during grandfathering
 * 
 */
public class GrandfatherState extends RebalancerState {

    HashMultimap<Integer, Integer> partitionToNodeIds = HashMultimap.create();

    public GrandfatherState(List<RebalancePartitionsInfo> stealInfoList) {
        super(stealInfoList);
        for(RebalancePartitionsInfo info: stealInfoList) {
            for(int partition: info.getPartitionList()) {
                Set<Integer> nodeIds = partitionToNodeIds.get(partition);
                if(nodeIds == null) {
                    nodeIds = Sets.newHashSet();
                    partitionToNodeIds.putAll(partition, nodeIds);
                }
                nodeIds.add(info.getStealerId());
            }
        }
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

}

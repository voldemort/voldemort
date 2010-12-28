package voldemort.store.grandfather;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import voldemort.client.rebalance.RebalancePartitionsInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GrandfatherStateTest {

    private final int NUM_NODES = 5;
    private final int NUM_PARTITIONS_PER_NODE = 4;

    private List<RebalancePartitionsInfo> dummyPartitionsInfo(int numberOfStealerNodes,
                                                              int numberOfPartitionsPerNode,
                                                              boolean includeDuplicate) {

        List<RebalancePartitionsInfo> partitionsInfo = Lists.newArrayList();
        int partitionId = 0;
        int endPartitionId = numberOfStealerNodes * numberOfPartitionsPerNode - 1;

        for(int stealerNodeId = 0; stealerNodeId < numberOfStealerNodes; stealerNodeId++) {
            List<Integer> partitionIds = Lists.newArrayList();
            for(int i = 0; i < numberOfPartitionsPerNode; i++) {
                partitionIds.add(partitionId);
                if(includeDuplicate) {
                    partitionIds.add(endPartitionId);
                    endPartitionId--;
                }
                partitionId++;
            }
            partitionsInfo.add(new RebalancePartitionsInfo(stealerNodeId,
                                                           0,
                                                           partitionIds,
                                                           new ArrayList<Integer>(),
                                                           new ArrayList<Integer>(),
                                                           new ArrayList<String>(),
                                                           new HashMap<String, String>(),
                                                           new HashMap<String, String>(),
                                                           0));
        }
        return partitionsInfo;
    }

    @Test
    public void testPartitionToNodeMapping() {
        List<RebalancePartitionsInfo> partitionsInfo = dummyPartitionsInfo(NUM_NODES,
                                                                           NUM_PARTITIONS_PER_NODE,
                                                                           false);
        GrandfatherState state = new GrandfatherState(partitionsInfo);

        for(int partitionId = 0; partitionId < NUM_NODES * NUM_PARTITIONS_PER_NODE; partitionId++) {
            assertEquals(state.findNodeIds(partitionId).size(), 1);
            assertEquals(state.findNodeIds(partitionId), Sets.newHashSet(partitionId
                                                                         / NUM_PARTITIONS_PER_NODE));
        }

        partitionsInfo = dummyPartitionsInfo(5, 4, true);
        state = new GrandfatherState(partitionsInfo);

        for(int partitionId = 0; partitionId < NUM_NODES * NUM_PARTITIONS_PER_NODE; partitionId++) {
            assertEquals(state.findNodeIds(partitionId),
                         Sets.newHashSet(partitionId / NUM_PARTITIONS_PER_NODE,
                                         NUM_NODES - (partitionId / NUM_PARTITIONS_PER_NODE) - 1));
        }

    }
}

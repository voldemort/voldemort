package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import voldemort.ServerTestUtils;

import com.google.common.collect.Lists;

public class RebalancePartitionsInfoTest {

    @Test
    public void testRebalancePartitionsInfoCreate() {
        // TEST 1 ) Empty hashmaps
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   new HashMap<Integer, List<Integer>>(),
                                                                   Lists.newArrayList("test"),
                                                                   ServerTestUtils.getLocalCluster(1),
                                                                   true,
                                                                   0);
        String jsonString = info.toJsonString();
        RebalancePartitionsInfo info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

        // TEST 2 ) With some replicas only
        HashMap<Integer, List<Integer>> testMap = new HashMap<Integer, List<Integer>>();
        testMap.put(1, Lists.newArrayList(1));
        info = new RebalancePartitionsInfo(0,
                                           1,
                                           testMap,
                                           Lists.newArrayList("test"),
                                           ServerTestUtils.getLocalCluster(1),
                                           false,
                                           0);
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);
        assertEquals(info.getReplicaToPartitionList(), info2.getReplicaToPartitionList());

        // TEST 3 ) With some more replicas
        testMap.put(3, Lists.newArrayList(1, 3, 5));
        info = new RebalancePartitionsInfo(0,
                                           1,
                                           testMap,
                                           Lists.newArrayList("test", "test2"),
                                           ServerTestUtils.getLocalCluster(1),
                                           false,
                                           0);
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);
        assertEquals(info.getReplicaToPartitionList(), info2.getReplicaToPartitionList());

    }

}

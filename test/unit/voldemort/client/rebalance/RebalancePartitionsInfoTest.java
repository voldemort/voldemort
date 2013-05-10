package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import voldemort.ServerTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RebalancePartitionsInfoTest {

    @Test
    public void testRebalancePartitionsInfoCreate() {
        // TEST 1 ) Empty hashmaps
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(0,
                                                                   1,
                                                                   new HashMap<String, HashMap<Integer, List<Integer>>>(),
                                                                   ServerTestUtils.getLocalCluster(1));
        String jsonString = info.toJsonString();
        RebalancePartitionsInfo info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

        HashMap<Integer, List<Integer>> testMap = new HashMap<Integer, List<Integer>>();
        testMap.put(1, Lists.newArrayList(1));
        HashMap<String, HashMap<Integer, List<Integer>>> storeTestMap1 = Maps.newHashMap();
        HashMap<String, HashMap<Integer, List<Integer>>> storeTestMap2 = Maps.newHashMap();

        // TEST 2 ) With empty maps
        info = new RebalancePartitionsInfo(0, 1, storeTestMap1, ServerTestUtils.getLocalCluster(1));
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

        // TEST 3 ) With different stores
        storeTestMap1.put("test1", testMap);
        storeTestMap2.put("test2", testMap);

        try {
            info = new RebalancePartitionsInfo(0,
                                               1,
                                               storeTestMap1,
                                               ServerTestUtils.getLocalCluster(1));

            fail("Should have thrown an exception");
        } catch(Exception e) {}

        // TEST 3 ) With some more replicas
        testMap.put(3, Lists.newArrayList(1, 3, 5));
        info = new RebalancePartitionsInfo(0, 1, storeTestMap1, ServerTestUtils.getLocalCluster(1));
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

    }

}

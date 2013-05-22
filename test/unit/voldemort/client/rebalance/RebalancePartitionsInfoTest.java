/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;

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

        // TEST 2 ) With empty maps
        info = new RebalancePartitionsInfo(0, 1, storeTestMap1, ServerTestUtils.getLocalCluster(1));
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

        // TEST 3 ) With some more replicas
        testMap.put(3, Lists.newArrayList(1, 3, 5));
        info = new RebalancePartitionsInfo(0, 1, storeTestMap1, ServerTestUtils.getLocalCluster(1));
        jsonString = info.toJsonString();
        info2 = RebalancePartitionsInfo.create(jsonString);
        assertEquals(info, info2);

    }

}

/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.server.rebalance;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalancePartitionsInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Test for {@link RebalancerState}
 */
public class RebalancerStateTest {

    @Test
    public void testToJson() {
        HashMap<Integer, List<Integer>> replicaToPartitionList = Maps.newHashMap();
        replicaToPartitionList.put(0, Lists.newArrayList(0, 1, 2));

        HashMap<Integer, List<Integer>> replicaToPartitionList2 = Maps.newHashMap();
        replicaToPartitionList2.put(1, Lists.newArrayList(3, 4));
        replicaToPartitionList2.put(3, Lists.newArrayList(5, 6));

        HashMap<String, HashMap<Integer, List<Integer>>> storeToReplicaToAddPartitionList = Maps.newHashMap();
        storeToReplicaToAddPartitionList.put("test1", replicaToPartitionList);
        storeToReplicaToAddPartitionList.put("test2", replicaToPartitionList2);

        List<RebalancePartitionsInfo> rebalancePartitionsInfos = Arrays.asList(new RebalancePartitionsInfo(2,
                                                                                                           0,
                                                                                                           storeToReplicaToAddPartitionList,
                                                                                                           ServerTestUtils.getLocalCluster(1)),
                                                                               new RebalancePartitionsInfo(3,
                                                                                                           1,
                                                                                                           storeToReplicaToAddPartitionList,
                                                                                                           ServerTestUtils.getLocalCluster(2)));

        RebalancerState in = new RebalancerState(rebalancePartitionsInfos);
        String jsonIn = in.toJsonString();
        RebalancerState out = RebalancerState.create(jsonIn);
        assertEquals(in, out);

        String jsonOut = out.toJsonString();
        assertEquals(jsonIn, jsonOut);

    }
}

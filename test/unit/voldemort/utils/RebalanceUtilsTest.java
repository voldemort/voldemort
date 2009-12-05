/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.cluster.Cluster;

public class RebalanceUtilsTest extends TestCase {

    private String testStoreName = "test-store";

    public void testGetStealPartitionsMap() {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 1, 2, 3, 4 },
                { 5, 6, 7, 8 } });
        Cluster targetCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 1, 2 },
                { 5, 6, 8 }, { 3, 4, 7 } });

        Map<Integer, List<RebalanceStealInfo>> map = RebalanceUtils.getStealPartitionsMap(testStoreName,
                                                                                          currentCluster,
                                                                                          targetCluster);
        checkStealPartitions(2, map, Arrays.asList(Arrays.asList(3, 4), Arrays.asList(7)));
        System.out.println(RebalanceUtils.getStealPartitionsMapAsString(map));
    }

    private void checkStealPartitions(int nodeId,
                                      Map<Integer, List<RebalanceStealInfo>> stealPartitionsMap,
                                      List<List<Integer>> nodeToStealPartition) {
        assertEquals("NodeId should be present in stealMap",
                     true,
                     stealPartitionsMap.containsKey(nodeId));

        List<RebalanceStealInfo> stealInfoList = stealPartitionsMap.get(nodeId);
        assertEquals("stealInfo sizes should match",
                     stealInfoList.size(),
                     nodeToStealPartition.size());

        for(RebalanceStealInfo stealInfo: stealInfoList) {
            assertEquals("NodeId should be present in expected list",
                         true,
                         stealInfo.getDonorId() < nodeToStealPartition.size());
            Collections.sort(stealInfo.getPartitionList());
            Collections.sort(nodeToStealPartition.get(stealInfo.getDonorId()));
            assertEquals("partitionList should match",
                         stealInfo.getPartitionList(),
                         nodeToStealPartition.get(stealInfo.getDonorId()));
        }
    }

    public void testRebalanceStealInfo() {
        RebalanceStealInfo info = new RebalanceStealInfo("test-store", 3, Arrays.asList(1,
                                                                                        2,
                                                                                        3,
                                                                                        4,
                                                                                        5), 1);
        assertEquals("fromString(toString) should match with original",
                     info.toString(),
                     new RebalanceStealInfo(info.toString()).toString());
        System.out.println(info.toString());
    }
}

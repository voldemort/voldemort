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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

public class RebalanceUtilsTest extends TestCase {

    private static String testStoreName = "test-replication-memory";
    private static String storeDefFile = "test/common/voldemort/config/stores.xml";

    private Cluster currentCluster;
    private Cluster targetCluster;
    List<StoreDefinition> storeDefList;

    @Override
    public void setUp() {
        currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {}, {}, {} });

        targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 4, 5, 6, 7, 8 },
                { 2, 3 } });

        try {
            storeDefList = new StoreDefinitionsMapper().readStoreList(new FileReader(new File(storeDefFile)));
        } catch(FileNotFoundException e) {
            throw new RuntimeException("Failed to find storeDefFile:" + storeDefFile, e);
        }
    }

    public void testRebalancePlan() {
        Queue<Pair<Integer, List<RebalanceStealInfo>>> rebalancePlan = RebalanceUtils.getRebalanceTaskQueue(currentCluster,
                                                                                                            targetCluster,
                                                                                                            Arrays.asList(testStoreName));
        int[][] stealList = { {}, { 2, 3 } };

        // the rebalancing plan should have exactly 3 entries.
        assertEquals("There should be three node rebalancing", 1, rebalancePlan.size());
        for(Pair<Integer, List<RebalanceStealInfo>> rebalanceInfo: rebalancePlan) {
            assertEquals("rebalanceInfo should have exactly one item", 1, rebalanceInfo.getSecond()
                                                                                       .size());
            RebalanceStealInfo expected = new RebalanceStealInfo(rebalanceInfo.getFirst(),
                                                                 0,
                                                                 listFromArray(stealList[rebalanceInfo.getFirst()]),
                                                                 Arrays.asList(testStoreName),
                                                                 0);

            assertEquals("rebalanceStealInfo should match",
                         expected.toJsonString(),
                         rebalanceInfo.getSecond().get(0).toJsonString());
        }
    }

    public void testUpdateCluster() {
        Cluster updatedCluster = RebalanceUtils.updateCluster(currentCluster,
                                                              new ArrayList<Node>(targetCluster.getNodes()));
        assertEquals("updated cluster should match targetCluster", updatedCluster, targetCluster);
    }

    private List<Integer> listFromArray(int[] array) {
        List<Integer> list = new ArrayList<Integer>();
        for(int x: array) {
            list.add(x);
        }

        return list;
    }

    public void testRebalanceStealInfo() {
        RebalanceStealInfo info = new RebalanceStealInfo(0,
                                                         1,
                                                         Arrays.asList(1, 2, 3, 4),
                                                         Arrays.asList("test1", "test2"),
                                                         0);
        System.out.println("info:" + info.toString());

        assertEquals("RebalanceStealInfo fromString --> toString should match.",
                     info.toString(),
                     (new RebalanceStealInfo(info.toJsonString())).toString());
    }

}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.mockito.Mockito;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.task.StealerBasedRebalanceTask;
import voldemort.cluster.Cluster;

public class RebalanceSchedulerTest {

    private static List<StealerBasedRebalanceTask> sbTaskList = new ArrayList<StealerBasedRebalanceTask>();
    private static ExecutorService service = Executors.newFixedThreadPool(3);
    private static RebalanceScheduler scheduler = new RebalanceScheduler(service, 10);
    private static AdminClient adminClient;
    private final Semaphore donorPermit = new Semaphore(1);
    private final RebalanceBatchPlanProgressBar progressBar = new RebalanceBatchPlanProgressBar(0,
                                                                                                1,
                                                                                                1);
    @Test
    public void test() {

        RebalanceScheduler mockedScheduler = Mockito.spy(scheduler);
        Cluster zzCurrent = ClusterTestUtils.getZZCluster();
        adminClient = ServerTestUtils.getAdminClient(zzCurrent);

        Map<String, HashMap<Integer, List<Integer>>> outerMap = new HashMap<String, HashMap<Integer, List<Integer>>>();
        Map<Integer, List<Integer>> innerMap = new HashMap<Integer, List<Integer>>();

        List<Integer> someList = Arrays.asList(0, 1, 2);
        innerMap.put(0, someList);
        outerMap.put("storeA", (HashMap<Integer, List<Integer>>) innerMap);
        int stealerId = 0;
        int donorId = 1;

        RebalancePartitionsInfo partitionsInfo = new RebalancePartitionsInfo(stealerId,
                                                                             donorId,
                                                                             (HashMap<String, HashMap<Integer, List<Integer>>>) outerMap,
                                                                             zzCurrent);

        StealerBasedRebalanceTask sbTask = new StealerBasedRebalanceTask(0,
                                                                         0,
                                                                         partitionsInfo,
                                                                         3,
                                                                         donorPermit,
                                                                         adminClient,
                                                                         progressBar,
                                                                         mockedScheduler);
        sbTaskList.add(sbTask);
        sbTaskList.add(sbTask);
        sbTaskList.add(sbTask);
        sbTaskList.add(sbTask);
        sbTaskList.add(sbTask);

        mockedScheduler.initializeLatch(sbTaskList.size());
        mockedScheduler.populateTasksByStealer(sbTaskList);

        // In the beginning both stealer and donor are idle so scheduler should return the scheduled 
        // task
        StealerBasedRebalanceTask scheduledTask = mockedScheduler.scheduleNextTask(false);
        org.junit.Assert.assertNotNull(sbTask);
        org.junit.Assert.assertEquals(sbTask, scheduledTask);
        mockedScheduler.removeNodesFromWorkerList(Arrays.asList(stealerId, donorId));

        // Now lets remove the donor from the worker list so that the donor is idle and add the 
        // stealer to the worker list. The scheduler should return null as it won't be able to 
        // schedule the task.
        mockedScheduler.addNodesToWorkerList(Arrays.asList(stealerId));
        mockedScheduler.removeNodesFromWorkerList(Arrays.asList(donorId));
        org.junit.Assert.assertEquals(mockedScheduler.scheduleNextTask(false), null);

        // This time stealer doesn't have any work but donor does
        mockedScheduler.addNodesToWorkerList(Arrays.asList(donorId));
        mockedScheduler.removeNodesFromWorkerList(Arrays.asList(stealerId));
        org.junit.Assert.assertEquals(mockedScheduler.scheduleNextTask(false), null);

        // And now both stealer and donor have work to do
        mockedScheduler.addNodesToWorkerList(Arrays.asList(donorId));
        mockedScheduler.addNodesToWorkerList(Arrays.asList(stealerId));
        org.junit.Assert.assertEquals(mockedScheduler.scheduleNextTask(false), null);

        // And stealer and donor are idle again
        mockedScheduler.removeNodesFromWorkerList(Arrays.asList(stealerId, donorId));
        StealerBasedRebalanceTask nextscheduledTask = mockedScheduler.scheduleNextTask(false);
        org.junit.Assert.assertNotNull(sbTask);
        org.junit.Assert.assertEquals(sbTask, nextscheduledTask);

     
    }
}

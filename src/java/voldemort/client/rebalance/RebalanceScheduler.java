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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.rebalance.task.RebalanceTask;
import voldemort.client.rebalance.task.StealerBasedRebalanceTask;
import voldemort.server.rebalance.VoldemortRebalancingException;

// TODO: This scheduler deprecates the need for donor permits. Consider removing
// them.
/**
 * Scheduler for rebalancing tasks. There is at most one rebalancing task per
 * stealer-donor pair. This scheduler ensures the following invariant is obeyed:
 * 
 * A node works on no more than one rebalancing task at a time.
 * 
 * Note that a node working on a rebalancing task may be either a stealer or a
 * donor. This invariant should somewhat isolate the foreground workload against
 * the work a server must do for rebalancing. Because of this isolation, it is
 * safe to attempt "infinite" parallelism since no more than floor(number of
 * nodes / 2) rebalancing tasks can possibly be scheduled to execute while
 * obeying the invariant.
 * 
 * The order of tasks are randomized within this class. The intent is to
 * "spread" rebalancing work smoothly out over the cluster and avoid
 * "long tails" of straggler rebalancing tasks. Only experience will tell us if
 * we need to do anything smarter.
 */
public class RebalanceScheduler {

    private static final Logger logger = Logger.getLogger(RebalanceScheduler.class);

    private final ExecutorService service;
    private final int maxParallelRebalancing;
    private Map<Integer, List<StealerBasedRebalanceTask>> tasksByStealer;
    private int numTasksExecuting;
    private Set<Integer> nodeIdsWithWork;
    private CountDownLatch doneSignal;

    public RebalanceScheduler(ExecutorService service, int maxParallelRebalancing) {
        this.service = service;
        this.maxParallelRebalancing = maxParallelRebalancing;
        this.tasksByStealer = new HashMap<Integer, List<StealerBasedRebalanceTask>>();
        this.numTasksExecuting = 0;
        this.nodeIdsWithWork = new TreeSet<Integer>();
    }

    /**
     * Initialize the count down latch.
     * 
     * @param size of the task list
     */
    protected void initializeLatch(int size) {
        this.doneSignal = new CountDownLatch(size);
    }

    /**
     * Go over the task list and create a map of stealerId -> Tasks
     * 
     * @param sbTaskList List of all stealer-based rebalancing tasks to be
     *        scheduled.
     */
    protected void populateTasksByStealer(List<StealerBasedRebalanceTask> sbTaskList) {
        // Setup mapping of stealers to work for this run.
        for(StealerBasedRebalanceTask task: sbTaskList) {
            if(task.getStealInfos().size() != 1) {
                throw new VoldemortException("StealerBasedRebalanceTasks should have a list of RebalancePartitionsInfo of length 1.");
            }

            RebalanceTaskInfo stealInfo = task.getStealInfos().get(0);
            int stealerId = stealInfo.getStealerId();
            if(!this.tasksByStealer.containsKey(stealerId)) {
                this.tasksByStealer.put(stealerId, new ArrayList<StealerBasedRebalanceTask>());
            }
            this.tasksByStealer.get(stealerId).add(task);
        }

        if(tasksByStealer.isEmpty()) {
            return;
        }
        // Shuffle order of each stealer's work list. This randomization
        // helps to get rid of any "patterns" in how rebalancing tasks were
        // added to the task list passed in.
        for(List<StealerBasedRebalanceTask> taskList: tasksByStealer.values()) {
            Collections.shuffle(taskList);
        }
    }

    /**
     * Set up scheduling structures and then start scheduling tasks to execute.
     * Blocks until all tasks have been scheduled. (For all tasks to be
     * scheduled, most tasks must have completed.)
     * 
     * @param sbTaskList List of all stealer-based rebalancing tasks to be
     *        scheduled.
     */
    public void run(List<StealerBasedRebalanceTask> sbTaskList) {
        initializeLatch(sbTaskList.size());
        populateTasksByStealer(sbTaskList);
        // Start scheduling tasks to execute!
        scheduleMoreTasks();
        try {
            doneSignal.await();
        } catch(InterruptedException e) {
            logger.error("RebalancController scheduler interrupted while waiting for rebalance "
                         + "tasks to be scheduled.", e);
            throw new VoldemortRebalancingException("RebalancController scheduler interrupted "
                                                    + "while waiting for rebalance tasks to be "
                                                    + "scheduled.");
        }
    }

    /**
     * Schedule as many tasks as possible.
     * 
     * To schedule a task is to make it available to the executor service to
     * run.
     * 
     * "As many tasks as possible" is limited by (i) the parallelism level
     * permitted and (ii) the invariant that a node shall be part of at most one
     * rebalancing task at a time (as either stealer or donor).
     */
    private synchronized void scheduleMoreTasks() {
        RebalanceTask scheduledTask = scheduleNextTask();
        while(scheduledTask != null) {
            scheduledTask = scheduleNextTask();
        }
    }

    /**
     * Schedule at most one task.
     */
    private synchronized StealerBasedRebalanceTask scheduleNextTask() {
        return scheduleNextTask(true);
    }

    /**
     * Schedule at most one task.
     * 
     * The scheduled task *must* invoke 'doneTask()' upon
     * completion/termination.
     * 
     * @param executeService flag to control execution of the service, some tests pass
     *        in value 'false'
     * @return The task scheduled or null if not possible to schedule a task at
     *         this time.
     */
    protected synchronized StealerBasedRebalanceTask scheduleNextTask(boolean executeService) {
        // Make sure there is work left to do.
        if(doneSignal.getCount() == 0) {
            logger.info("All tasks completion signaled... returning");

            return null;
        }
        // Limit number of tasks outstanding.
        if(this.numTasksExecuting >= maxParallelRebalancing) {
            logger.info("Executing more tasks than [" + this.numTasksExecuting
                        + "] the parallel allowed " + maxParallelRebalancing);
            return null;
        }
        // Shuffle list of stealer IDs each time a new task to schedule needs to
        // be found. Randomizing the order should avoid prioritizing one
        // specific stealer's work ahead of all others.
        List<Integer> stealerIds = new ArrayList<Integer>(tasksByStealer.keySet());
        Collections.shuffle(stealerIds);
        for(int stealerId: stealerIds) {
            if(nodeIdsWithWork.contains(stealerId)) {
                logger.info("Stealer " + stealerId + " is already working... continuing");
                continue;
            }

            for(StealerBasedRebalanceTask sbTask: tasksByStealer.get(stealerId)) {
                int donorId = sbTask.getStealInfos().get(0).getDonorId();
                if(nodeIdsWithWork.contains(donorId)) {
                    logger.info("Stealer " + stealerId + " Donor " + donorId
                                + " is already working... continuing");
                    continue;
                }
                // Book keeping
                addNodesToWorkerList(Arrays.asList(stealerId, donorId));
                numTasksExecuting++;
                // Remove this task from list thus destroying list being
                // iterated over. This is safe because returning directly out of
                // this branch.
                tasksByStealer.get(stealerId).remove(sbTask);
                try {
                    if(executeService) {
                        logger.info("Stealer " + stealerId + " Donor " + donorId
                                    + " going to schedule work");
                        service.execute(sbTask);
                    }
                } catch(RejectedExecutionException ree) {
                    logger.error("Stealer " + stealerId
                                 + "Rebalancing task rejected by executor service.", ree);
                    throw new VoldemortRebalancingException("Stealer "
                                                            + stealerId
                                                            + "Rebalancing task rejected by executor service.");
                }
                return sbTask;
            }
        }
        printRemainingTasks(stealerIds);
        return null;
    }

    private void printRemainingTasks(List<Integer> stealerIds) {
        for(int stealerId: stealerIds) {
            List<Integer> donorIds = new ArrayList<Integer>();
            for(StealerBasedRebalanceTask sbTask: tasksByStealer.get(stealerId)) {
                int donorId = sbTask.getStealInfos().get(0).getDonorId();
                donorIds.add(donorId);
            }
            logger.info(" Remaining work for Stealer " + stealerId + " Donors : " + donorIds);
        }
    }

    /**
     * Add nodes to the workers list
     * 
     * @param nodeIds list of node ids.
     */
    public synchronized void addNodesToWorkerList(List<Integer> nodeIds) {
        // Bookkeeping for nodes that will be involved in the next task
        nodeIdsWithWork.addAll(nodeIds);
        logger.info("Node IDs with work: " + nodeIdsWithWork + " Newly added nodes " + nodeIds);
    }

    /**
     * Removes nodes from the workers list
     * 
     * @param nodeIds list of node ids.
     */
    public synchronized void removeNodesFromWorkerList(List<Integer> nodeIds) {
        // Bookkeeping for nodes that have finished the task.
        nodeIdsWithWork.removeAll(nodeIds);
    }

    /**
     * Method must be invoked upon completion of a rebalancing task. It is the
     * task's responsibility to do so.
     * 
     * @param stealerId
     * @param donorId
     */
    public synchronized void doneTask(int stealerId, int donorId) {
        removeNodesFromWorkerList(Arrays.asList(stealerId, donorId));
        numTasksExecuting--;
        doneSignal.countDown();
        // Try and schedule more tasks now that resources may be available to do
        // so.
        scheduleMoreTasks();
    }
}

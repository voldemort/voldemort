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

import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.utils.Utils;

public class RebalanceBatchPlanProgressBar {

    private static final Logger logger = Logger.getLogger(RebalanceBatchPlanProgressBar.class);
    private static final DecimalFormat decimalFormatter = new DecimalFormat("#.##");

    private final int batchId;
    private final int taskCount;
    private final int partitionStoreCount;

    private final long startTimeMs;

    private Set<Integer> tasksInFlight;
    private int numTasks;
    private int numPartitionStores;

    RebalanceBatchPlanProgressBar(int batchId, int taskCount, int partitionStoreCount) {
        this.batchId = batchId;
        this.taskCount = taskCount;
        this.partitionStoreCount = partitionStoreCount;

        this.startTimeMs = System.currentTimeMillis();

        this.tasksInFlight = new HashSet<Integer>();
        this.numTasks = 0;
        this.numPartitionStores = 0;
    }

    /**
     * Called whenever a rebalance task starts.
     * 
     * @param taskId
     */
    synchronized public void beginTask(int taskId) {
        tasksInFlight.add(taskId);

        updateProgressBar();
    }

    /**
     * Called whenever a rebalance task completes. This means one task is done
     * and some number of partition stores have been moved.
     * 
     * @param taskId
     * @param partitionStoreCount Number of partition stores moved by this
     *        completed task.
     */
    synchronized public void completeTask(int taskId, int taskPartitionStores) {
        tasksInFlight.remove(taskId);

        numTasks++;
        numPartitionStores += taskPartitionStores;

        updateProgressBar();
    }

    /**
     * Construct a pretty string documenting progress for this batch plan thus
     * far.
     * 
     * @return
     */
    synchronized public String getPrettyProgressBar() {
        StringBuilder sb = new StringBuilder();

        double taskRate = numTasks / (double) taskCount;
        double partitionStoreRate = numPartitionStores / (double) partitionStoreCount;

        long deltaTimeMs = System.currentTimeMillis() - startTimeMs;
        long taskTimeRemainingMs = Long.MAX_VALUE;
        if(taskRate > 0) {
            taskTimeRemainingMs = (long) (deltaTimeMs * ((1.0 / taskRate) - 1.0));
        }
        long partitionStoreTimeRemainingMs = Long.MAX_VALUE;
        if(partitionStoreRate > 0) {
            partitionStoreTimeRemainingMs = (long) (deltaTimeMs * ((1.0 / partitionStoreRate) - 1.0));
        }

        // Title line
        sb.append("Progess update on rebalancing batch " + batchId).append(Utils.NEWLINE);
        // Tasks in flight update
        sb.append("There are currently " + tasksInFlight.size() + " rebalance tasks executing: ")
          .append(tasksInFlight)
          .append(".")
          .append(Utils.NEWLINE);
        // Tasks completed update
        sb.append("\t" + numTasks + " out of " + taskCount + " rebalance tasks complete.")
          .append(Utils.NEWLINE)
          .append("\t")
          .append(decimalFormatter.format(taskRate * 100.0))
          .append("% done, estimate ")
          .append(taskTimeRemainingMs)
          .append(" ms (")
          .append(TimeUnit.MILLISECONDS.toMinutes(taskTimeRemainingMs))
          .append(" minutes) remaining.")
          .append(Utils.NEWLINE);
        // Partition-stores migrated update
        sb.append("\t" + numPartitionStores + " out of " + partitionStoreCount
                  + " partition-stores migrated.")
          .append(Utils.NEWLINE)
          .append("\t")
          .append(decimalFormatter.format(partitionStoreRate * 100.0))
          .append("% done, estimate ")
          .append(partitionStoreTimeRemainingMs)
          .append(" ms (")
          .append(TimeUnit.MILLISECONDS.toMinutes(partitionStoreTimeRemainingMs))
          .append(" minutes) remaining.")
          .append(Utils.NEWLINE);
        return sb.toString();
    }

    public void updateProgressBar() {
        if(logger.isInfoEnabled()) {
            String progressBar = getPrettyProgressBar();
            logger.info(progressBar);
        }
    }
}

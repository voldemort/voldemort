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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.utils.Utils;

public class RebalanceBatchPlanProgressBar {

    private static final Logger logger = Logger.getLogger(RebalanceBatchPlanProgressBar.class);
    private static final DecimalFormat decimalFormatter = new DecimalFormat("#.##");

    private final int batchId;
    private final int totalTaskCount;
    private final int totalPartitionStoreCount;

    private final long startTimeMs;

    private Set<Integer> tasksInFlight;
    private int numTasksCompleted;
    private int numPartitionStoresMigrated;

    /**
     * Construct a progress bar object to track rebalance tasks completed and
     * partition-stores migrated.
     * 
     * @param batchId
     * @param totalTaskCount
     * @param totalPartitionStoreCount
     */
    RebalanceBatchPlanProgressBar(int batchId, int totalTaskCount, int totalPartitionStoreCount) {
        this.batchId = batchId;
        this.totalTaskCount = totalTaskCount;
        this.totalPartitionStoreCount = totalPartitionStoreCount;

        this.startTimeMs = System.currentTimeMillis();

        this.tasksInFlight = new TreeSet<Integer>();
        this.numTasksCompleted = 0;
        this.numPartitionStoresMigrated = 0;
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
     * and some number of partition stores have been migrated.
     * 
     * @param taskId
     * @param totalPartitionStoreCount Number of partition stores moved by this
     *        completed task.
     */
    synchronized public void completeTask(int taskId, int partitionStoresMigrated) {
        tasksInFlight.remove(taskId);

        numTasksCompleted++;
        numPartitionStoresMigrated += partitionStoresMigrated;

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

        double taskRate = numTasksCompleted / (double) totalTaskCount;
        double partitionStoreRate = numPartitionStoresMigrated / (double) totalPartitionStoreCount;

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
        sb.append("Progress update on rebalancing batch " + batchId).append(Utils.NEWLINE);
        // Tasks in flight update
        sb.append("There are currently " + tasksInFlight.size() + " rebalance tasks executing: ")
          .append(tasksInFlight)
          .append(".")
          .append(Utils.NEWLINE);
        // Tasks completed update
        sb.append("\t" + numTasksCompleted + " out of " + totalTaskCount
                  + " rebalance tasks complete.")
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
        sb.append("\t" + numPartitionStoresMigrated + " out of " + totalPartitionStoreCount
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

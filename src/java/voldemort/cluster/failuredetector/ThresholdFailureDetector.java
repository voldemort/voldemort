/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import org.apache.log4j.Level;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class ThresholdFailureDetector extends AbstractFailureDetector {

    public ThresholdFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig, ThresholdNodeStatus.class);
    }

    public void recordException(Node node, UnreachableStoreException e) {
        update(node, 0, 1, e);
    }

    public void recordSuccess(Node node) {
        update(node, 1, 1, null);
    }

    @Override
    public boolean isAvailable(Node node) {
        return update(node, 0, 0, null);
    }

    public void destroy() {}

    private boolean update(Node node, int successDelta, int totalDelta, UnreachableStoreException e) {
        ThresholdNodeStatus nodeStatus = (ThresholdNodeStatus) getNodeStatus(node);

        // We don't actually call the needsNotifyAvailable or notifyUnavailable
        // *after* we exit the synchronized block to avoid nested locks.
        boolean needsNotifyAvailable = false;
        boolean needsNotifyUnavailable = false;
        long threshold = 0;
        boolean isAvailable = false;

        synchronized(nodeStatus) {
            nodeStatus.setLastChecked(getConfig().getTime().getMilliseconds());

            if(nodeStatus.lastChecked >= nodeStatus.startMillis
                                         + getConfig().getThresholdInterval()) {
                // If the node was not previously available and is now, then we
                // need to notify everyone of that fact.
                needsNotifyAvailable = !nodeStatus.isAvailable;

                nodeStatus.isAvailable = true;
                nodeStatus.startMillis = nodeStatus.lastChecked;
                nodeStatus.success = successDelta;
                nodeStatus.total = totalDelta;
            } else {
                nodeStatus.success += successDelta;
                nodeStatus.total += totalDelta;
            }

            int thresholdCountMinimum = getConfig().getThresholdCountMinimum();

            if(nodeStatus.total >= thresholdCountMinimum) {
                threshold = nodeStatus.total >= thresholdCountMinimum ? (nodeStatus.success * 100)
                                                                        / nodeStatus.total : 100;
                boolean previouslyAvailable = nodeStatus.isAvailable;
                nodeStatus.isAvailable = threshold >= getConfig().getThreshold();

                if(nodeStatus.isAvailable && !previouslyAvailable)
                    needsNotifyAvailable = true;
                else if(!nodeStatus.isAvailable && previouslyAvailable)
                    needsNotifyUnavailable = true;
            }

            isAvailable = nodeStatus.isAvailable;
        }

        if(needsNotifyAvailable) {
            if(logger.isInfoEnabled())
                logger.info("Threshold for node " + node.getId() + " at " + node.getHost()
                            + " now " + threshold + "%; marking as available");

            notifyAvailable(node);
        } else if(needsNotifyUnavailable) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Threshold for node " + node.getId() + " at " + node.getHost()
                            + " now " + threshold + "%; marking as unavailable");

            notifyUnavailable(node);
        }

        return isAvailable;
    }

    @SuppressWarnings("serial")
    public static class ThresholdNodeStatus extends NodeStatus {

        private long startMillis;

        private long success;

        private long total;

        public ThresholdNodeStatus(Time time) {
            super(time);

            this.startMillis = time.getMilliseconds();
            this.lastChecked = startMillis;
        }

    }

}

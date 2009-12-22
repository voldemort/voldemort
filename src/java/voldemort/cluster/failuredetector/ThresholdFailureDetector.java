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

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class ThresholdFailureDetector extends AsyncRecoveryFailureDetector {

    public ThresholdFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
    }

    @Override
    public void recordException(Node node, UnreachableStoreException e) {
        update(node, 0, e);
    }

    @Override
    public void recordSuccess(Node node) {
        update(node, 1, null);
    }

    /**
     * We delegate node recovery detection to the
     * {@link AsyncRecoveryFailureDetector} class. When it determines that the
     * node has recovered, this callback is executed with the newly-recovered
     * node.
     */

    @Override
    protected void nodeRecovered(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            nodeStatus.setStartMillis(getConfig().getTime().getMilliseconds());
            nodeStatus.setSuccess(0);
            nodeStatus.setTotal(0);
            setAvailable(node);
        }
    }

    private void update(Node node, int successDelta, UnreachableStoreException e) {
        if(logger.isTraceEnabled()) {
            if(e != null)
                logger.trace(node + " updated, successDelta: " + successDelta, e);
            else
                logger.trace(node + " updated, successDelta: " + successDelta);
        }

        final long currentTime = getConfig().getTime().getMilliseconds();

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            if(currentTime >= nodeStatus.getStartMillis() + getConfig().getThresholdInterval()) {
                // We've passed into a new interval, so reset our counts
                // appropriately.
                nodeStatus.setStartMillis(currentTime);
                nodeStatus.setSuccess(successDelta);
                nodeStatus.setTotal(1);
            } else {
                nodeStatus.incrementSuccess(successDelta);
                nodeStatus.incrementTotal(1);

                if(nodeStatus.getTotal() >= getConfig().getThresholdCountMinimum()) {
                    long newThreshold = (nodeStatus.getSuccess() * 100) / nodeStatus.getTotal();

                    if(logger.isTraceEnabled())
                        logger.trace(node + " threshold: " + newThreshold);

                    if(newThreshold >= getConfig().getThreshold())
                        setAvailable(node);
                    else
                        setUnavailable(node, e);
                }
            }
        }
    }
}

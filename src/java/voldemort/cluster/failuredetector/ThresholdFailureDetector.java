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
public class ThresholdFailureDetector extends AbstractFailureDetector {

    public ThresholdFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
    }

    public void recordException(Node node, UnreachableStoreException e) {
        update(node, 0, 1, e);
    }

    public void recordSuccess(Node node) {
        update(node, 1, 1, null);
    }

    public boolean isAvailable(Node node) {
        return update(node, 0, 0, null);
    }

    private boolean update(Node node, int successDelta, int totalDelta, UnreachableStoreException e) {
        final long currentTime = getConfig().getTime().getMilliseconds();
        final long thresholdInterval = getConfig().getThresholdInterval();
        final int thresholdCountMinimum = getConfig().getThresholdCountMinimum();
        final int threshold = getConfig().getThreshold();

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            if(currentTime >= nodeStatus.getStartMillis() + thresholdInterval) {
                // We've passed into a new interval, so we're by default
                // available. Reset our counts appropriately.
                nodeStatus.setStartMillis(currentTime);
                nodeStatus.setSuccess(successDelta);
                nodeStatus.setTotal(totalDelta);

                setAvailable(node);
            } else {
                nodeStatus.incrementSuccess(successDelta);
                nodeStatus.incrementTotal(totalDelta);

                if(nodeStatus.getTotal() >= thresholdCountMinimum) {
                    long newThreshold = (nodeStatus.getSuccess() * 100) / nodeStatus.getTotal();

                    if(newThreshold >= threshold)
                        setAvailable(node);
                    else
                        setUnavailable(node, e);
                }
            }

            return nodeStatus.isAvailable();
        }
    }

}

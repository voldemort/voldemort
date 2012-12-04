/*
 * Copyright 2009-2012 LinkedIn, Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

/**
 * ThresholdFailureDetector builds upon the AsyncRecoveryFailureDetector and
 * provides a more lenient for marking nodes as unavailable. Fundamentally, for
 * each node, the ThresholdFailureDetector keeps track of a "success ratio"
 * which is a ratio of successful operations to total operations and requires
 * that ratio to meet or exceed a threshold. That is, every call to
 * recordException or recordSuccess increments the total count while only calls
 * to recordSuccess increments the success count. Calls to recordSuccess
 * increase the success ratio while calls to recordException by contrast
 * decrease the success ratio.
 * 
 * <p/>
 * 
 * As the success ratio threshold continues to exceed the threshold, the node
 * will be considered as available. Once the success ratio dips below the
 * threshold, the node is marked as unavailable. As this class extends the
 * AsyncRecoveryFailureDetector, an unavailable node is only marked as available
 * once a background thread has been able to contact the node asynchronously.
 * 
 * <p/>
 * 
 * There is also a minimum number of requests that must occur before the success
 * ratio is checked against the threshold. This is to prevent occurrences like 1
 * failure out of 1 attempt yielding a success ratio of 0%. There is also a
 * threshold interval which means that the success ratio for a given node is
 * only "valid" for a certain period of time, after which it is reset. This
 * prevents scenarios like 100,000,000 successful requests (and thus 100%
 * success threshold) overshadowing a subsequent stream of 10,000,000 failures
 * because this is only 10% of the total and above a given threshold.
 * 
 */

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class ThresholdFailureDetector extends AsyncRecoveryFailureDetector {

    public ThresholdFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
    }

    @Override
    public void recordException(Node node, long requestTime, UnreachableStoreException e) {
        checkArgs(node, requestTime);
        update(node, 0, e);
    }

    @Override
    public void recordSuccess(Node node, long requestTime) {
        checkArgs(node, requestTime);

        int successDelta = 1;
        UnreachableStoreException e = null;

        if(requestTime > getConfig().getRequestLengthThreshold()) {
            // Consider slow requests as "soft" errors that are counted against
            // us in our success threshold.
            e = new UnreachableStoreException("Node " + node.getId()
                                              + " recording success, but request time ("
                                              + requestTime + ") exceeded threshold ("
                                              + getConfig().getRequestLengthThreshold() + ")");

            successDelta = 0;
        }

        update(node, successDelta, e);
    }

    @JmxGetter(name = "nodeThresholdStats", description = "Each node is listed with its status (available/unavailable) and success percentage")
    public String getNodeThresholdStats() {
        List<String> list = new ArrayList<String>();

        for(Node node: getConfig().getCluster().getNodes()) {
            NodeStatus nodeStatus = getNodeStatus(node);
            boolean isAvailabile = false;
            long percentage = 0;

            synchronized(nodeStatus) {
                isAvailabile = nodeStatus.isAvailable();
                percentage = nodeStatus.getTotal() > 0 ? (nodeStatus.getSuccess() * 100)
                                                         / nodeStatus.getTotal() : 0;
            }

            list.add(node.getId() + ",status=" + (isAvailabile ? "available" : "unavailable")
                     + ",percentage=" + percentage + "%");
        }

        return StringUtils.join(list, ";");
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
            super.nodeRecovered(node);
        }
    }

    protected void update(Node node, int successDelta, UnreachableStoreException e) {
        if(logger.isTraceEnabled()) {
            if(e != null)
                logger.trace("Node " + node.getId() + " updated, successDelta: " + successDelta, e);
            else
                logger.trace("Node " + node.getId() + " updated, successDelta: " + successDelta);
        }

        final long currentTime = getConfig().getTime().getMilliseconds();
        String catastrophicError = getCatastrophicError(e);
        NodeStatus nodeStatus = getNodeStatus(node);

        boolean invokeSetAvailable = false;
        boolean invokeSetUnavailable = false;
        // Protect all logic to decide on available/unavailable w/in
        // synchronized section
        synchronized(nodeStatus) {
            if(currentTime >= nodeStatus.getStartMillis() + getConfig().getThresholdInterval()) {
                // We've passed into a new interval, so reset our counts
                // appropriately.
                nodeStatus.setStartMillis(currentTime);
                nodeStatus.setSuccess(successDelta);
                if(successDelta < 1)
                    nodeStatus.setFailure(1);
                nodeStatus.setTotal(1);
            } else {
                nodeStatus.incrementSuccess(successDelta);
                if(successDelta < 1)
                    nodeStatus.incrementFailure(1);
                nodeStatus.incrementTotal(1);

                if(catastrophicError != null) {
                    if(logger.isTraceEnabled())
                        logger.trace("Node " + node.getId() + " experienced catastrophic error: "
                                     + catastrophicError);

                    invokeSetUnavailable = true;
                } else if(nodeStatus.getFailure() >= getConfig().getThresholdCountMinimum()) {
                    long percentage = (nodeStatus.getSuccess() * 100) / nodeStatus.getTotal();

                    if(logger.isTraceEnabled())
                        logger.trace("Node " + node.getId() + " percentage: " + percentage + "%");

                    if(percentage >= getConfig().getThreshold())
                        invokeSetAvailable = true;
                    else
                        invokeSetUnavailable = true;
                }
            }
        }
        // Actually call set(Un)Available outside of synchronized section. This
        // ensures that side effects are not w/in a sync section (e.g., alerting
        // all the failure detector listeners).
        if(invokeSetAvailable) {
            setAvailable(node);
        } else if(invokeSetUnavailable) {
            setUnavailable(node, e);
        }
    }

    protected String getCatastrophicError(UnreachableStoreException e) {
        Throwable t = e != null ? e.getCause() : null;

        if(t == null)
            return null;

        for(String errorType: getConfig().getCatastrophicErrorTypes()) {
            if(t.getClass().getName().equals(errorType))
                return errorType;
        }

        return null;
    }

}

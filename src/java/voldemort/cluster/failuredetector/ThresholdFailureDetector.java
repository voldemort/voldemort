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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class ThresholdFailureDetector extends AbstractFailureDetector {

    protected final Map<Node, NodeData> nodeDataMap;

    public ThresholdFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
        nodeDataMap = new ConcurrentHashMap<Node, NodeData>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeDataMap.put(node, new NodeData(getConfig().getTime().getMilliseconds()));
    }

    public long getLastChecked(Node node) {
        return getNodeData(node).lastChecked;
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

    public void destroy() {}

    private boolean update(Node node, int successDelta, int totalDelta, UnreachableStoreException e) {
        if(e != null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        NodeData nd = getNodeData(node);

        // We don't actually call the needsNotifyAvailable or notifyUnavailable
        // *after* we exit the synchronized block to avoid nested locks.
        boolean needsNotifyAvailable = false;
        boolean needsNotifyUnavailable = false;
        long threshold = 0;
        boolean isAvailable = false;

        synchronized(nd) {
            nd.lastChecked = getConfig().getTime().getMilliseconds();

            if(nd.lastChecked >= nd.startMillis + getConfig().getThresholdInterval()) {
                // If the node was not previously available and is now, then we
                // need to notify everyone of that fact.
                needsNotifyAvailable = !nd.isAvailable;

                nd.isAvailable = true;
                nd.startMillis = nd.lastChecked;
                nd.success = successDelta;
                nd.total = totalDelta;
            } else {
                nd.success += successDelta;
                nd.total += totalDelta;
            }

            int thresholdCountMinimum = getConfig().getThresholdCountMinimum();

            if(nd.total >= thresholdCountMinimum) {
                threshold = nd.total >= thresholdCountMinimum ? (nd.success * 100) / nd.total : 100;
                boolean previouslyAvailable = nd.isAvailable;
                nd.isAvailable = threshold >= getConfig().getThreshold();

                if(nd.isAvailable && !previouslyAvailable)
                    needsNotifyAvailable = true;
                else if(!nd.isAvailable && previouslyAvailable)
                    needsNotifyUnavailable = true;
            }

            isAvailable = nd.isAvailable;
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

    private NodeData getNodeData(Node node) {
        NodeData nodeData = nodeDataMap.get(node);

        if(nodeData == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");

        return nodeData;
    }

    private static class NodeData {

        private long startMillis;

        private long lastChecked;

        private long success;

        private long total;

        private boolean isAvailable;

        public NodeData(long startMillis) {
            this.startMillis = startMillis;
            this.lastChecked = startMillis;
            this.isAvailable = true;
        }

    }

}

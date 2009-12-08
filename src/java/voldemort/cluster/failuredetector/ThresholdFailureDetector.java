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
            nodeDataMap.put(node, new NodeData(node));
    }

    public long getLastChecked(Node node) {
        return getNodeData(node).lastChecked;
    }

    public void recordException(Node node, UnreachableStoreException e) {
        getNodeData(node).update(0, 1, e);
    }

    public void recordSuccess(Node node) {
        getNodeData(node).update(1, 1, null);
    }

    public boolean isAvailable(Node node) {
        return getNodeData(node).update(0, 0, null);
    }

    public void destroy() {}

    private NodeData getNodeData(Node node) {
        NodeData nodeData = nodeDataMap.get(node);

        if(nodeData == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");
        return nodeData;
    }

    private class NodeData {

        private final Node node;

        private volatile long startMillis;

        private volatile long lastChecked;

        private volatile long success;

        private volatile long total;

        private volatile boolean isAvailable;

        public NodeData(Node node) {
            this.node = node;
            this.startMillis = getConfig().getTime().getMilliseconds();
            this.lastChecked = startMillis;
            this.isAvailable = true;
        }

        private boolean update(int successDelta, int totalDelta, UnreachableStoreException e) {
            long currTime = getConfig().getTime().getMilliseconds();

            if(currTime >= startMillis + getConfig().getThresholdInterval()) {
                startMillis = currTime;
                success = successDelta;
                total = totalDelta;
                isAvailable = true;
            } else {
                success += successDelta;
                total += totalDelta;
            }

            lastChecked = currTime;

            if(total >= getConfig().getThresholdCountMinimum()) {
                long newThreshold = total >= getConfig().getThresholdCountMinimum() ? (success * 100)
                                                                                      / total
                                                                                   : 100;
                boolean newAvailable = newThreshold >= getConfig().getThreshold();

                if(newAvailable && !isAvailable) {
                    if(logger.isInfoEnabled())
                        logger.info("Threshold for node " + node.getId() + " at " + node.getHost()
                                    + " now " + newThreshold + "%; marking as available");

                    notifyAvailable(node);
                } else if(!newAvailable && isAvailable) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn("Threshold for node " + node.getId() + " at " + node.getHost()
                                    + " now " + newThreshold + "%; marking as unavailable", e);

                    if(logger.isDebugEnabled())
                        logger.debug(e);

                    notifyUnavailable(node);
                }

                isAvailable = newAvailable;
            }

            return isAvailable;
        }
    }

}

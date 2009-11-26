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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.Node;

/**
 * AbstractFailureDetector serves as a building block for FailureDetector
 * implementations.
 * 
 * @author Kirk True
 */

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public abstract class AbstractFailureDetector implements FailureDetector {

    private final FailureDetectorConfig failureDetectorConfig;

    protected final Map<Node, NodeStatus> nodeStatusMap;

    protected final Set<FailureDetectorListener> listeners;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected AbstractFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        this.failureDetectorConfig = failureDetectorConfig;
        nodeStatusMap = new HashMap<Node, NodeStatus>();
        listeners = Collections.synchronizedSet(new HashSet<FailureDetectorListener>());
    }

    public long getLastChecked(Node node) {
        return getNodeStatus(node).getLastCheckedMs();
    }

    public void waitFor(Node node) throws InterruptedException {
        while(!isAvailable(node)) {
            if(logger.isInfoEnabled())
                logger.info("Sleeping for 1 second to check " + node);

            failureDetectorConfig.getTime().sleep(1000);
        }
    }

    @JmxOperation(impact = MBeanOperationInfo.INFO, description = "The number of available nodes")
    public int getNumberOfAvailableNodes() {
        List<NodeStatus> nodeStatuses = null;

        synchronized(nodeStatusMap) {
            nodeStatuses = new ArrayList<NodeStatus>(nodeStatusMap.values());
        }

        int available = 0;

        for(NodeStatus nodeStatus: nodeStatuses)
            if(nodeStatus.isAvailable())
                available++;

        return available;
    }

    public boolean isAvailable(Node node) {
        return !getNodeStatus(node).isUnavailable(failureDetectorConfig.getNodeBannagePeriod());
    }

    public void addFailureDetectorListener(FailureDetectorListener failureDetectorListener) {
        listeners.add(failureDetectorListener);
    }

    public void removeFailureDetectorListener(FailureDetectorListener failureDetectorListener) {
        listeners.remove(failureDetectorListener);
    }

    public FailureDetectorConfig getConfig() {
        return failureDetectorConfig;
    }

    protected void setAvailable(Node node) {
        getNodeStatus(node).setAvailable();

        Set<FailureDetectorListener> listenersCopy = new HashSet<FailureDetectorListener>(listeners);

        for(FailureDetectorListener fdl: listenersCopy) {
            try {
                fdl.nodeOnline(node);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }
    }

    protected void setUnavailable(Node node) {
        getNodeStatus(node).setUnavailable();

        Set<FailureDetectorListener> listenersCopy = new HashSet<FailureDetectorListener>(listeners);

        for(FailureDetectorListener fdl: listenersCopy) {
            try {
                fdl.nodeOffline(node);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }
    }

    private NodeStatus getNodeStatus(Node node) {
        synchronized(nodeStatusMap) {
            NodeStatus nodeStatus = nodeStatusMap.get(node);

            if(nodeStatus == null) {
                nodeStatus = new NodeStatus();
                nodeStatusMap.put(node, nodeStatus);
            }

            return nodeStatus;
        }
    }

}

/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

/**
 * AbstractFailureDetector serves as a building block for FailureDetector
 * implementations.
 * 
 */

public abstract class AbstractFailureDetector implements FailureDetector {

    protected final FailureDetectorConfig failureDetectorConfig;

    // Ugh. There's no ConcurrentHashSet and every implementation out there is
    // simply a wrapper around a ConcurrentHashMap anyway :(
    protected final ConcurrentHashMap<FailureDetectorListener, Object> listeners;

    protected final Map<Node, NodeStatus> nodeStatusMap;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected AbstractFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        if(failureDetectorConfig == null)
            throw new IllegalArgumentException("FailureDetectorConfig must be non-null");

        this.failureDetectorConfig = failureDetectorConfig;
        listeners = new ConcurrentHashMap<FailureDetectorListener, Object>();
        nodeStatusMap = new ConcurrentHashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes()) {
            nodeStatusMap.put(node, createNodeStatus(failureDetectorConfig.getTime()
                                                                          .getMilliseconds()));
        }
    }

    private NodeStatus createNodeStatus(long currTime) {
        NodeStatus nodeStatus = new NodeStatus();
        nodeStatus.setLastChecked(currTime);
        nodeStatus.setStartMillis(currTime);
        nodeStatus.setAvailable(true);
        return nodeStatus;
    }

    public void addFailureDetectorListener(FailureDetectorListener failureDetectorListener) {
        if(failureDetectorListener == null)
            throw new IllegalArgumentException("FailureDetectorListener must be non-null");

        listeners.put(failureDetectorListener, failureDetectorListener);
    }

    public void removeFailureDetectorListener(FailureDetectorListener failureDetectorListener) {
        if(failureDetectorListener == null)
            throw new IllegalArgumentException("FailureDetectorListener must be non-null");

        listeners.remove(failureDetectorListener);
    }

    public FailureDetectorConfig getConfig() {
        return failureDetectorConfig;
    }

    @JmxGetter(name = "availableNodes", description = "The available nodes")
    public String getAvailableNodes() {
        List<String> list = new ArrayList<String>();

        for(Node node: getConfig().getNodes())
            if(isAvailable(node))
                list.add(String.valueOf(node.getId()));

        return StringUtils.join(list, ",");
    }

    @JmxGetter(name = "unavailableNodes", description = "The unavailable nodes")
    public String getUnavailableNodes() {
        List<String> list = new ArrayList<String>();

        for(Node node: getConfig().getNodes())
            if(!isAvailable(node))
                list.add(String.valueOf(node.getId()));

        return StringUtils.join(list, ",");
    }

    @JmxGetter(name = "availableNodeCount", description = "The number of available nodes")
    public int getAvailableNodeCount() {
        int available = 0;

        for(Node node: getConfig().getNodes())
            if(isAvailable(node))
                available++;

        return available;
    }

    @JmxGetter(name = "nodeCount", description = "The number of total nodes")
    public int getNodeCount() {
        return getConfig().getNodes().size();
    }

    public void waitForAvailability(Node node) throws InterruptedException {
        checkNodeArg(node);
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            while(!isAvailable(node))
                nodeStatus.wait();
        }
    }

    public long getLastChecked(Node node) {
        checkNodeArg(node);
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            return nodeStatus.getLastChecked();
        }
    }

    public void destroy() {}

    protected void setAvailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        if(logger.isTraceEnabled())
            logger.trace(node + " set as available");

        // We need to distinguish the case where we're newly available and the
        // case where we're getting redundant availability notices. So let's
        // check the node status before we update it.
        boolean previouslyAvailable = setAvailable(nodeStatus, true);

        // If we were not previously available, we've just switched state,
        // so notify any listeners.
        if(!previouslyAvailable) {
            if(logger.isInfoEnabled())
                logger.info(node + " now available");

            synchronized(nodeStatus) {
                nodeStatus.notifyAll();
            }

            for(FailureDetectorListener fdl: listeners.keySet()) {
                try {
                    fdl.nodeAvailable(node);
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }
            }
        }
    }

    protected void setUnavailable(Node node, UnreachableStoreException e) {
        NodeStatus nodeStatus = getNodeStatus(node);

        if(logger.isEnabledFor(Level.WARN)) {
            if(e != null)
                logger.warn(node + " set as unavailable", e);
            else
                logger.warn(node + " set as unavailable");
        }

        // We need to distinguish the case where we're newly unavailable and the
        // case where we're getting redundant failure notices. So let's check
        // the node status before we update it.
        boolean previouslyAvailable = setAvailable(nodeStatus, false);

        // If we were previously available, we've just switched state from
        // available to unavailable, so notify any listeners.
        if(previouslyAvailable) {
            if(logger.isInfoEnabled())
                logger.info(node + " now unavailable");

            for(FailureDetectorListener fdl: listeners.keySet()) {
                try {
                    fdl.nodeUnavailable(node);
                } catch(Exception ex) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(ex, ex);
                }
            }
        }
    }

    protected NodeStatus getNodeStatus(Node node) {
        NodeStatus nodeStatus = nodeStatusMap.get(node);

        if(nodeStatus == null) {
            logger.warn("creating new node status for node " + node + " for failure detector.");
            nodeStatus = createNodeStatus(failureDetectorConfig.getTime().getMilliseconds());
            nodeStatusMap.put(node, nodeStatus);
        }

        return nodeStatus;
    }

    protected void checkNodeArg(Node node) {
        if(node == null)
            throw new IllegalArgumentException("node must be non-null");
    }

    protected void checkArgs(Node node, long requestTime) {
        checkNodeArg(node);

        if(requestTime < 0)
            throw new IllegalArgumentException("requestTime - " + requestTime + " - less than 0");
    }

    /**
     * We need to distinguish the case where we're newly available and the case
     * where we're already available. So we check the node status before we
     * update it and return it to the caller.
     * 
     * @param isAvailable True to set to available, false to make unavailable
     * 
     * @return Previous value of isAvailable
     */

    private boolean setAvailable(NodeStatus nodeStatus, boolean isAvailable) {
        synchronized(nodeStatus) {
            boolean previous = nodeStatus.isAvailable();

            nodeStatus.setAvailable(isAvailable);
            nodeStatus.setLastChecked(getConfig().getTime().getMilliseconds());

            return previous;
        }
    }

}

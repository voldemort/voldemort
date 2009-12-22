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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

/**
 * AbstractFailureDetector serves as a building block for FailureDetector
 * implementations.
 * 
 * @author Kirk True
 */

public abstract class AbstractFailureDetector implements FailureDetector {

    protected final FailureDetectorConfig failureDetectorConfig;

    protected final Set<FailureDetectorListener> listeners;

    protected final Map<Node, NodeStatus> nodeStatusMap;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected AbstractFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        this.failureDetectorConfig = failureDetectorConfig;
        listeners = Collections.synchronizedSet(new HashSet<FailureDetectorListener>());

        nodeStatusMap = new ConcurrentHashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeStatusMap.put(node, new NodeStatus(failureDetectorConfig.getTime()));
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
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            if(!isAvailable(node))
                nodeStatus.wait();
        }
    }

    public long getLastChecked(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            return nodeStatus.getLastChecked();
        }
    }

    public void destroy() {}

    protected void setAvailable(Node node) {
        if(logger.isTraceEnabled())
            logger.trace(node + " set as available");

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // We need to distinguish the case where we're newly available and
            // the case where we're getting redundant availability notices. So
            // let's check the node status before we update it.
            boolean previouslyAvailable = nodeStatus.isAvailable();

            // Update our state to be available.
            nodeStatus.setAvailable(true);

            // If we were not previously available, we've just switched state,
            // so notify any listeners.
            if(!previouslyAvailable)
                notifyAvailable(node);
        }
    }

    protected void setUnavailable(Node node, UnreachableStoreException e) {
        if(logger.isEnabledFor(Level.WARN)) {
            if(e != null)
                logger.warn(node + " set as unavailable", e);
            else
                logger.warn(node + " set as unavailable");
        }

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // We need to distinguish the case where we're newly unavailable and
            // the case where we're getting redundant failure notices. So let's
            // check the node status before we update it.
            boolean previouslyAvailable = nodeStatus.isAvailable();

            // Update our state to be unavailable.
            nodeStatus.setAvailable(false);

            // If we were previously available, we've just switched state from
            // available to unavailable, so notify any listeners.
            if(previouslyAvailable)
                notifyUnavailable(node);
        }
    }

    protected void notifyAvailable(Node node) {
        if(logger.isInfoEnabled())
            logger.info(node + " now available");

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            nodeStatus.notifyAll();
        }

        Set<FailureDetectorListener> listenersCopy = new HashSet<FailureDetectorListener>(listeners);

        for(FailureDetectorListener fdl: listenersCopy) {
            try {
                fdl.nodeAvailable(node);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }
    }

    protected void notifyUnavailable(Node node) {
        if(logger.isInfoEnabled())
            logger.info(node + " now unavailable");

        Set<FailureDetectorListener> listenersCopy = new HashSet<FailureDetectorListener>(listeners);

        for(FailureDetectorListener fdl: listenersCopy) {
            try {
                fdl.nodeUnavailable(node);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }
    }

    protected NodeStatus getNodeStatus(Node node) {
        NodeStatus nodeStatus = nodeStatusMap.get(node);

        if(nodeStatus == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");

        return nodeStatus;
    }

}

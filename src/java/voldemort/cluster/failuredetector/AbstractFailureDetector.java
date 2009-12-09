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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

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
        this(failureDetectorConfig, NodeStatus.class);
    }

    protected AbstractFailureDetector(FailureDetectorConfig failureDetectorConfig,
                                      Class<? extends NodeStatus> nodeStatusClass) {
        this.failureDetectorConfig = failureDetectorConfig;
        listeners = Collections.synchronizedSet(new HashSet<FailureDetectorListener>());

        nodeStatusMap = new ConcurrentHashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes()) {
            NodeStatus nodeStatus = null;

            try {
                Constructor<? extends NodeStatus> nodeStatusCtor = nodeStatusClass.getConstructor(Time.class);
                nodeStatus = nodeStatusCtor.newInstance(failureDetectorConfig.getTime());
            } catch(Exception e) {
                throw new VoldemortException(e);
            }

            nodeStatusMap.put(node, nodeStatus);
        }
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
            return nodeStatus.getLastCheckedMs();
        }
    }

    public boolean isAvailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // The node can be available in one of two ways: a) it's actually
            // available, or b) it was unavailable but our "bannage" period has
            // expired so we're free to consider it as available.
            boolean isAvailable = !nodeStatus.isUnavailable(failureDetectorConfig.getNodeBannagePeriod());

            // If we're now considered available but our actual status is *not*
            // available, this means we've become available via a timeout. We
            // act like we're fully available in that we send out the
            // availability event.
            if(isAvailable && !nodeStatus.isAvailable())
                setAvailable(node);

            return isAvailable;
        }
    }

    protected void setAvailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // We need to distinguish the case where we're newly available and
            // the case where we're getting redundant availability notices. So
            // let's check the node status before we update it.
            boolean previouslyAvailable = nodeStatus.isAvailable();

            // Update our state to be available.
            nodeStatus.setAvailable();

            // If we were not previously available, we've just switched state,
            // so notify any listeners.
            if(!previouslyAvailable)
                notifyAvailable(node);
        }
    }

    protected void setUnavailable(Node node, UnreachableStoreException e) {
        if(e != null) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // The node can be available in one of two ways: a) it's actually
            // available, or b) it was unavailable but our "bannage" period has
            // expired so we're free to consider it as available.
            boolean previouslyAvailable = !nodeStatus.isUnavailable(failureDetectorConfig.getNodeBannagePeriod());

            // Update our state to be unavailable.
            nodeStatus.setUnavailable();

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

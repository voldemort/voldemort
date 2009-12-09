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

/**
 * AbstractFailureDetector serves as a building block for FailureDetector
 * implementations.
 * 
 * @author Kirk True
 */

public abstract class AbstractFailureDetector implements FailureDetector {

    protected final FailureDetectorConfig failureDetectorConfig;

    private final Set<FailureDetectorListener> listeners;

    private final Map<Node, Object> nodeLockMap;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected AbstractFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        this.failureDetectorConfig = failureDetectorConfig;
        listeners = Collections.synchronizedSet(new HashSet<FailureDetectorListener>());

        nodeLockMap = new ConcurrentHashMap<Node, Object>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeLockMap.put(node, new Object());
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
        Object nodeLock = getNodeLock(node);

        synchronized(nodeLock) {
            if(!isAvailable(node))
                nodeLock.wait();
        }
    }

    protected void notifyAvailable(Node node) {
        if(logger.isInfoEnabled())
            logger.info(node + " now available");

        Object nodeLock = getNodeLock(node);

        synchronized(nodeLock) {
            nodeLock.notifyAll();
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

    private Object getNodeLock(Node node) {
        Object nodeLock = nodeLockMap.get(node);

        if(nodeLock == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");
        return nodeLock;
    }

}

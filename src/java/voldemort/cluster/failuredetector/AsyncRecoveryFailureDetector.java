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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;

/**
 * AsyncRecoveryFailureDetector detects failures and then attempts to contact
 * the failing node's Store to determine availability.
 * 
 * <p/>
 * 
 * When a node does go down, attempts to access the remote Store for that node
 * may take several seconds. Rather than cause the thread to block, we perform
 * this check in a background thread.
 * 
 * @author Kirk True
 */

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class AsyncRecoveryFailureDetector extends AbstractFailureDetector implements Runnable {

    /**
     * A set of nodes that have been marked as unavailable. As nodes go offline
     * and callers report such via recordException, they are added to this set.
     * When a node becomes available via the check in the thread, they are
     * removed.
     */

    private final Set<Node> unavailableNodes;

    /**
     * Thread that checks availability of the nodes in the unavailableNodes set.
     */

    private final Thread recoveryThread;

    private volatile boolean isRunning;

    public AsyncRecoveryFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);

        unavailableNodes = new HashSet<Node>();

        isRunning = true;

        recoveryThread = new Thread(this, "AsyncRecoveryFailureDetector");
        recoveryThread.setDaemon(true);
        recoveryThread.start();

        Map<Node, NodeStatus> nodeStatusMap = new HashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeStatusMap.put(node, new NodeStatus(failureDetectorConfig.getTime()));
    }

    public boolean isAvailable(Node node) {
        // We override the default behavior and keep track of the unavailable
        // nodes in our set.
        synchronized(unavailableNodes) {
            return !unavailableNodes.contains(node);
        }
    }

    public void recordException(Node node, UnreachableStoreException e) {
        synchronized(unavailableNodes) {
            unavailableNodes.add(node);
        }

        setUnavailable(node, e);
    }

    public void recordSuccess(Node node) {
    // Do nothing. Nodes only become available in our thread...
    }

    @Override
    public void destroy() {
        isRunning = false;
    }

    private void check() {
        Set<Node> unavailableNodesCopy = null;

        synchronized(this) {
            unavailableNodesCopy = new HashSet<Node>(unavailableNodes);
        }

        ByteArray key = new ByteArray((byte) 1);

        for(Node node: unavailableNodesCopy) {
            if(logger.isDebugEnabled())
                logger.debug("Checking previously unavailable node " + node);

            Store<ByteArray, byte[]> store = getConfig().getStoreResolver().getStore(node);

            if(store == null) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(node + " store is null; cannot determine node availability");

                continue;
            }

            try {
                store.get(key);

                synchronized(unavailableNodes) {
                    unavailableNodes.remove(node);
                }

                setAvailable(node);
            } catch(UnreachableStoreException e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(node + " still unavailable");
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(node + " unavailable due to error", e);
            }
        }
    }

    public void run() {
        long asyncRecoveryInterval = getConfig().getAsyncRecoveryInterval();

        while(!Thread.currentThread().isInterrupted() && isRunning) {
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("Sleeping for " + asyncRecoveryInterval
                                 + " ms before checking node availability");
                }

                getConfig().getTime().sleep(asyncRecoveryInterval);
            } catch(InterruptedException e) {
                break;
            }

            check();
        }
    }

}

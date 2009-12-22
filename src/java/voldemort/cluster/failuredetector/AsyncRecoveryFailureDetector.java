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

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

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

        recoveryThread = new Thread(this, "AsyncNodeRecoverer");
        recoveryThread.setDaemon(true);
        recoveryThread.start();
    }

    public boolean isAvailable(Node node) {
        // This is a sanity check that will throw an error if the node is
        // invalid.
        getNodeStatus(node);

        // We override the default behavior and keep track of the unavailable
        // nodes in our set.
        synchronized(unavailableNodes) {
            return !unavailableNodes.contains(node);
        }
    }

    public void recordException(Node node, UnreachableStoreException e) {
        setUnavailable(node, e);
    }

    public void recordSuccess(Node node) {
    // Do nothing. Nodes only become available in our thread...
    }

    @Override
    protected void setUnavailable(Node node, UnreachableStoreException e) {
        synchronized(unavailableNodes) {
            unavailableNodes.add(node);
        }

        super.setUnavailable(node, e);
    }

    @Override
    public void destroy() {
        isRunning = false;
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

            Set<Node> unavailableNodesCopy = null;

            synchronized(this) {
                unavailableNodesCopy = new HashSet<Node>(unavailableNodes);
            }

            for(Node node: unavailableNodesCopy) {
                if(logger.isDebugEnabled())
                    logger.debug("Checking previously unavailable node " + node);

                StoreVerifier storeVerifier = getConfig().getStoreVerifier();

                if(storeVerifier == null) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(node
                                    + " store verifier is null; cannot determine node availability");

                    continue;
                }

                try {
                    // This is our test.
                    storeVerifier.verifyStore(node);

                    synchronized(unavailableNodes) {
                        unavailableNodes.remove(node);
                    }

                    nodeRecovered(node);
                } catch(UnreachableStoreException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(node + " still unavailable", e);
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(node + " unavailable due to error", e);
                }
            }
        }
    }

    protected void nodeRecovered(Node node) {
        setAvailable(node);
    }

}

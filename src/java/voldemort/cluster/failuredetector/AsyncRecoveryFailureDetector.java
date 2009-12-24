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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;

import voldemort.annotations.jmx.JmxGetter;
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
     * A map of nodes that have been marked as unavailable. As nodes go offline
     * and callers report such via recordException, they are added here. When a
     * node becomes available via the check in the thread, they are removed. The
     * Integer is the number of times we've tried to access it since going
     * offline.
     */

    private final ConcurrentHashMap<Node, Integer> unavailableNodes;

    /**
     * Thread that checks availability of the nodes in the unavailableNodes set.
     */

    private final Thread recoveryThread;

    private volatile boolean isRunning;

    public AsyncRecoveryFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);

        unavailableNodes = new ConcurrentHashMap<Node, Integer>();

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
        return !unavailableNodes.containsKey(node);
    }

    public void recordException(Node node, UnreachableStoreException e) {
        setUnavailable(node, e);
    }

    public void recordSuccess(Node node) {
    // Do nothing. Nodes only become available in our thread...
    }

    @JmxGetter(name = "nodeAttempts", description = "Each unavailable node is listed with the number of attempts to contact since becoming unavailable")
    public String getNodeAttempts() {
        List<String> list = new ArrayList<String>();

        for(Map.Entry<Node, Integer> entry: unavailableNodes.entrySet()) {
            Node node = entry.getKey();
            list.add(node + "=" + entry.getValue());
        }

        return StringUtils.join(list, ",");
    }

    @Override
    protected void setUnavailable(Node node, UnreachableStoreException e) {
        unavailableNodes.putIfAbsent(node, 0);

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

            for(Map.Entry<Node, Integer> entry: unavailableNodes.entrySet()) {
                Node node = entry.getKey();

                if(logger.isDebugEnabled())
                    logger.debug("Checking previously unavailable node " + node);

                StoreVerifier storeVerifier = getConfig().getStoreVerifier();

                try {
                    // This is our test.
                    if(logger.isDebugEnabled())
                        logger.debug("Verifying previously unavailable node " + node);

                    storeVerifier.verifyStore(node);

                    if(logger.isDebugEnabled())
                        logger.debug("Verified previously unavailable node " + node
                                     + ", will mark as available...");

                    unavailableNodes.remove(node);

                    nodeRecovered(node);
                } catch(UnreachableStoreException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(node + " still unavailable", e);

                    unavailableNodes.put(node, entry.getValue() + 1);

                    setUnavailable(node, e);
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(node + " unavailable due to error", e);

                    unavailableNodes.put(node, entry.getValue() + 1);

                    setUnavailable(node, new UnreachableStoreException(e.getMessage()));
                }
            }
        }
    }

    protected void nodeRecovered(Node node) {
        setAvailable(node);
    }

}

/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2012 LinkedIn, Inc.
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

import java.lang.Thread.UncaughtExceptionHandler;

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
 */

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
public class AsyncRecoveryFailureDetector extends AbstractFailureDetector implements Runnable {

    private volatile boolean isRunning;

    public AsyncRecoveryFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);

        isRunning = true;

        Thread recoveryThread = new Thread(this, "AsyncNodeRecoverer");
        recoveryThread.setDaemon(true);
        recoveryThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread t, Throwable e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error("Uncaught exception in failure detector recovery thread:", e);
            }
        });

        recoveryThread.start();
    }

    public boolean isAvailable(Node node) {
        checkNodeArg(node);
        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            return nodeStatus.isAvailable();
        }
    }

    public void recordException(Node node, long requestTime, UnreachableStoreException e) {
        checkArgs(node, requestTime);
        setUnavailable(node, e);
    }

    public void recordSuccess(Node node, long requestTime) {
        // Nodes only become available in our thread, but let's sanity-check our
        // arguments...
        checkArgs(node, requestTime);
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

            for(Node node: getConfig().getCluster().getNodes()) {
                if(isAvailable(node))
                    continue;

                if(logger.isDebugEnabled())
                    logger.debug("Checking previously unavailable node " + node.getId());

                StoreVerifier storeVerifier = getConfig().getStoreVerifier();

                try {
                    // This is our test.
                    if(logger.isDebugEnabled())
                        logger.debug("Verifying previously unavailable node " + node.getId());

                    storeVerifier.verifyStore(node);

                    if(logger.isDebugEnabled())
                        logger.debug("Verified previously unavailable node " + node.getId()
                                     + "is now available.");

                    nodeRecovered(node);
                } catch(UnreachableStoreException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Node " + node.getId() + " still unavailable.");
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error("Node " + node.getId() + " unavailable due to error", e);
                }
            }
        }
    }

    protected void nodeRecovered(Node node) {
        setAvailable(node);
    }

}

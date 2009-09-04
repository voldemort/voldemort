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

import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;

public class AsyncRecoveryFailureDetector extends AbstractFailureDetector implements Runnable {

    private final Set<Node> unavailableNodes;

    public AsyncRecoveryFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
        unavailableNodes = new HashSet<Node>();

        Thread t = new Thread(this);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public boolean isAvailable(Node node) {
        synchronized(unavailableNodes) {
            return !unavailableNodes.contains(node);
        }
    }

    public void recordException(Node node, Exception e) {
        synchronized(unavailableNodes) {
            unavailableNodes.add(node);
        }

        setUnavailable(node);

        if(logger.isInfoEnabled())
            logger.info(node + " now unavailable");
    }

    public void recordSuccess(Node node) {
        boolean wasRemoved = false;

        synchronized(unavailableNodes) {
            wasRemoved = unavailableNodes.remove(node);
        }

        setAvailable(node);

        if(wasRemoved) {
            if(logger.isInfoEnabled())
                logger.info(node + " now available");
        }
    }

    public void run() {
        ByteArray key = new ByteArray((byte) 1);

        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("Sleeping for " + failureDetectorConfig.getNodeBannagePeriod());

            try {
                Thread.sleep(failureDetectorConfig.getNodeBannagePeriod());
            } catch(InterruptedException e) {
                break;
            }

            Set<Node> unavailableNodesCopy = new HashSet<Node>();

            synchronized(unavailableNodes) {
                unavailableNodesCopy.addAll(unavailableNodes);
            }

            for(Node node: unavailableNodesCopy) {
                if(logger.isInfoEnabled())
                    logger.info("Checking previously unavailable node " + node);

                Store<ByteArray, byte[]> store = failureDetectorConfig.getStore(node);

                if(store == null) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(node + " store is null; cannot determine node availability");

                    continue;
                }

                try {
                    store.get(key);

                    recordSuccess(node);

                    if(logger.isInfoEnabled())
                        logger.info(node + " now available");
                } catch(UnreachableStoreException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(node + " still unavailable");
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(node + " unavailable due to error", e);
                }
            }
        }
    }

}

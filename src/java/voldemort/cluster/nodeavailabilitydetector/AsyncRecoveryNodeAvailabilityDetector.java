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

package voldemort.cluster.nodeavailabilitydetector;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;

public class AsyncRecoveryNodeAvailabilityDetector extends AbstractNodeAvailabilityDetector
        implements Runnable {

    private final Set<Node> unavailableNodes;

    public AsyncRecoveryNodeAvailabilityDetector() {
        unavailableNodes = new HashSet<Node>();

        Thread t = new Thread(this);
        t.setDaemon(true);
        t.start();
    }

    public boolean isAvailable(Node node) {
        synchronized(unavailableNodes) {
            return !unavailableNodes.contains(node);
        }
    }

    public void recordException(Node node, Exception e) {
        synchronized(unavailableNodes) {
            unavailableNodes.add(node);
        }

        getNodeStatus(node).setUnavailable();

        if(logger.isInfoEnabled())
            logger.info(node + " now unavailable");
    }

    public void recordSuccess(Node node) {
        synchronized(unavailableNodes) {
            unavailableNodes.remove(node);
        }

        getNodeStatus(node).setAvailable();
    }

    public void run() {
        ByteArray key = new ByteArray((byte) 1);

        while(!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(nodeBannagePeriod);
            } catch(InterruptedException e) {
                break;
            }

            Set<Node> unavailableNodesCopy = new HashSet<Node>();

            synchronized(unavailableNodes) {
                unavailableNodesCopy.addAll(unavailableNodes);
            }

            if(getStores() == null) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("Stores not yet set; cannot determine node availability");

                continue;
            }

            for(Node node: unavailableNodesCopy) {
                if(logger.isDebugEnabled())
                    logger.debug("Checking previously unavailable node " + node);

                Store<ByteArray, byte[]> store = stores.get(node.getId());

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

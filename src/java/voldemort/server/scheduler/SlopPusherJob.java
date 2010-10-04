/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.server.scheduler;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A task which goes through the slop table and attempts to push out all the
 * slop to its rightful owner node
 * 
 * 
 */
public class SlopPusherJob implements Runnable {

    private static final Logger logger = Logger.getLogger(SlopPusherJob.class.getName());

    private final StoreRepository storeRepo;
    private final Cluster cluster;
    private final FailureDetector failureDetector;
    private final long maxWriteBytesPerSec;

    public SlopPusherJob(StoreRepository storeRepo,
                         Cluster cluster,
                         FailureDetector failureDetector,
                         long maxWriteBytesPerSec) {
        this.storeRepo = storeRepo;
        this.cluster = cluster;
        this.failureDetector = failureDetector;
        this.maxWriteBytesPerSec = maxWriteBytesPerSec;
    }

    /**
     * Loop over entries in the slop table and attempt to push them to the
     * deserving server
     */
    public void run() {
        logger.debug("Pushing slop...");
        int slopsPushed = 0;
        int attemptedPushes = 0;
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;
        try {
            StorageEngine<ByteArray, Slop> slopStore = storeRepo.getSlopStore();
            EventThrottler throttler = new EventThrottler(maxWriteBytesPerSec);
            iterator = slopStore.entries();
            while(iterator.hasNext()) {
                if(Thread.interrupted())
                    throw new InterruptedException("Task cancelled!");
                attemptedPushes++;

                if(attemptedPushes % 1000 == 0)
                    logger.info("Attempted pushing " + attemptedPushes + " slops");

                try {
                    Pair<ByteArray, Versioned<Slop>> keyAndVal = iterator.next();
                    Versioned<Slop> versioned = keyAndVal.getSecond();
                    Slop slop = versioned.getValue();
                    Node node = cluster.getNodeById(slop.getNodeId());
                    if(failureDetector.isAvailable(node)) {
                        Store<ByteArray, byte[]> store = storeRepo.getNodeStore(slop.getStoreName(),
                                                                                node.getId());
                        Long startNs = System.nanoTime();
                        try {
                            int nBytes = slop.getKey().length();
                            if(slop.getOperation() == Operation.PUT) {
                                store.put(slop.getKey(),
                                          new Versioned<byte[]>(slop.getValue(), versioned.getVersion()));
                                nBytes += slop.getValue().length +
                                          ((VectorClock) versioned.getVersion()).sizeInBytes() + 1;

                            }
                            else if(slop.getOperation() == Operation.DELETE)
                                store.delete(slop.getKey(), versioned.getVersion());
                            else
                                logger.error("Unknown slop operation: " + slop.getOperation());
                            failureDetector.recordSuccess(node, deltaMs(startNs));
                            slopStore.delete(slop.makeKey(), versioned.getVersion());
                            slopsPushed++;

                            throttler.maybeThrottle(nBytes);
                        } catch(ObsoleteVersionException e) {
                            // okay it is old, just delete it
                            slopStore.delete(slop.makeKey(), versioned.getVersion());
                        } catch(UnreachableStoreException e) {
                            failureDetector.recordException(node, deltaMs(startNs), e);
                        }
                    }
                } catch(Exception e) {
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error(e);
        } finally {
            try {
                if(iterator != null)
                    iterator.close();
            } catch(Exception e) {
                logger.error("Failed to close iterator.", e);
            }
        }

        // typically not useful to hear that 0 items were attempted so log as
        // debug
        logger.log(attemptedPushes > 0 ? Level.INFO : Level.DEBUG,
                   "Attempted " + attemptedPushes + " hinted handoff pushes of which "
                           + slopsPushed + " succeeded.");
    }

    private long deltaMs(Long startNs) {
        return (System.nanoTime() - startNs) / Time.NS_PER_MS;
    }

}

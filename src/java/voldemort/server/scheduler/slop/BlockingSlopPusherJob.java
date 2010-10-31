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

package voldemort.server.scheduler.slop;

import java.util.Date;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
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
public class BlockingSlopPusherJob implements Runnable {

    private static final Logger logger = Logger.getLogger(BlockingSlopPusherJob.class.getName());
    public final static String TYPE_NAME = "blocking";

    private final static Object lock = new Object();

    private final StoreRepository storeRepo;
    private final MetadataStore metadataStore;
    private final FailureDetector failureDetector;
    private final long maxWriteBytesPerSec;

    public BlockingSlopPusherJob(StoreRepository storeRepo,
                                 MetadataStore metadataStore,
                                 FailureDetector failureDetector,
                                 long maxWriteBytesPerSec) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.failureDetector = failureDetector;
        this.maxWriteBytesPerSec = maxWriteBytesPerSec;
    }

    /**
     * Loop over entries in the slop table and attempt to push them to the
     * deserving server
     */
    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            logger.error("Cannot run slop pusher job since cluster is rebalancing");
            return;
        }

        // Allow only one job to run at one time - Two jobs may get started if
        // someone disables and enables (through JMX) immediately during a run
        synchronized(lock) {

            logger.info("Started slop pusher job at " + new Date());
            Cluster cluster = metadataStore.getCluster();
            ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;

            SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
            try {
                StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
                EventThrottler throttler = new EventThrottler(maxWriteBytesPerSec);

                iterator = slopStore.entries();

                while(iterator.hasNext()) {
                    if(Thread.interrupted())
                        throw new InterruptedException("Slop pusher job cancelled");
                    try {
                        Pair<ByteArray, Versioned<Slop>> keyAndVal;
                        try {
                            keyAndVal = iterator.next();
                        } catch(Exception e) {
                            logger.error("Exception in iterator, escaping the loop ", e);
                            break;
                        }

                        Versioned<Slop> versioned = keyAndVal.getSecond();
                        Slop slop = versioned.getValue();
                        int nodeId = slop.getNodeId();
                        Node node = cluster.getNodeById(nodeId);

                        if(failureDetector.isAvailable(node)) {
                            Store<ByteArray, byte[], byte[]> store = storeRepo.getNodeStore(slop.getStoreName(),
                                                                                            node.getId());
                            Long startNs = System.nanoTime();
                            int nBytes = 0;
                            try {
                                nBytes = slop.getKey().length();
                                if(slop.getOperation() == Operation.PUT) {
                                    store.put(slop.getKey(),
                                              new Versioned<byte[]>(slop.getValue(),
                                                                    versioned.getVersion()),
                                              slop.getTransforms());
                                    nBytes += slop.getValue().length
                                              + ((VectorClock) versioned.getVersion()).sizeInBytes()
                                              + 1;

                                } else if(slop.getOperation() == Operation.DELETE) {
                                    nBytes += ((VectorClock) versioned.getVersion()).sizeInBytes() + 1;
                                    store.delete(slop.getKey(), versioned.getVersion());
                                } else {
                                    logger.error("Unknown slop operation: " + slop.getOperation());
                                    continue;
                                }
                                failureDetector.recordSuccess(node, deltaMs(startNs));
                                slopStore.delete(slop.makeKey(), versioned.getVersion());
                                // Throttle the bytes...
                                throttler.maybeThrottle(nBytes);
                            } catch(ObsoleteVersionException e) {

                                // Throttle the bytes...
                                throttler.maybeThrottle(nBytes);

                            } catch(UnreachableStoreException e) {
                                failureDetector.recordException(node, deltaMs(startNs), e);
                            }
                        }
                    } catch(Exception e) {
                        logger.error(e, e);
                    }
                }
            } catch(Exception e) {
                logger.error(e, e);
            } finally {
                try {
                    if(iterator != null)
                        iterator.close();
                } catch(Exception e) {
                    logger.error("Failed to close iterator.", e);
                }
            }
        }
    }

    private long deltaMs(Long startNs) {
        return (System.nanoTime() - startNs) / Time.NS_PER_MS;
    }

}

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
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

/**
 * A task which goes through the slop table and attempts to push out all the
 * slop to its rightful owner node
 * 
 * 
 */
public class BlockingSlopPusherJob implements Runnable {

    private static final Logger logger = Logger.getLogger(BlockingSlopPusherJob.class.getName());
    public final static String TYPE_NAME = "blocking";

    private final StoreRepository storeRepo;
    private final MetadataStore metadataStore;
    private final FailureDetector failureDetector;
    private final long maxWriteBytesPerSec;
    private final ScanPermitWrapper repairPermits;

    public BlockingSlopPusherJob(StoreRepository storeRepo,
                                 MetadataStore metadataStore,
                                 FailureDetector failureDetector,
                                 VoldemortConfig voldemortConfig,
                                 ScanPermitWrapper repairPermits) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.repairPermits = Utils.notNull(repairPermits);
        this.failureDetector = failureDetector;
        this.maxWriteBytesPerSec = voldemortConfig.getSlopMaxWriteBytesPerSec();
    }

    /**
     * Loop over entries in the slop table and attempt to push them to the
     * deserving server
     */
    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(metadataStore.getServerState()
                        .equals(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)) {
            logger.error("Cannot run slop pusher job since Voldemort server is rebalancing");
            return;
        }

        logger.info("Started blocking slop pusher job at " + new Date());

        Cluster cluster = metadataStore.getCluster();
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;

        Map<Integer, Long> attemptedByNode = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        Map<Integer, Long> succeededByNode = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        long slopsPushed = 0L;
        long attemptedPushes = 0L;
        for(Node node: cluster.getNodes()) {
            attemptedByNode.put(node.getId(), 0L);
            succeededByNode.put(node.getId(), 0L);
        }

        acquireRepairPermit();
        try {
            SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
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

                    attemptedPushes++;
                    if(attemptedPushes % 10000 == 0)
                        logger.info("Attempted pushing " + attemptedPushes + " slops");
                    Long attempted = attemptedByNode.get(nodeId);
                    attemptedByNode.put(nodeId, attempted + 1L);

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

                            slopsPushed++;
                            // Increment succeeded
                            Long succeeded = succeededByNode.get(nodeId);
                            succeededByNode.put(nodeId, succeeded + 1L);

                            // Throttle the bytes...
                            throttler.maybeThrottle(nBytes);

                        } catch(ObsoleteVersionException e) {

                            // okay it is old, just delete it
                            slopStore.delete(slop.makeKey(), versioned.getVersion());
                            slopsPushed++;

                            // Increment succeeded
                            Long succeeded = succeededByNode.get(nodeId);
                            succeededByNode.put(nodeId, succeeded + 1L);

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

            // Only if we reached here do we update stats
            logger.log(attemptedPushes > 0 ? Level.INFO : Level.DEBUG,
                       "Attempted " + attemptedPushes + " hinted handoff pushes of which "
                               + slopsPushed + " succeeded.");

            Map<Integer, Long> outstanding = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
            for(int nodeId: succeededByNode.keySet()) {
                outstanding.put(nodeId, attemptedByNode.get(nodeId) - succeededByNode.get(nodeId));
            }

            slopStorageEngine.resetStats(outstanding);

        } catch(Exception e) {
            logger.error(e, e);
        } finally {
            try {
                if(iterator != null)
                    iterator.close();
            } catch(Exception e) {
                logger.error("Failed to close iterator.", e);
            }
            this.repairPermits.release();
        }
    }

    private void acquireRepairPermit() {
        logger.info("Acquiring lock to perform blocking slop pusher job ");
        try {
            this.repairPermits.acquire(null);
            logger.info("Acquired lock to perform blocking slop pusher job ");
        } catch(InterruptedException e) {
            throw new IllegalStateException("Blocking slop pusher job interrupted while waiting for permit.",
                                            e);
        }
    }

    private long deltaMs(Long startNs) {
        return (System.nanoTime() - startNs) / Time.NS_PER_MS;
    }

}

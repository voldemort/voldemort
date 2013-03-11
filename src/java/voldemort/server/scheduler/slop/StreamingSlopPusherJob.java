/*
 * Copyright 2013 LinkedIn, Inc
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("unchecked")
public class StreamingSlopPusherJob implements Runnable {

    private final static Logger logger = Logger.getLogger(StreamingSlopPusherJob.class.getName());
    public final static String TYPE_NAME = "streaming";

    private final static Versioned<Slop> END = Versioned.value(null);

    private final MetadataStore metadataStore;
    private final StoreRepository storeRepo;
    private final FailureDetector failureDetector;
    private ConcurrentMap<Integer, SynchronousQueue<Versioned<Slop>>> slopQueues;
    private ExecutorService consumerExecutor;
    private final EventThrottler readThrottler;
    private AdminClient adminClient;
    private Cluster cluster;

    private final List<Future> consumerResults;
    private final VoldemortConfig voldemortConfig;
    private final Map<Integer, Set<Integer>> zoneMapping;
    private ConcurrentHashMap<Integer, Long> attemptedByNode;
    private ConcurrentHashMap<Integer, Long> succeededByNode;
    private final ScanPermitWrapper repairPermits;

    public StreamingSlopPusherJob(StoreRepository storeRepo,
                                  MetadataStore metadataStore,
                                  FailureDetector failureDetector,
                                  VoldemortConfig voldemortConfig,
                                  ScanPermitWrapper repairPermits) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.failureDetector = failureDetector;
        this.voldemortConfig = voldemortConfig;
        this.repairPermits = Utils.notNull(repairPermits);
        this.readThrottler = new EventThrottler(voldemortConfig.getSlopMaxReadBytesPerSec());
        this.adminClient = null;
        this.consumerResults = Lists.newArrayList();
        this.zoneMapping = Maps.newHashMap();
        this.consumerExecutor = Executors.newCachedThreadPool(new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("slop-pusher");
                return thread;
            }
        });
    }

    public void run() {
        // load the metadata before each run, in case the cluster is changed
        loadMetadata();

        // don't try to run slop pusher job when rebalancing
        if(metadataStore.getServerState()
                        .equals(MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER)) {
            logger.error("Cannot run slop pusher job since Voldemort server is rebalancing");
            return;
        }

        boolean terminatedEarly = false;
        Date startTime = new Date();
        logger.info("Started streaming slop pusher job at " + startTime);

        SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;

        if(adminClient == null) {
            adminClient = new AdminClient(cluster,
                                          new AdminClientConfig().setMaxConnectionsPerNode(1),
                                          new ClientConfig());
        }

        if(voldemortConfig.getSlopZonesDownToTerminate() > 0) {
            // Populating the zone mapping for early termination
            zoneMapping.clear();
            for(Node n: cluster.getNodes()) {
                if(failureDetector.isAvailable(n)) {
                    Set<Integer> nodes = zoneMapping.get(n.getZoneId());
                    if(nodes == null) {
                        nodes = Sets.newHashSet();
                        zoneMapping.put(n.getZoneId(), nodes);
                    }
                    nodes.add(n.getId());
                }
            }

            // Check how many zones are down
            int zonesDown = 0;
            for(Zone zone: cluster.getZones()) {
                if(zoneMapping.get(zone.getId()) == null
                   || zoneMapping.get(zone.getId()).size() == 0)
                    zonesDown++;
            }

            // Terminate early
            if(voldemortConfig.getSlopZonesDownToTerminate() <= zoneMapping.size()
               && zonesDown >= voldemortConfig.getSlopZonesDownToTerminate()) {
                logger.info("Completed streaming slop pusher job at " + startTime
                            + " early because " + zonesDown + " zones are down");
                stopAdminClient();
                return;
            }
        }

        // Clearing the statistics
        AtomicLong attemptedPushes = new AtomicLong(0);
        for(Node node: cluster.getNodes()) {
            attemptedByNode.put(node.getId(), 0L);
            succeededByNode.put(node.getId(), 0L);
        }

        acquireRepairPermit();
        try {
            StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
            iterator = slopStore.entries();

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<Slop>> keyAndVal;
                try {
                    keyAndVal = iterator.next();
                    Versioned<Slop> versioned = keyAndVal.getSecond();

                    // Retrieve the node
                    int nodeId = versioned.getValue().getNodeId();
                    Node node = cluster.getNodeById(nodeId);

                    attemptedPushes.incrementAndGet();
                    Long attempted = attemptedByNode.get(nodeId);
                    attemptedByNode.put(nodeId, attempted + 1L);
                    if(attemptedPushes.get() % 10000 == 0)
                        logger.info("Attempted pushing " + attemptedPushes + " slops");

                    if(logger.isTraceEnabled())
                        logger.trace("Pushing slop for " + versioned.getValue().getNodeId()
                                     + " and store  " + versioned.getValue().getStoreName());

                    if(failureDetector.isAvailable(node)) {
                        SynchronousQueue<Versioned<Slop>> slopQueue = slopQueues.get(nodeId);
                        if(slopQueue == null) {
                            // No previous slop queue, add one
                            slopQueue = new SynchronousQueue<Versioned<Slop>>();
                            slopQueues.put(nodeId, slopQueue);
                            consumerResults.add(consumerExecutor.submit(new SlopConsumer(nodeId,
                                                                                         slopQueue,
                                                                                         slopStorageEngine)));
                        }
                        boolean offered = slopQueue.offer(versioned,
                                                          voldemortConfig.getClientRoutingTimeoutMs(),
                                                          TimeUnit.MILLISECONDS);
                        if(!offered) {
                            if(logger.isDebugEnabled())
                                logger.debug("No consumer appeared for slop in "
                                             + voldemortConfig.getClientConnectionTimeoutMs()
                                             + " ms");
                        }
                        readThrottler.maybeThrottle(nBytesRead(keyAndVal));
                    } else {
                        logger.trace(node + " declared down, won't push slop");
                    }
                } catch(RejectedExecutionException e) {
                    throw new VoldemortException("Ran out of threads in executor", e);
                }
            }

        } catch(InterruptedException e) {
            logger.warn("Interrupted exception", e);
            terminatedEarly = true;
        } catch(Exception e) {
            logger.error(e, e);
            terminatedEarly = true;
        } finally {
            try {
                if(iterator != null)
                    iterator.close();
            } catch(Exception e) {
                logger.warn("Failed to close iterator cleanly as database might be closed", e);
            }

            // Adding the poison pill
            for(SynchronousQueue<Versioned<Slop>> slopQueue: slopQueues.values()) {
                try {
                    slopQueue.put(END);
                } catch(InterruptedException e) {
                    logger.warn("Error putting poison pill", e);
                }
            }

            for(Future result: consumerResults) {
                try {
                    result.get();
                } catch(Exception e) {
                    logger.warn("Exception in consumer", e);
                }
            }

            // Only if exception didn't take place do we update the counts
            if(!terminatedEarly) {
                Map<Integer, Long> outstanding = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
                for(int nodeId: succeededByNode.keySet()) {
                    logger.info("Slops to node " + nodeId + " - Succeeded - "
                                + succeededByNode.get(nodeId) + " - Attempted - "
                                + attemptedByNode.get(nodeId));
                    outstanding.put(nodeId,
                                    attemptedByNode.get(nodeId) - succeededByNode.get(nodeId));
                }
                slopStorageEngine.resetStats(outstanding);
                logger.info("Completed streaming slop pusher job which started at " + startTime);
            } else {
                for(int nodeId: succeededByNode.keySet()) {
                    logger.info("Slops to node " + nodeId + " - Succeeded - "
                                + succeededByNode.get(nodeId) + " - Attempted - "
                                + attemptedByNode.get(nodeId));
                }
                logger.info("Completed early streaming slop pusher job which started at "
                            + startTime);
            }

            // Shut down admin client as not to waste connections
            consumerResults.clear();
            slopQueues.clear();
            stopAdminClient();
            this.repairPermits.release();
        }

    }

    private void loadMetadata() {
        this.cluster = metadataStore.getCluster();
        this.slopQueues = new ConcurrentHashMap<Integer, SynchronousQueue<Versioned<Slop>>>(cluster.getNumberOfNodes());
        this.attemptedByNode = new ConcurrentHashMap<Integer, Long>(cluster.getNumberOfNodes());
        this.succeededByNode = new ConcurrentHashMap<Integer, Long>(cluster.getNumberOfNodes());
    }

    private void stopAdminClient() {
        if(adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
    }

    private int nBytesRead(Pair<ByteArray, Versioned<Slop>> keyAndVal) {
        return keyAndVal.getFirst().length() + slopSize(keyAndVal.getSecond());
    }

    /**
     * Returns the approximate size of slop to help in throttling
     * 
     * @param slopVersioned The versioned slop whose size we want
     * @return Size in bytes
     */
    private int slopSize(Versioned<Slop> slopVersioned) {
        int nBytes = 0;
        Slop slop = slopVersioned.getValue();
        nBytes += slop.getKey().length();
        nBytes += ((VectorClock) slopVersioned.getVersion()).sizeInBytes();
        switch(slop.getOperation()) {
            case PUT: {
                nBytes += slop.getValue().length;
                break;
            }
            case DELETE: {
                break;
            }
            default:
                logger.error("Unknown slop operation: " + slop.getOperation());
        }
        return nBytes;
    }

    /**
     * Smart slop iterator which keeps two previous batches of data
     * 
     */
    private class SlopIterator extends AbstractIterator<Versioned<Slop>> {

        private final SynchronousQueue<Versioned<Slop>> slopQueue;
        private final List<Pair<ByteArray, Version>> deleteBatch;
        private final EventThrottler writeThrottler;

        private int writtenLast = 0;
        private long slopsDone = 0L;
        private boolean shutDown = false, isComplete = false;

        public SlopIterator(SynchronousQueue<Versioned<Slop>> slopQueue,
                            List<Pair<ByteArray, Version>> deleteBatch) {
            this.slopQueue = slopQueue;
            this.deleteBatch = deleteBatch;
            this.writeThrottler = new EventThrottler(voldemortConfig.getSlopMaxWriteBytesPerSec());
        }

        public boolean isComplete() {
            return isComplete;
        }

        @Override
        protected Versioned<Slop> computeNext() {
            try {
                Versioned<Slop> head = null;
                if(!shutDown) {
                    head = slopQueue.take();
                    if(head.equals(END)) {
                        shutDown = true;
                        isComplete = true;
                    } else {
                        slopsDone++;
                        if(slopsDone % voldemortConfig.getSlopBatchSize() == 0) {
                            shutDown = true;
                        }

                        writeThrottler.maybeThrottle(writtenLast);
                        writtenLast = slopSize(head);
                        deleteBatch.add(Pair.create(head.getValue().makeKey(),
                                                    (Version) head.getVersion()));
                        return head;
                    }
                }
                return endOfData();
            } catch(Exception e) {
                logger.error("Got an exception " + e);
                return endOfData();
            }
        }

    }

    private void acquireRepairPermit() {
        logger.info("Acquiring lock to perform streaming slop pusher job ");
        try {
            this.repairPermits.acquire(null);
            logger.info("Acquired lock to perform streaming slop pusher job ");
        } catch(InterruptedException e) {
            stopAdminClient();
            throw new IllegalStateException("Streaming slop pusher job interrupted while waiting for permit.",
                                            e);
        }
    }

    private class SlopConsumer implements Runnable {

        private final int nodeId;
        private SynchronousQueue<Versioned<Slop>> slopQueue;
        private long startTime;
        private SlopStorageEngine slopStorageEngine;

        // Keep two lists to track deleted items
        private List<Pair<ByteArray, Version>> previous, current;

        public SlopConsumer(int nodeId,
                            SynchronousQueue<Versioned<Slop>> slopQueue,
                            SlopStorageEngine slopStorageEngine) {
            this.nodeId = nodeId;
            this.slopQueue = slopQueue;
            this.slopStorageEngine = slopStorageEngine;
            this.previous = Lists.newArrayList();
            this.current = Lists.newArrayList();
        }

        public void run() {
            try {
                SlopIterator iterator = null;
                do {
                    if(!current.isEmpty()) {
                        if(!previous.isEmpty()) {
                            for(Pair<ByteArray, Version> entry: previous) {
                                slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                            }
                            Long succeeded = succeededByNode.get(nodeId);
                            succeeded += previous.size();
                            succeededByNode.put(nodeId, succeeded);
                            previous.clear();
                        }
                        previous = null;
                        previous = current;
                        current = Lists.newArrayList();
                    }
                    this.startTime = System.currentTimeMillis();
                    iterator = new SlopIterator(slopQueue, current);
                    adminClient.streamingOps.updateSlopEntries(nodeId, iterator);
                } while(!iterator.isComplete());

                // Clear up both previous and current
                if(!previous.isEmpty()) {
                    for(Pair<ByteArray, Version> entry: previous)
                        slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                    Long succeeded = succeededByNode.get(nodeId);
                    succeeded += previous.size();
                    succeededByNode.put(nodeId, succeeded);
                    previous.clear();
                }
                if(!current.isEmpty()) {
                    for(Pair<ByteArray, Version> entry: current)
                        slopStorageEngine.delete(entry.getFirst(), entry.getSecond());
                    Long succeeded = succeededByNode.get(nodeId);
                    succeeded += current.size();
                    succeededByNode.put(nodeId, succeeded);
                    current.clear();
                }

            } catch(UnreachableStoreException e) {
                failureDetector.recordException(metadataStore.getCluster().getNodeById(nodeId),
                                                System.currentTimeMillis() - this.startTime,
                                                e);
                throw e;
            } finally {
                // Clean the slop queue and remove the queue from the global
                // queue
                slopQueue.clear();
                slopQueues.remove(nodeId);
            }
        }
    }
}

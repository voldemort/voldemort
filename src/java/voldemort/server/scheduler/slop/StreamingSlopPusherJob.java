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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
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
import voldemort.store.stats.StreamingStats;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("unchecked")
public class StreamingSlopPusherJob extends SlopPusherJob implements Runnable {

    private final static Logger logger = Logger.getLogger(StreamingSlopPusherJob.class.getName());
    public final static String TYPE_NAME = "streaming";

    private final static Versioned<Slop> END = Versioned.value(null);

    private ConcurrentMap<Integer, SlopConsumer> slopConsumersMap;
    private final EventThrottler readThrottler;
    private AdminClient adminClient;
    private Cluster cluster;

    private final Map<Integer, Set<Integer>> zoneMapping;
    private ConcurrentHashMap<Integer, Long> attemptedByNode;
    private ConcurrentHashMap<Integer, Long> succeededByNode;
    private final StreamingStats streamStats;

    public StreamingSlopPusherJob(StoreRepository storeRepo,
                                  MetadataStore metadataStore,
                                  FailureDetector failureDetector,
                                  VoldemortConfig voldemortConfig,
                                  ScanPermitWrapper repairPermits) {
        super(storeRepo, metadataStore, failureDetector, voldemortConfig, repairPermits);
        if(voldemortConfig.isJmxEnabled()) {
            this.streamStats = storeRepo.getStreamingStats(this.storeRepo.getSlopStore().getName());
        } else {
            this.streamStats = null;
        }

        this.readThrottler = new EventThrottler(voldemortConfig.getSlopMaxReadBytesPerSec());
        this.adminClient = null;
        this.zoneMapping = Maps.newHashMap();
    }

    public void run() {
        // load the metadata before each run, in case the cluster is changed
        loadMetadata();

        // don't try to run slop pusher job when rebalancing
        if(metadataStore.getServerStateUnlocked()
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
        Set<String> storeNames = StoreDefinitionUtils.getStoreNamesSet(metadataStore.getStoreDefList());

        acquireRepairPermit();
        try {
            StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
            iterator = slopStore.entries();

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<Slop>> keyAndVal;
                try {
                    keyAndVal = iterator.next();
                    Versioned<Slop> versioned = keyAndVal.getSecond();

                    // Track the scan progress
                    if(this.streamStats != null) {
                        this.streamStats.reportStreamingSlopScan();
                    }

                    // Retrieve the node
                    int nodeId = versioned.getValue().getNodeId();

                    // check for dead slops
                    if(isSlopDead(cluster, storeNames, versioned.getValue())) {
                        handleDeadSlop(slopStorageEngine, keyAndVal);
                        // Move on to the next slop. we either delete it or
                        // ignore it.
                        continue;
                    }

                    Node node = cluster.getNodeById(nodeId);

                    attemptedPushes.incrementAndGet();
                    Long attempted = attemptedByNode.get(nodeId);
                    attemptedByNode.put(nodeId, attempted + 1L);
                    if(attemptedPushes.get() % 10000 == 0)
                        logger.info("Attempted pushing " + attemptedPushes + " slops");

                    if(logger.isTraceEnabled())
                        logger.trace("Pushing slop for " + versioned.getValue().getNodeId()
                                     + " and store  " + versioned.getValue().getStoreName()
                                     + " of key: " + versioned.getValue().getKey());

                    if(failureDetector.isAvailable(node)) {
                        SlopConsumer slopConsumer = slopConsumersMap.get(nodeId);
                        if(slopConsumer == null) {
                            // No previous slop queue, add one
                            slopConsumer = new SlopConsumer(nodeId, slopStorageEngine);
                            slopConsumersMap.put(nodeId, slopConsumer);
                        }
                        slopConsumer.processSlop(versioned);
                        readThrottler.maybeThrottle(nBytesRead(keyAndVal));
                    } else {
                        logger.trace(node + " declared down, won't push slop");
                    }
                } catch(RejectedExecutionException e) {
                    throw new VoldemortException("Ran out of threads in executor", e);
                }
            }

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
            for(SlopConsumer consumer: slopConsumersMap.values()) {
                consumer.streamSlops();
            }

            Map<Integer, Long> outstanding = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
            for(int nodeId: succeededByNode.keySet()) {
                Long succeeded = succeededByNode.get(nodeId);
                Long attempted = attemptedByNode.get(nodeId);
                if(succeeded > 0 || attempted > 0) {
                    logger.info("Slops to node " + nodeId + " - Succeeded - " + succeeded
                                + " - Attempted - " + attempted);
                }
                outstanding.put(nodeId, attempted - succeeded);
            }
            logger.info("Completed streaming slop pusher job which started at " + startTime
                        + " isTerminatedEarly " + terminatedEarly);

            // Only if exception didn't take place do we update the counts
            if(!terminatedEarly) {
                slopStorageEngine.resetStats(outstanding);
            }

            // Shut down admin client as not to waste connections
            slopConsumersMap.clear();
            stopAdminClient();
            this.repairPermits.release(this.getClass().getCanonicalName());
        }

    }

    private void loadMetadata() {
        this.cluster = metadataStore.getCluster();
        this.failureDetector.getConfig().setCluster(cluster);
        this.slopConsumersMap = new ConcurrentHashMap<Integer, SlopConsumer>(cluster.getNumberOfNodes());
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
     * This slop iterator serves 2 purposes 1) Throttle the slop, to ensure the
     * quota 2) Remember the sent slops, so that it can be deleted from
     * persistent store
     */
    private class SlopIterator extends AbstractIterator<Versioned<Slop>> {

        private final List<Versioned<Slop>> slopsForNode;
        private int currentPosition = 0;
        private final EventThrottler writeThrottler;

        private int writtenLast = 0;

        public SlopIterator(List<Versioned<Slop>> slopsForNode) {
            this.slopsForNode = slopsForNode;
            this.writeThrottler = new EventThrottler(voldemortConfig.getSlopMaxWriteBytesPerSec());
        }

        public int removeProcessedElements(SlopStorageEngine slopStorageEngine) {
            for(int i = 0; i < currentPosition; i++) {
                Versioned<Slop> head = slopsForNode.get(i);
                slopStorageEngine.delete(head.getValue().makeKey(), head.getVersion());
            }
            slopsForNode.subList(0, currentPosition).clear();
            int returnValue = currentPosition;
            return returnValue;
        }

        @Override
        protected Versioned<Slop> computeNext() {
            if(currentPosition < slopsForNode.size()) {

                Versioned<Slop> head = slopsForNode.get(currentPosition);

                writeThrottler.maybeThrottle(writtenLast);
                writtenLast = slopSize(head);

                currentPosition++;
                return head;
            } else {
                return endOfData();
            }
        }

    }

    private void acquireRepairPermit() {
        logger.info("Acquiring lock to perform streaming slop pusher job ");
        try {
            this.repairPermits.acquire(null, this.getClass().getCanonicalName());
            logger.info("Acquired lock to perform streaming slop pusher job ");
        } catch(InterruptedException e) {
            stopAdminClient();
            throw new IllegalStateException("Streaming slop pusher job interrupted while waiting for permit.",
                                            e);
        }
    }

    private class SlopConsumer {

        private final int nodeId;
        private List<Versioned<Slop>> slopsForNode;
        private long startTime;
        private SlopStorageEngine slopStorageEngine;

        public SlopConsumer(int nodeId, SlopStorageEngine slopStorageEngine) {
            this.nodeId = nodeId;
            this.slopsForNode = new ArrayList<Versioned<Slop>>(voldemortConfig.getSlopBatchSize());
            this.slopStorageEngine = slopStorageEngine;
        }

        public void streamSlops() {
            try {
                int succeeded = 0;
                if(slopsForNode.size() > 0) {
                    SlopIterator iterator = new SlopIterator(slopsForNode);
                    adminClient.streamingOps.updateSlopEntries(nodeId, iterator);

                    succeeded = iterator.removeProcessedElements(slopStorageEngine);
                }
                Long totalSuccess = succeededByNode.get(nodeId);
                totalSuccess += succeeded;
                succeededByNode.put(nodeId, totalSuccess);
            } catch(UnreachableStoreException e) {
                logger.info("UnreachableStoreException while streaming slop to the node " + nodeId
                            + " Message " + e.getMessage());
                failureDetector.recordException(metadataStore.getCluster().getNodeById(nodeId),
                                                System.currentTimeMillis() - this.startTime,
                                                e);
            } catch(Exception e) {
                logger.warn("Failed to stream slop to node " + nodeId, e);
            } finally {
                slopsForNode.clear();
            }
        }

        public void processSlop(Versioned<Slop> slop) {
            slopsForNode.add(slop);
            if(slopsForNode.size() >= voldemortConfig.getSlopBatchSize()) {
                streamSlops();
            }
        }
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}

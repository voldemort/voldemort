/*
 * Copyright 2012-2013 LinkedIn, Inc
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

package voldemort.server.rebalance.async;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.PartitionListIterator;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.StoreInstance;
import voldemort.versioning.Versioned;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Individual rebalancing operation run on the server side as an async
 * operation. This is run on the donor node
 */
public class DonorBasedRebalanceAsyncOperation extends RebalanceAsyncOperation {

    public static final Pair<ByteArray, Versioned<byte[]>> END = Pair.create(new ByteArray("END".getBytes()),
                                                                             new Versioned<byte[]>("END".getBytes()));
    public static final Pair<ByteArray, Versioned<byte[]>> BREAK = Pair.create(new ByteArray("BREAK".getBytes()),
                                                                               new Versioned<byte[]>("BREAK".getBytes()));

    // Batch 1000 entries for each fetchUpdate call.
    private static final int FETCHUPDATE_BATCH_SIZE = 1000;
    // Print scanned entries every 100k
    private static final int SCAN_PROGRESS_COUNT = 100000;

    private final List<RebalancePartitionsInfo> stealInfos;
    private final StoreRepository storeRepository;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Cluster initialCluster;
    private final Cluster targetCluster;
    private final boolean usePartitionScan;

    private final HashMultimap<String, Pair<Integer, HashMap<Integer, List<Integer>>>> storeToNodePartitionMapping;

    // each table being rebalanced is associated with one executor service and a
    // pool of threads
    private Map<String, Pair<ExecutorService, List<DonorBasedRebalancePusherSlave>>> updatePushSlavePool;

    private HashMultimap<String, Pair<Integer, HashMap<Integer, List<Integer>>>> groupByStores(List<RebalancePartitionsInfo> stealInfos) {

        HashMultimap<String, Pair<Integer, HashMap<Integer, List<Integer>>>> returnMap = HashMultimap.create();
        for(RebalancePartitionsInfo info: stealInfos) {
            int stealerNodeId = info.getStealerId();
            for(Entry<String, HashMap<Integer, List<Integer>>> entry: info.getStoreToReplicaToAddPartitionList()
                                                                          .entrySet()) {
                returnMap.put(entry.getKey(), Pair.create(stealerNodeId, entry.getValue()));
            }
        }
        return returnMap;
    }

    public DonorBasedRebalanceAsyncOperation(Rebalancer rebalancer,
                                             StoreRepository storeRepository,
                                             VoldemortConfig voldemortConfig,
                                             MetadataStore metadataStore,
                                             int requestId,
                                             List<RebalancePartitionsInfo> stealInfos,
                                             boolean usePartitionScan) {
        super(rebalancer, voldemortConfig, metadataStore, requestId, "Donor based rebalance : "
                                                                     + stealInfos);
        this.storeRepository = storeRepository;
        this.stealInfos = stealInfos;
        this.targetCluster = metadataStore.getCluster();
        this.initialCluster = stealInfos.get(0).getInitialCluster();
        this.usePartitionScan = usePartitionScan;

        // Group the plans by the store names
        this.storeToNodePartitionMapping = groupByStores(stealInfos);
        updatePushSlavePool = Collections.synchronizedMap(new HashMap<String, Pair<ExecutorService, List<DonorBasedRebalancePusherSlave>>>());
    }

    @Override
    public void operate() throws Exception {

        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           metadataStore.getCluster(),
                                                           voldemortConfig.getMaxParallelStoresRebalancing());
        final CopyOnWriteArrayList<Exception> failures = new CopyOnWriteArrayList<Exception>();
        final ConcurrentLinkedQueue<String> storesRebalancing = new ConcurrentLinkedQueue<String>();
        final ConcurrentLinkedQueue<String> storesCompleted = new ConcurrentLinkedQueue<String>();
        final int totalStoresCount = storeToNodePartitionMapping.keySet().size();

        try {

            for(final String storeName: ImmutableList.copyOf(storeToNodePartitionMapping.keySet())) {

                executors.submit(new Runnable() {

                    public void run() {
                        try {
                            Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples = storeToNodePartitionMapping.get(storeName);
                            boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                                   .getType()
                                                                   .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

                            // Add the store to the rebalancing list
                            storesRebalancing.add(storeName);
                            updateStatus(getHeader(stealInfos) + "Completed working on "
                                         + storesCompleted.size() + " out of " + totalStoresCount
                                         + " stores. Still rebalancing " + storesRebalancing);

                            // Start the rebalance..
                            rebalanceStore(storeName,
                                           adminClient,
                                           stealerNodeToMappingTuples,
                                           isReadOnlyStore);

                            // Remove the metadata from all the stealer nodes
                            for(Pair<Integer, HashMap<Integer, List<Integer>>> entry: stealerNodeToMappingTuples) {
                                adminClient.rebalanceOps.deleteStoreRebalanceState(metadataStore.getNodeId(),
                                                                                   entry.getFirst(),
                                                                                   storeName);
                                logger.info("Removed rebalance state for store " + storeName
                                            + " : " + metadataStore.getNodeId() + " ---> "
                                            + entry.getFirst());
                            }

                            // We finished the store, delete it
                            storesRebalancing.remove(storeName);

                            // Increment the store count
                            storesCompleted.add(storeName);

                            updateStatus(getHeader(stealInfos) + "Completed working on "
                                         + storesCompleted.size() + " out of " + totalStoresCount
                                         + " stores. Still rebalancing " + storesRebalancing);

                        } catch(Exception e) {
                            logger.error(getHeader(stealInfos)
                                         + "Error while rebalancing for store " + storeName + " - "
                                         + e.getMessage(), e);
                            failures.add(e);
                        }
                    }
                });

            }

            waitForShutdown();

            // Check if we have finished all of the stores
            if(storesCompleted.size() != totalStoresCount) {
                logger.error(getHeader(stealInfos)
                             + "Could not complete all stores. Completed stores - "
                             + storesCompleted);
                throw new VoldemortRebalancingException(getHeader(stealInfos)
                                                                + "Could not complete all stores. Completed stores - "
                                                                + storesCompleted,
                                                        failures);
            } else {
                logger.info(getHeader(stealInfos) + "Rebalance of " + stealInfos
                            + " completed successfully for all " + totalStoresCount + " stores");
            }
        } finally {
            adminClient.close();
            adminClient = null;
            for(RebalancePartitionsInfo stealInfo: stealInfos) {
                rebalancer.releaseRebalancingPermit(stealInfo.getStealerId());
            }
        }
    }

    private String getHeader(List<RebalancePartitionsInfo> stealInfos) {
        List<Integer> stealerNodeIds = Lists.newArrayList();
        for(RebalancePartitionsInfo info: stealInfos) {
            stealerNodeIds.add(info.getStealerId());
        }
        return " Donor " + stealInfos.get(0).getDonorId() + ", Stealer " + stealerNodeIds + "] ";
    }

    /**
     * Blocking function which completes the migration of one store
     * 
     * @param storeName The name of the store
     * @param adminClient Admin client used to initiate the copying of data
     * @param stealerNodeToMappingTuples For a particular store set of stealer
     *        node to [ replica to partition ] mapping
     * @param isReadOnlyStore Boolean indicating that this is a read-only store
     */
    private void rebalanceStore(final String storeName,
                                final AdminClient adminClient,
                                Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples,
                                boolean isReadOnlyStore) {

        StorageEngine<ByteArray, byte[], byte[]> storageEngine = storeRepository.getStorageEngine(storeName);
        StoreDefinition storeDef = metadataStore.getStoreDef(storeName);
        List<DonorBasedRebalancePusherSlave> storePushSlaves = Lists.newArrayList();
        ExecutorService pushSlavesExecutor = Executors.newCachedThreadPool(new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
        updatePushSlavePool.put(storeName,
                                new Pair<ExecutorService, List<DonorBasedRebalancePusherSlave>>(pushSlavesExecutor,
                                                                                                storePushSlaves));

        if(isReadOnlyStore) {

            // TODO: Add support for reading local RO files and streaming them
            // over
            throw new VoldemortException("Donor-based rebalancing for read-only store is currently not supported!");
        } else {

            // Create queue for every node that we need to dump data to
            HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeToQueue = Maps.newHashMap();

            // Optimization to get rid of redundant copying of
            // data which already exists on this node
            Set<Pair<Integer, HashMap<Integer, List<Integer>>>> optimizedStealerNodeToMappingTuples = Sets.newHashSet();

            if(voldemortConfig.getRebalancingOptimization() && !storageEngine.isPartitionAware()) {
                for(Pair<Integer, HashMap<Integer, List<Integer>>> entry: stealerNodeToMappingTuples) {
                    HashMap<Integer, List<Integer>> optimizedReplicaToPartition = RebalanceUtils.getOptimizedReplicaToPartitionList(entry.getFirst(),
                                                                                                                                    initialCluster,
                                                                                                                                    storeDef,
                                                                                                                                    entry.getSecond());

                    if(optimizedReplicaToPartition.size() > 0) {
                        optimizedStealerNodeToMappingTuples.add(Pair.create(entry.getFirst(),
                                                                            optimizedReplicaToPartition));
                    }
                }
            } else {
                optimizedStealerNodeToMappingTuples.addAll(stealerNodeToMappingTuples);
            }

            if(optimizedStealerNodeToMappingTuples.size() <= 0) {
                return;
            }

            for(Pair<Integer, HashMap<Integer, List<Integer>>> tuple: stealerNodeToMappingTuples) {
                final SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> queue = new SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>();
                nodeToQueue.put(tuple.getFirst(), queue);

                String jobName = "DonorBasedRebalancePusherSlave for store " + storeName
                                 + " on node " + tuple.getFirst();
                DonorBasedRebalancePusherSlave updatePushSlave = new DonorBasedRebalancePusherSlave(tuple.getFirst(),
                                                                                                    queue,
                                                                                                    storeName,
                                                                                                    adminClient);
                storePushSlaves.add(updatePushSlave);
                pushSlavesExecutor.execute(updatePushSlave);
                logger.info("Started a thread for " + jobName);
            }

            if(usePartitionScan && storageEngine.isPartitionScanSupported())
                fetchEntriesForStealersPartitionScan(storageEngine,
                                                     optimizedStealerNodeToMappingTuples,
                                                     storeDef,
                                                     nodeToQueue,
                                                     storeName);
            else
                fetchEntriesForStealers(storageEngine,
                                        optimizedStealerNodeToMappingTuples,
                                        storeDef,
                                        nodeToQueue,
                                        storeName);
        }
    }

    private void fetchEntriesForStealers(StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                         Set<Pair<Integer, HashMap<Integer, List<Integer>>>> optimizedStealerNodeToMappingTuples,
                                         StoreDefinition storeDef,
                                         HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeToQueue,
                                         String storeName) {
        int scanned = 0;
        int[] fetched = new int[targetCluster.getNumberOfNodes()];
        long startTime = System.currentTimeMillis();

        ClosableIterator<ByteArray> keys = storageEngine.keys();

        try {
            while(running.get() && keys.hasNext()) {
                ByteArray key = keys.next();
                scanned++;
                List<Integer> nodeIds = StoreInstance.checkKeyBelongsToPartition(key.get(),
                                                                                 optimizedStealerNodeToMappingTuples,
                                                                                 targetCluster,
                                                                                 storeDef);

                if(nodeIds.size() > 0) {
                    List<Versioned<byte[]>> values = storageEngine.get(key, null);
                    putAll(nodeIds, key, values, nodeToQueue, fetched);
                }

                // print progress for every 100k entries.
                if(0 == scanned % SCAN_PROGRESS_COUNT) {
                    printProgress(scanned, fetched, startTime, storeName);
                }
            }
            terminateAllSlaves(storeName);
        } catch(InterruptedException e) {
            logger.info("InterruptedException received while sending entries to remote nodes, the process is terminating...");
            terminateAllSlavesAsync(storeName);
        } finally {
            close(keys, storeName, scanned, fetched, startTime);
        }
    }

    private void fetchEntriesForStealersPartitionScan(StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                                      Set<Pair<Integer, HashMap<Integer, List<Integer>>>> optimizedStealerNodeToMappingTuples,
                                                      StoreDefinition storeDef,
                                                      HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeToQueue,
                                                      String storeName) {
        int scanned = 0;
        int[] fetched = new int[targetCluster.getNumberOfNodes()];
        long startTime = System.currentTimeMillis();

        // construct a set of all the partitions we will be fetching
        Set<Integer> partitionsToDonate = new HashSet<Integer>();
        for(Pair<Integer, HashMap<Integer, List<Integer>>> nodePartitionMapPair: optimizedStealerNodeToMappingTuples) {
            // for each of the nodes, add all the partitions requested
            HashMap<Integer, List<Integer>> replicaToPartitionMap = nodePartitionMapPair.getSecond();
            if(replicaToPartitionMap != null && replicaToPartitionMap.values() != null) {
                for(List<Integer> partitions: replicaToPartitionMap.values())
                    if(partitions != null)
                        partitionsToDonate.addAll(partitions);
            }
        }

        // check if all the partitions being requested are present in the
        // current node
        for(Integer partition: partitionsToDonate) {
            if(!StoreInstance.checkPartitionBelongsToNode(partition,
                                                          voldemortConfig.getNodeId(),
                                                          initialCluster,
                                                          storeDef)) {
                logger.info("Node " + voldemortConfig.getNodeId()
                            + " does not seem to contain partition " + partition
                            + " as primary/secondary");
            }
        }

        PartitionListIterator entries = new PartitionListIterator(storageEngine,
                                                                  new ArrayList<Integer>(partitionsToDonate));

        try {
            while(running.get() && entries.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entries.next();
                ByteArray key = entry.getFirst();
                Versioned<byte[]> value = entry.getSecond();

                scanned++;
                List<Integer> nodeIds = StoreInstance.checkKeyBelongsToPartition(key.get(),
                                                                                 optimizedStealerNodeToMappingTuples,
                                                                                 targetCluster,
                                                                                 storeDef);

                if(nodeIds.size() > 0) {
                    putValue(nodeIds, key, value, nodeToQueue, fetched);
                }

                // print progress for every 100k entries.
                if(0 == scanned % SCAN_PROGRESS_COUNT) {
                    printProgress(scanned, fetched, startTime, storeName);
                }
            }
            terminateAllSlaves(storeName);
        } catch(InterruptedException e) {
            logger.info("InterruptedException received while sending entries to remote nodes, the process is terminating...");
            terminateAllSlavesAsync(storeName);
        } finally {
            close(entries, storeName, scanned, fetched, startTime);
        }
    }

    private void putAll(List<Integer> dests,
                        ByteArray key,
                        List<Versioned<byte[]>> values,
                        HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeToQueue,
                        int[] fetched) throws InterruptedException {
        for(Versioned<byte[]> value: values)
            putValue(dests, key, value, nodeToQueue, fetched);
    }

    private void putValue(List<Integer> dests,
                          ByteArray key,
                          Versioned<byte[]> value,
                          HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeToQueue,
                          int[] fetched) throws InterruptedException {
        for(int nodeId: dests) {
            fetched[nodeId]++;
            nodeToQueue.get(nodeId).put(Pair.create(key, value));
            if(0 == fetched[nodeId] % FETCHUPDATE_BATCH_SIZE) {
                nodeToQueue.get(nodeId).put(BREAK);
            }
        }
    }

    private void printProgress(int scanned, int[] fetched, long startTime, String storeName) {
        logger.info("Successfully scanned " + scanned + " tuples in "
                    + ((System.currentTimeMillis() - startTime) / 1000) + " s");
        for(int i = 0; i < fetched.length; i++) {
            logger.info(fetched[i] + " tuples fetched for store '" + storeName + " for node " + i);
        }
    }

    private void close(ClosableIterator<?> storageItr,
                       String storeName,
                       int scanned,
                       int[] fetched,
                       long startTime) {

        printProgress(scanned, fetched, startTime, storeName);
        if(null != storageItr)
            storageItr.close();
    }

    private void terminateAllSlaves(String storeName) {
        // Everything is done, put the terminator in
        logger.info("Terminating DonorBasedRebalancePushSlaves...");
        ExecutorService pushSlavesExecutor = updatePushSlavePool.get(storeName).getFirst();
        List<DonorBasedRebalancePusherSlave> pushSlaves = updatePushSlavePool.get(storeName)
                                                                             .getSecond();
        for(Iterator<DonorBasedRebalancePusherSlave> it = pushSlaves.iterator(); it.hasNext();) {
            it.next().requestCompletion();
        }

        // signal and wait for all slaves to finish
        pushSlavesExecutor.shutdown();
        try {
            if(pushSlavesExecutor.awaitTermination(30, TimeUnit.MINUTES)) {
                logger.info("All DonorBasedRebalancePushSlaves terminated successfully.");
            } else {
                logger.warn("Timed out while waiting for pusher slaves to shutdown!!!");
            }
        } catch(InterruptedException e) {
            logger.warn("Interrupted while waiting for pusher slaves to shutdown!!!");
        }
        logger.info("DonorBasedRebalancingOperation existed.");
    }

    private void terminateAllSlavesAsync(String storeName) {
        logger.info("Terminating DonorBasedRebalancePushSlaves asynchronously.");
        if(null == storeName) {
            for(Pair<ExecutorService, List<DonorBasedRebalancePusherSlave>> pair: updatePushSlavePool.values()) {
                ExecutorService pushSlavesExecutor = pair.getFirst();
                List<DonorBasedRebalancePusherSlave> pushSlaves = pair.getSecond();
                for(Iterator<DonorBasedRebalancePusherSlave> it = pushSlaves.iterator(); it.hasNext();) {
                    it.next().requestCompletion();
                }
                pushSlavesExecutor.shutdownNow();
            }
        } else {
            ExecutorService pushSlavesExecutor = updatePushSlavePool.get(storeName).getFirst();
            List<DonorBasedRebalancePusherSlave> pushSlaves = updatePushSlavePool.get(storeName)
                                                                                 .getSecond();
            for(Iterator<DonorBasedRebalancePusherSlave> it = pushSlaves.iterator(); it.hasNext();) {
                it.next().requestCompletion();
            }
            pushSlavesExecutor.shutdownNow();
        }
        logger.info("DonorBasedRebalancingAsyncOperation existed.");
    }

    @Override
    public void stop() {
        running.set(false);
        updateStatus(getHeader(stealInfos) + "Stop called on donor-based rebalance operation");
        logger.info(getHeader(stealInfos) + "Stop called on donor-based rebalance operation");
        terminateAllSlavesAsync(null);
        executors.shutdownNow();
    }
}
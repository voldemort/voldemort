/*
 * Copyright 2011 LinkedIn, Inc
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
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
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

    private final List<RebalancePartitionsInfo> stealInfos;
    private final StoreRepository storeRepository;
    private final static Pair<ByteArray, Versioned<byte[]>> END = Pair.create(null, null);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Cluster initialCluster;

    private final HashMultimap<String, Pair<Integer, HashMap<Integer, List<Integer>>>> storeToNodePartitionMapping;

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
                                             List<RebalancePartitionsInfo> stealInfos) {
        super(rebalancer, voldemortConfig, metadataStore, requestId, "Donor based rebalance : "
                                                                     + stealInfos);
        this.storeRepository = storeRepository;
        this.stealInfos = stealInfos;
        this.initialCluster = stealInfos.get(0).getInitialCluster();

        // Group the plans by the store names
        this.storeToNodePartitionMapping = groupByStores(stealInfos);
    }

    @Override
    public void operate() throws Exception {

        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           metadataStore.getCluster(),
                                                           voldemortConfig.getMaxParallelStoresRebalancing());
        final List<Exception> failures = new ArrayList<Exception>();
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

                            // We finished the store, delete it
                            storesRebalancing.remove(storeName);

                            // Increment the store count
                            storesCompleted.add(storeName);

                            // Remove the metadata from all the stealer nodes
                            for(Pair<Integer, HashMap<Integer, List<Integer>>> entry: stealerNodeToMappingTuples) {
                                adminClient.deleteStoreRebalanceState(metadataStore.getNodeId(),
                                                                      entry.getFirst(),
                                                                      storeName);
                            }

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
                                                        + storesCompleted);
            } else {
                logger.info(getHeader(stealInfos) + "Rebalance of " + stealInfos
                            + " completed successfully for all " + totalStoresCount + " stores");
            }
        } finally {
            adminClient.stop();
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
        return " Donor " + stealInfos.get(0).getDonorId() + ", Donor " + stealerNodeIds + "] ";
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

                ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> nodeIterator = new ClosableIterator<Pair<ByteArray, Versioned<byte[]>>>() {

                    public void close() {}

                    public boolean hasNext() {
                        // Empty => Not yet finished
                        Pair<ByteArray, Versioned<byte[]>> element = queue.poll();
                        if(element != null && element.equals(END)) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    public Pair<ByteArray, Versioned<byte[]>> next() {
                        try {
                            return queue.take();
                        } catch(InterruptedException e) {
                            logger.info("Next did not return anything");
                        }
                        return null;
                    }

                    public void remove() {
                        throw new VoldemortException("Remove not supported");
                    }

                };

                adminClient.updateEntries(tuple.getFirst(), storeName, nodeIterator, null);
            }

            ClosableIterator<ByteArray> keys = storageEngine.keys();
            while(running.get() && keys.hasNext()) {
                ByteArray key = keys.next();
                List<Integer> nodeIds = RebalanceUtils.checkKeyBelongsToPartition(key.get(),
                                                                                  optimizedStealerNodeToMappingTuples,
                                                                                  initialCluster,
                                                                                  storeDef);

                if(nodeIds.size() > 0) {
                    List<Versioned<byte[]>> values = storageEngine.get(key, null);
                    for(Versioned<byte[]> value: values) {
                        for(int nodeId: nodeIds) {
                            try {
                                nodeToQueue.get(nodeId).put(Pair.create(key, value));
                            } catch(InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

            // Everything is done, put the terminator in
            for(SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> queue: nodeToQueue.values()) {
                queue.add(END);
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);
        updateStatus(getHeader(stealInfos) + "Stop called on rebalance operation");
        logger.info(getHeader(stealInfos) + "Stop called on rebalance operation");
        executors.shutdownNow();
    }
}

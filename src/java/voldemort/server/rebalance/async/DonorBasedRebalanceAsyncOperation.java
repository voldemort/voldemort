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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Individual rebalancing operation run on the server side as an async
 * operation. This is run on the donor node
 */
public class DonorBasedRebalanceAsyncOperation extends RebalanceAsyncOperation {

    private final List<RebalancePartitionsInfo> stealInfos;
    private final StoreRepository storeRepository;

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

    public DonorBasedRebalanceAsyncOperation(StoreRepository storeRepository,
                                             VoldemortConfig voldemortConfig,
                                             MetadataStore metadataStore,
                                             int requestId,
                                             List<RebalancePartitionsInfo> stealInfos) {
        super(voldemortConfig, metadataStore, requestId, "Donor based rebalance : " + stealInfos);
        this.storeRepository = storeRepository;
        this.stealInfos = stealInfos;

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
                            boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                                   .getType()
                                                                   .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

                            // Add the store to the rebalancing list
                            storesRebalancing.add(storeName);
                            updateStatus(getHeader(stealInfos) + "Completed working on "
                                         + storesCompleted.size() + " out of " + totalStoresCount
                                         + " stores. Still rebalancing " + storesRebalancing);

                            // Start the rebalance..
                            rebalanceStore(storeName, adminClient, stealInfos, isReadOnlyStore);

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
                                                        + storesCompleted);
            } else {
                logger.info(getHeader(stealInfos) + "Rebalance of " + stealInfos
                            + " completed successfully for all " + totalStoresCount + " stores");
            }
        } finally {
            adminClient.stop();
            adminClient = null;
        }
    }

    private String getHeader(List<RebalancePartitionsInfo> stealInfos) {
        List<Integer> stealerNodeIds = Lists.newArrayList();
        for(RebalancePartitionsInfo info: stealInfos) {
            stealerNodeIds.add(info.getStealerId());
        }
        return " Donor " + stealInfos.get(0).getStealerId() + ", Donor " + stealerNodeIds + "] ";
    }

    /**
     * Blocking function which completes the migration of one store
     * 
     * @param storeName The name of the store
     * @param adminClient Admin client used to initiate the copying of data
     * @param stealInfos List of partition infos
     * @param isReadOnlyStore Boolean indicating that this is a read-only store
     */
    private void rebalanceStore(String storeName,
                                final AdminClient adminClient,
                                List<RebalancePartitionsInfo> stealInfos,
                                boolean isReadOnlyStore) {

    }

    @Override
    public void stop() {
        updateStatus(getHeader(stealInfos) + "Stop called on rebalance operation");
        executors.shutdownNow();
    }
}

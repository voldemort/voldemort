/*
 * Copyright 2011-2013 LinkedIn, Inc
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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Individual rebalancing operation run on the server side as an async
 * operation. This is run on the stealer node
 */
public class StealerBasedRebalanceAsyncOperation extends RebalanceAsyncOperation {

    private List<Integer> rebalanceStatusList;

    private final RebalancePartitionsInfo stealInfo;

    public StealerBasedRebalanceAsyncOperation(Rebalancer rebalancer,
                                               VoldemortConfig voldemortConfig,
                                               MetadataStore metadataStore,
                                               int requestId,
                                               RebalancePartitionsInfo stealInfo) {
        super(rebalancer, voldemortConfig, metadataStore, requestId, "Stealer based rebalance : "
                                                                     + stealInfo);
        this.rebalancer = rebalancer;
        this.stealInfo = stealInfo;
        this.rebalanceStatusList = new ArrayList<Integer>();
    }

    @Override
    public void operate() throws Exception {
        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           metadataStore.getCluster(),
                                                           voldemortConfig.getMaxParallelStoresRebalancing());
        final List<Exception> failures = new ArrayList<Exception>();
        final ConcurrentLinkedQueue<String> storesRebalancing = new ConcurrentLinkedQueue<String>();
        final AtomicInteger completedStoresCount = new AtomicInteger(0);
        final int totalStoresCount = stealInfo.getUnbalancedStoreList().size();

        try {

            for(final String storeName: ImmutableList.copyOf(stealInfo.getUnbalancedStoreList())) {

                executors.submit(new Runnable() {

                    public void run() {
                        try {
                            boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                                   .getType()
                                                                   .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

                            // Add the store to the rebalancing list
                            storesRebalancing.add(storeName);
                            updateStatus(getHeader(stealInfo) + "Completed working on "
                                         + completedStoresCount.get() + " out of "
                                         + totalStoresCount + " stores. Still rebalancing "
                                         + storesRebalancing);

                            // Start the rebalance..
                            rebalanceStore(storeName, adminClient, stealInfo, isReadOnlyStore);

                            // We finished the store, delete it
                            stealInfo.removeStore(storeName);
                            storesRebalancing.remove(storeName);

                            // Increment the store count
                            completedStoresCount.getAndIncrement();

                            updateStatus(getHeader(stealInfo) + "Completed working on "
                                         + completedStoresCount.get() + " out of "
                                         + totalStoresCount + " stores. Still rebalancing "
                                         + storesRebalancing);

                        } catch(Exception e) {
                            logger.error(getHeader(stealInfo)
                                         + "Error while rebalancing for store " + storeName + " - "
                                         + e.getMessage(), e);
                            failures.add(e);
                        }
                    }
                });

            }

            waitForShutdown();

            // If empty, clean state
            List<String> unbalancedStores = Lists.newArrayList(stealInfo.getUnbalancedStoreList());
            if(unbalancedStores.isEmpty()) {
                logger.info(getHeader(stealInfo) + "Rebalance of " + stealInfo
                            + " completed successfully for all " + totalStoresCount + " stores");
                updateStatus(getHeader(stealInfo) + "Rebalance of " + stealInfo
                             + " completed successfully for all " + totalStoresCount + " stores");
                metadataStore.deleteRebalancingState(stealInfo);
            } else {
                throw new VoldemortRebalancingException(getHeader(stealInfo)
                                                        + "Failed to rebalance task " + stealInfo
                                                        + ". Could only complete "
                                                        + completedStoresCount.get() + " out of "
                                                        + totalStoresCount + " stores", failures);
            }

        } finally {
            // free the permit in all cases.
            logger.info(getHeader(stealInfo) + "Releasing permit for donor node "
                        + stealInfo.getDonorId());

            rebalancer.releaseRebalancingPermit(stealInfo.getDonorId());
            adminClient.close();
            adminClient = null;
        }
    }

    @Override
    public void stop() {
        updateStatus(getHeader(stealInfo) + "Stop called on rebalance operation");
        if(null != adminClient) {
            for(int asyncID: rebalanceStatusList) {
                adminClient.rpcOps.stopAsyncRequest(metadataStore.getNodeId(), asyncID);
            }
        }

        executors.shutdownNow();
    }

    private String getHeader(RebalancePartitionsInfo stealInfo) {
        return "Stealer " + stealInfo.getStealerId() + ", Donor " + stealInfo.getDonorId() + "] ";
    }

    /**
     * Blocking function which completes the migration of one store
     * 
     * @param storeName The name of the store
     * @param adminClient Admin client used to initiate the copying of data
     * @param stealInfo The steal information
     * @param isReadOnlyStore Boolean indicating that this is a read-only store
     */
    private void rebalanceStore(String storeName,
                                final AdminClient adminClient,
                                RebalancePartitionsInfo stealInfo,
                                boolean isReadOnlyStore) {
        // Move partitions
        if(stealInfo.getReplicaToAddPartitionList(storeName) != null
           && stealInfo.getReplicaToAddPartitionList(storeName).size() > 0) {

            logger.info(getHeader(stealInfo) + "Starting partitions migration for store "
                        + storeName + " from donor node " + stealInfo.getDonorId());

            int asyncId = adminClient.storeMntOps.migratePartitions(stealInfo.getDonorId(),
                                                                    metadataStore.getNodeId(),
                                                                    storeName,
                                                                    stealInfo.getReplicaToAddPartitionList(storeName),
                                                                    null,
                                                                    stealInfo.getInitialCluster(),
                                                                    true);
            rebalanceStatusList.add(asyncId);

            if(logger.isDebugEnabled()) {
                logger.debug(getHeader(stealInfo) + "Waiting for completion for " + storeName
                             + " with async id " + asyncId);
            }
            adminClient.rpcOps.waitForCompletion(metadataStore.getNodeId(),
                                                 asyncId,
                                                 voldemortConfig.getRebalancingTimeoutSec(),
                                                 TimeUnit.SECONDS,
                                                 getStatus());

            rebalanceStatusList.remove((Object) asyncId);

            logger.info(getHeader(stealInfo) + "Completed partition migration for store "
                        + storeName + " from donor node " + stealInfo.getDonorId());
        }

        // Delete partitions
        if(stealInfo.getReplicaToDeletePartitionList(storeName) != null
           && stealInfo.getReplicaToDeletePartitionList(storeName).size() > 0 && !isReadOnlyStore) {

            logger.info(getHeader(stealInfo) + "Deleting partitions for store " + storeName
                        + " on donor node " + stealInfo.getDonorId());

            adminClient.storeMntOps.deletePartitions(stealInfo.getDonorId(),
                                                     storeName,
                                                     stealInfo.getReplicaToDeletePartitionList(storeName),
                                                     stealInfo.getInitialCluster(),
                                                     null);
            logger.info(getHeader(stealInfo) + "Deleted partitions for store " + storeName
                        + " on donor node " + stealInfo.getDonorId());

        }

        logger.info(getHeader(stealInfo) + "Finished all migration for store " + storeName);
    }
}

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

package voldemort.server.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Individual rebalancing operation run on the server side as an async operation
 */
class RebalanceAsyncOperation extends AsyncOperation {

    private final static Logger logger = Logger.getLogger(RebalanceAsyncOperation.class);

    private List<Integer> rebalanceStatusList;
    private AdminClient adminClient;

    private final ExecutorService executors;
    private final RebalancePartitionsInfo stealInfo;
    private final VoldemortConfig voldemortConfig;
    private final MetadataStore metadataStore;

    private Rebalancer rebalancer;

    protected ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    public RebalanceAsyncOperation(Rebalancer rebalancer,
                                   VoldemortConfig voldemortConfig,
                                   MetadataStore metadataStore,
                                   int requestId,
                                   RebalancePartitionsInfo stealInfo) {
        super(requestId, "Rebalance operation: " + stealInfo.toString());
        this.rebalancer = rebalancer;
        this.voldemortConfig = voldemortConfig;
        this.metadataStore = metadataStore;
        this.stealInfo = stealInfo;
        this.rebalanceStatusList = new ArrayList<Integer>();
        this.adminClient = null;
        this.executors = createExecutors(voldemortConfig.getMaxParallelStoresRebalancing());
    }

    @Override
    public void operate() throws Exception {
        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           metadataStore.getCluster(),
                                                           voldemortConfig.getMaxParallelStoresRebalancing());
        final List<Exception> failures = new ArrayList<Exception>();
        try {

            for(final String storeName: ImmutableList.copyOf(stealInfo.getUnbalancedStoreList())) {

                executors.submit(new Runnable() {

                    public void run() {
                        try {
                            boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                                   .getType()
                                                                   .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

                            logger.info(getHeader(stealInfo) + "Working on store " + storeName);

                            rebalanceStore(storeName, adminClient, stealInfo, isReadOnlyStore);

                            // We finished the store, delete it
                            stealInfo.removeStore(storeName);

                            logger.info(getHeader(stealInfo) + "Completed working on store "
                                        + storeName);

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
                            + " completed successfully.");
                updateStatus(getHeader(stealInfo) + "Rebalance of " + stealInfo
                             + " completed successfully.");
                metadataStore.deleteRebalancingState(stealInfo);
            } else {
                throw new VoldemortRebalancingException(getHeader(stealInfo)
                                                                + "Failed to rebalance task "
                                                                + stealInfo,
                                                        failures);
            }

        } finally {
            // free the permit in all cases.
            logger.info(getHeader(stealInfo) + "Releasing permit for donor node "
                        + stealInfo.getDonorId());

            rebalancer.releaseRebalancingPermit(stealInfo.getDonorId());
            adminClient.stop();
            adminClient = null;
        }
    }

    private void waitForShutdown() {
        try {
            executors.shutdown();
            executors.awaitTermination(voldemortConfig.getRebalancingTimeoutSec(), TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.error("Interrupted while awaiting termination for executors.", e);
        }
    }

    @Override
    public void stop() {
        updateStatus(getHeader(stealInfo) + "Stop called on rebalance operation");
        if(null != adminClient) {
            for(int asyncID: rebalanceStatusList) {
                adminClient.stopAsyncRequest(metadataStore.getNodeId(), asyncID);
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
        logger.info(getHeader(stealInfo) + "Starting partitions migration for store " + storeName
                    + " from donor node " + stealInfo.getDonorId());
        updateStatus(getHeader(stealInfo) + "Started partition migration for store " + storeName
                     + " from donor node " + stealInfo.getDonorId());

        int asyncId = adminClient.migratePartitions(stealInfo.getDonorId(),
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
        adminClient.waitForCompletion(metadataStore.getNodeId(),
                                      asyncId,
                                      voldemortConfig.getRebalancingTimeoutSec(),
                                      TimeUnit.SECONDS,
                                      getStatus());

        rebalanceStatusList.remove((Object) asyncId);

        logger.info(getHeader(stealInfo) + "Completed partition migration for store " + storeName
                    + " from donor node " + stealInfo.getDonorId());
        updateStatus(getHeader(stealInfo) + "Completed partition migration for store " + storeName
                     + " from donor node " + stealInfo.getDonorId());

        if(stealInfo.getReplicaToDeletePartitionList(storeName) != null
           && stealInfo.getReplicaToDeletePartitionList(storeName).size() > 0 && !isReadOnlyStore) {
            logger.info(getHeader(stealInfo) + "Deleting partitions for store " + storeName
                        + " on donor node " + stealInfo.getDonorId());
            updateStatus(getHeader(stealInfo) + "Deleting partitions for store " + storeName
                         + " on donor node " + stealInfo.getDonorId());

            adminClient.deletePartitions(stealInfo.getDonorId(),
                                         storeName,
                                         stealInfo.getReplicaToDeletePartitionList(storeName),
                                         stealInfo.getInitialCluster(),
                                         null);
            logger.info(getHeader(stealInfo) + "Deleted partitions for store " + storeName
                        + " on donor node " + stealInfo.getDonorId());
            updateStatus(getHeader(stealInfo) + "Deleted partitions for store " + storeName
                         + " on donor node " + stealInfo.getDonorId());
        }

        logger.info(getHeader(stealInfo) + "Finished all migration for store " + storeName);
        updateStatus(getHeader(stealInfo) + "Finished all migration for store " + storeName);
    }
}

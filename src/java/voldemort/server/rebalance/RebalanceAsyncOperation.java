/*
 * Copyright 2008-2010 LinkedIn, Inc
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
 * Individual Rebalancing Operation
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
        super(requestId, "Rebalance Operation: " + stealInfo.toString());
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
                                                           voldemortConfig.getMaxParallelStoresRebalancing(),
                                                           1);
        final List<Exception> failures = new ArrayList<Exception>();
        try {

            for(final String storeName: ImmutableList.copyOf(stealInfo.getUnbalancedStoreList())) {

                executors.submit(new Runnable() {

                    public void run() {
                        try {
                            boolean isReadOnlyStore = metadataStore.getStoreDef(storeName)
                                                                   .getType()
                                                                   .compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0;

                            if(logger.isDebugEnabled()) {
                                logger.debug("operate() -  storeName: " + storeName
                                             + ", stealInfo: " + stealInfo);
                            }

                            rebalanceStore(storeName, adminClient, stealInfo, isReadOnlyStore);

                            List<String> tempUnbalancedStoreList = new ArrayList<String>(stealInfo.getUnbalancedStoreList());
                            tempUnbalancedStoreList.remove(storeName);
                            stealInfo.setUnbalancedStoreList(tempUnbalancedStoreList);
                            metadataStore.deleteRebalancingState(stealInfo);

                        } catch(Exception e) {
                            logger.error("rebalanceSubTask:" + stealInfo + " failed for store:"
                                         + storeName + " - " + e.getMessage(), e);
                            failures.add(e);
                        }
                    }
                });

            }

            waitForShutdown();

            // If empty, clean state
            List<String> unbalancedStores = Lists.newArrayList(stealInfo.getUnbalancedStoreList());
            if(unbalancedStores.isEmpty()) {
                logger.info("Rebalancer: rebalance " + stealInfo + " completed successfully.");
                metadataStore.deleteRebalancingState(stealInfo);
            } else {
                throw new VoldemortRebalancingException("Failed to rebalance task " + stealInfo,
                                                        failures);
            }

        } finally {
            // free the permit in all cases.
            if(logger.isInfoEnabled()) {
                logger.info("Releasing permit for Donor: " + stealInfo.getDonorId());
            }

            rebalancer.releaseRebalancingPermit(stealInfo.getDonorId());
            adminClient.stop();
            adminClient = null;
        }
    }

    private void waitForShutdown() {
        try {
            executors.shutdown();
            executors.awaitTermination(voldemortConfig.getAdminSocketTimeout(), TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.error("Interrupted while awaiting termination for executors.", e);
        }
    }

    @Override
    public void stop() {
        updateStatus("stop() called on rebalance operation !!");
        if(null != adminClient) {
            for(int asyncID: rebalanceStatusList) {
                adminClient.stopAsyncRequest(metadataStore.getNodeId(), asyncID);
            }
        }

        executors.shutdownNow();
    }

    private void rebalanceStore(String storeName,
                                final AdminClient adminClient,
                                RebalancePartitionsInfo stealInfo,
                                boolean isReadOnlyStore) throws Exception {
        logger.info("starting partitions migration for store:" + storeName);
        updateStatus("started");

        List<Integer> partitionList = null;
        if(isReadOnlyStore) {
            partitionList = stealInfo.getStealMasterPartitions();
        } else {
            partitionList = stealInfo.getPartitions();
        }
        int asyncId = adminClient.migratePartitions(stealInfo.getDonorId(),
                                                    metadataStore.getNodeId(),
                                                    storeName,
                                                    partitionList,
                                                    null);
        rebalanceStatusList.add(asyncId);

        if(logger.isDebugEnabled()) {
            logger.debug("waitForCompletion: stealer:" + metadataStore.getNodeId() + ", asyncId:"
                         + asyncId);
        }
        adminClient.waitForCompletion(metadataStore.getNodeId(),
                                      asyncId,
                                      voldemortConfig.getRebalancingTimeoutSec(),
                                      TimeUnit.SECONDS);

        rebalanceStatusList.remove((Object) asyncId);

        if(stealInfo.getDeletePartitionsList().size() > 0 && !isReadOnlyStore) {
            adminClient.deletePartitions(stealInfo.getDonorId(),
                                         storeName,
                                         stealInfo.getDeletePartitionsList(),
                                         null);
            logger.debug("Deleted partitions " + stealInfo.getDeletePartitionsList()
                         + " from donorNode:" + stealInfo.getDonorId() + " for store " + storeName);
        }

        logger.info("partitions migration for store:" + storeName + " completed.");
    }
}

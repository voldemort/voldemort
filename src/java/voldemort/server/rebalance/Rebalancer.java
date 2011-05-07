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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;

import com.google.common.collect.Lists;

/**
 * Service responsible for rebalancing
 * 
 * <br>
 * 
 * Handles two scenarios a) When a new request comes in b) When a rebalancing
 * was shut down and the box was restarted
 * 
 */
public class Rebalancer implements Runnable {

    private final static Logger logger = Logger.getLogger(Rebalancer.class);

    private final MetadataStore metadataStore;
    private final AsyncOperationService asyncService;
    private final VoldemortConfig voldemortConfig;
    private final StoreRepository storeRepository;
    private final Set<Integer> rebalancePermits = Collections.synchronizedSet(new HashSet<Integer>());

    public Rebalancer(StoreRepository storeRepository,
                      MetadataStore metadataStore,
                      VoldemortConfig voldemortConfig,
                      AsyncOperationService asyncService) {
        this.storeRepository = storeRepository;
        this.metadataStore = metadataStore;
        this.asyncService = asyncService;
        this.voldemortConfig = voldemortConfig;
    }

    public AsyncOperationService getAsyncOperationService() {
        return asyncService;
    }

    public void start() {}

    public void stop() {}

    /**
     * This is called only once at startup
     */
    public void run() {}

    /**
     * Acquire a permit for a particular node id so as to allow rebalancing
     * 
     * @param nodeId The id of the node for which we are acquiring a permit
     * @return Returns true if permit acquired, false if the permit is already
     *         held by someone
     */
    public synchronized boolean acquireRebalancingPermit(int nodeId) {
        boolean added = rebalancePermits.add(nodeId);
        if(logger.isInfoEnabled())
            logger.info("Acquiring rebalancing permit for nodeId: " + nodeId + ", returned: "
                        + added);

        return added;
    }

    /**
     * Release the rebalancing permit for a particular node id
     * 
     * @param nodeId The node id whose permit we want to release
     */
    public synchronized void releaseRebalancingPermit(int nodeId) {
        boolean removed = rebalancePermits.remove(nodeId);
        logger.info("Releasing rebalancing permit for nodeId: " + nodeId + ", returned: " + removed);
        if(!removed)
            throw new VoldemortException(new IllegalStateException("Invalid state, must hold a "
                                                                   + "permit to release"));
    }

    /**
     * Support four different stages <br>
     * For normal operation:
     * 
     * <pre>
     * | swapRO | changeClusterMetadata | changeRebalanceState | Order |
     * | f | t | t | cluster -> rebalance | 
     * | f | f | t | rebalance |
     * | t | t | f | cluster -> swap |
     * | t | t | t | cluster -> swap -> rebalance |
     * </pre>
     * 
     * In general we need to do [ cluster change -> swap -> rebalance state
     * change ]
     * 
     * @param cluster Cluster metadata to change
     * @param rebalancePartitionsInfo List of rebalance partitions info
     * @param swapRO Boolean to indicate swapping of RO store
     * @param changeClusterMetadata Boolean to indicate a change of cluster
     *        metadata
     * @param changeRebalanceState Boolean to indicate a change in rebalance
     *        state
     * @param rollback Boolean to indicate that we are rolling back or not
     */
    public void rebalanceStateChange(Cluster cluster,
                                     List<RebalancePartitionsInfo> rebalancePartitionsInfo,
                                     boolean swapRO,
                                     boolean changeClusterMetadata,
                                     boolean changeRebalanceState,
                                     boolean rollback) {
        Cluster currentCluster = metadataStore.getCluster();

        // Variables to track what has completed
        List<RebalancePartitionsInfo> completedRebalancePartitionsInfo = Lists.newArrayList();
        List<String> swappedStoreNames = Lists.newArrayList();
        boolean completedClusterChange = false;

        try {
            // CHANGE CLUSTER METADATA
            if(changeClusterMetadata) {
                changeCluster(cluster);
                completedClusterChange = true;
            }

            // SWAP RO DATA FOR ALL STORES
            if(swapRO) {
                swapROStores(swappedStoreNames, false);
            }

            // CHANGE REBALANCING STATE
            if(changeRebalanceState) {
                if(!rollback) {
                    for(RebalancePartitionsInfo info: rebalancePartitionsInfo) {
                        metadataStore.addRebalancingState(info);
                        completedRebalancePartitionsInfo.add(info);
                    }
                } else {
                    for(RebalancePartitionsInfo info: rebalancePartitionsInfo) {
                        metadataStore.deleteRebalancingState(info);
                        completedRebalancePartitionsInfo.add(info);
                    }
                }
            }
        } catch(VoldemortException e) {

            logger.error("Got exception while changing state, now rolling back changes", e);

            // ROLLBACK CLUSTER CHANGE
            if(completedClusterChange) {
                try {
                    changeCluster(currentCluster);
                } catch(Exception exception) {
                    logger.error("Error while rolling back cluster metadata ", exception);
                }
            }

            // SWAP RO DATA FOR ALL COMPLETED STORES
            if(swappedStoreNames.size() > 0) {
                try {
                    swapROStores(swappedStoreNames, true);
                } catch(Exception exception) {
                    logger.error("Error while swapping back to old state ", exception);
                }
            }

            // CHANGE BACK ALL REBALANCING STATES FOR COMPLETED ONES
            if(completedRebalancePartitionsInfo.size() > 0) {

                if(!rollback) {
                    for(RebalancePartitionsInfo info: completedRebalancePartitionsInfo) {
                        try {
                            metadataStore.deleteRebalancingState(info);
                        } catch(Exception exception) {
                            logger.error("Error while deleting back rebalance info during error rollback "
                                                 + info,
                                         exception);
                        }
                    }
                } else {
                    for(RebalancePartitionsInfo info: completedRebalancePartitionsInfo) {
                        try {
                            metadataStore.addRebalancingState(info);
                        } catch(Exception exception) {
                            logger.error("Error while adding back rebalance info during error rollback "
                                                 + info,
                                         exception);
                        }
                    }
                }

            }

            throw e;
        }

    }

    /**
     * Goes through all the RO Stores in the plan and swaps it
     * 
     * @param swappedStoreNames Names of stores already swapped
     * @param useSwappedStoreNames Swap only the previously swapped stores (
     *        Happens during error )
     */
    private void swapROStores(List<String> swappedStoreNames, boolean useSwappedStoreNames) {

        for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {

            // Only pick up the RO stores
            if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {

                if(useSwappedStoreNames && !swappedStoreNames.contains(storeDef.getName())) {
                    continue;
                }

                ReadOnlyStorageEngine engine = (ReadOnlyStorageEngine) storeRepository.getStorageEngine(storeDef.getName());

                if(engine == null) {
                    throw new VoldemortException("Could not find storage engine for "
                                                 + storeDef.getName() + " to swap ");
                }

                // Time to swap this store - Could have used admin client, but
                // why incur the overhead?
                engine.swapFiles(engine.getCurrentDirPath());

                // Add to list of stores already swapped
                if(!useSwappedStoreNames)
                    swappedStoreNames.add(storeDef.getName());
            }
        }
    }

    /**
     * Updates the cluster metadata
     * 
     * @param cluster The cluster metadata information
     */
    private void changeCluster(final Cluster cluster) {
        metadataStore.writeLock.lock();
        try {
            logger.info("Switching metadata to " + cluster);
            metadataStore.put(MetadataStore.CLUSTER_KEY, cluster);
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

    /**
     * This function is responsible for starting the actual async rebalance
     * operation
     * 
     * <br>
     * 
     * We also assume that the check that this server is in rebalancing state
     * has been done at a higher level
     * 
     * @param stealInfo Partition info to steal
     * @return Returns a id identifying the async operation
     */
    public int rebalanceNode(final RebalancePartitionsInfo stealInfo) {

        final RebalancePartitionsInfo info = metadataStore.getRebalancerState()
                                                          .find(stealInfo.getDonorId());

        // Do we have the plan in the state?
        if(info == null) {
            throw new VoldemortException("Could not find plan " + stealInfo
                                         + " in the server state on " + metadataStore.getNodeId());
        } else if(!info.equals(stealInfo)) {
            // If we do have the plan, is it the same
            throw new VoldemortException("The plan in server state " + info
                                         + " is not the same as the process passed " + stealInfo);
        } else if(!acquireRebalancingPermit(stealInfo.getDonorId())) {
            // Both are same, now try to acquire a lock for the donor node
            throw new AlreadyRebalancingException("Node " + metadataStore.getNodeId()
                                                  + " is already rebalancing from donor "
                                                  + info.getDonorId() + " with info " + info);
        }

        // Acquired lock successfully, start rebalancing...
        int requestId = asyncService.getUniqueRequestId();

        // Why do we pass 'info' instead of 'stealInfo'? So that we can change
        // the state as the stores finish rebalance
        asyncService.submitOperation(requestId, new RebalanceAsyncOperation(this,
                                                                            voldemortConfig,
                                                                            metadataStore,
                                                                            requestId,
                                                                            info));

        return requestId;
    }
}
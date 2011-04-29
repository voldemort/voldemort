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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

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
    private final Set<Integer> rebalancePermits = Collections.synchronizedSet(new HashSet<Integer>());

    public Rebalancer(MetadataStore metadataStore,
                      VoldemortConfig voldemortConfig,
                      AsyncOperationService asyncService) {
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
     * Has rebalancing permit
     * 
     * @param nodeId The node id for which we want to check if we have a permit
     * @return Returns true if permit has been taken
     */
    public synchronized boolean hasRebalancingPermit(int nodeId) {
        boolean contained = rebalancePermits.contains(nodeId);
        if(logger.isInfoEnabled())
            logger.info("hasRebalancingPermit for nodeId: " + nodeId + ", returned: " + contained);

        return contained;
    }

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
            logger.info("acquireRebalancingPermit for nodeId: " + nodeId + ", returned: " + added);

        return added;
    }

    /**
     * Release the rebalancing permit for a particular node id
     * 
     * @param nodeId The node id whose permit we want to release
     */
    public synchronized void releaseRebalancingPermit(int nodeId) {
        boolean removed = rebalancePermits.remove(nodeId);
        logger.info("releaseRebalancingPermit for nodeId: " + nodeId + ", returned: " + removed);
        if(!removed)
            throw new VoldemortException(new IllegalStateException("Invalid state, must hold a "
                                                                   + "permit to release"));
    }

    /**
     * This is triggered only once when a node comes back up from a failure (
     * down node ) and tries to complete rebalancing
     */
    public void run() {
        logger.debug("rebalancer run() called.");
        VoldemortState voldemortState;
        RebalancerState rebalancerState;

        metadataStore.readLock.lock();
        try {
            voldemortState = metadataStore.getServerState();
            rebalancerState = metadataStore.getRebalancerState();
        } catch(Exception e) {
            logger.error("Error determining state", e);
            return;
        } finally {
            metadataStore.readLock.unlock();
        }

        final List<Exception> failures = new ArrayList<Exception>();

        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadataStore.getCluster(),
                                                                       4,
                                                                       2);

        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(voldemortState)) {
            for(RebalancePartitionsInfo stealInfo: rebalancerState.getAll()) {

                if(stealInfo.getUnbalancedStoreList().size() > 0) {
                    try {
                        logger.warn("Rebalance server found incomplete rebalancing attempt, restarting rebalancing task "
                                    + stealInfo);
                        if(stealInfo.getAttempt() < voldemortConfig.getMaxRebalancingAttempt()) {
                            stealInfo.setAttempt(stealInfo.getAttempt() + 1);

                            int rebalanceAsyncId = rebalanceNode(stealInfo);
                            adminClient.waitForCompletion(stealInfo.getStealerId(),
                                                          rebalanceAsyncId,
                                                          voldemortConfig.getRebalancingTimeoutSec(),
                                                          TimeUnit.SECONDS);
                        } else {
                            logger.warn("Rebalancing for rebalancing task " + stealInfo
                                        + " failed multiple times (max. attemps:"
                                        + voldemortConfig.getMaxRebalancingAttempt()
                                        + "), Aborting more trials.");

                            metadataStore.deleteRebalancingState(stealInfo);
                        }
                    } catch(Exception e) {
                        logger.error("RebalanceService rebalancing attempt " + stealInfo
                                     + " failed with exception - " + e.getMessage(), e);
                        failures.add(e);
                    }
                } else {
                    metadataStore.deleteRebalancingState(stealInfo);
                }
            }
        }
    }

    /**
     * Every rebalance state change comes with a couple of requests ( swap ro,
     * change the cluster metadata or change rebalance state ). We need to track
     * the progress of these operations in order to rollback in case of failures
     * 
     */
    private enum RebalanceChangeStatus {
        NOT_REQUIRED,
        STARTED,
        COMPLETED
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
        RebalanceChangeStatus changeClusterMetadataStatus = swapRO ? RebalanceChangeStatus.STARTED
                                                                  : RebalanceChangeStatus.NOT_REQUIRED;
        RebalanceChangeStatus swapROStatus = swapRO ? RebalanceChangeStatus.STARTED
                                                   : RebalanceChangeStatus.NOT_REQUIRED;
        RebalanceChangeStatus changeRebalanceStateStatus = swapRO ? RebalanceChangeStatus.STARTED
                                                                 : RebalanceChangeStatus.NOT_REQUIRED;

        Cluster currentCluster = metadataStore.getCluster();

        try {
            // CHANGE CLUSTER METADATA
            if(changeClusterMetadataStatus == RebalanceChangeStatus.STARTED) {
                changeCluster(cluster);
                changeClusterMetadataStatus = RebalanceChangeStatus.COMPLETED;
            }

            // SWAP RO DATA
            if(swapROStatus == RebalanceChangeStatus.STARTED) {
                // TODO: Put swap code
                swapROStatus = RebalanceChangeStatus.COMPLETED;
            }

            // CHANGE REBALANCING STATE
            if(changeRebalanceStateStatus == RebalanceChangeStatus.STARTED) {
                if(!rollback) {
                    for(RebalancePartitionsInfo info: rebalancePartitionsInfo) {
                        metadataStore.addRebalancingState(info);
                    }
                } else {
                    for(RebalancePartitionsInfo info: rebalancePartitionsInfo) {
                        metadataStore.deleteRebalancingState(info);
                    }
                }
                changeRebalanceStateStatus = RebalanceChangeStatus.COMPLETED;
            }
        } catch(VoldemortException e) {

            // ROLLBACK CLUSTER CHANGE
            if(changeClusterMetadataStatus == RebalanceChangeStatus.COMPLETED) {
                try {
                    changeCluster(currentCluster);
                } catch(Exception exception) {
                    logger.error("Error while rolling back cluster metadata ");
                }
            }

            if(swapROStatus == RebalanceChangeStatus.COMPLETED) {
                try {
                    // TODO:
                } catch(Exception exception) {
                    logger.error("Error while rolling back cluster metadata ");
                }
            }

            if(changeRebalanceStateStatus == RebalanceChangeStatus.COMPLETED) {
                try {
                    // TODO:
                } catch(Exception exception) {
                    logger.error("Error while rolling back cluster metadata ");
                }
            }

            throw e;
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

        // Acquire a lock for the donor node
        if(!acquireRebalancingPermit(stealInfo.getDonorId())) {
            final RebalancerState rebalancerState = metadataStore.getRebalancerState();
            final RebalancePartitionsInfo info = rebalancerState.find(stealInfo.getDonorId());
            if(info != null) {
                throw new AlreadyRebalancingException("Node " + metadataStore.getNodeId()
                                                      + " is already rebalancing from donor "
                                                      + info.getDonorId() + " with info :" + info);
            }
        }

        // Acquired lock successfully, start rebalancing...
        int requestId = asyncService.getUniqueRequestId();

        asyncService.submitOperation(requestId, new RebalanceAsyncOperation(this,
                                                                            voldemortConfig,
                                                                            metadataStore,
                                                                            requestId,
                                                                            stealInfo));

        return requestId;
    }
}
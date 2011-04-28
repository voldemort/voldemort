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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
import voldemort.utils.ByteUtils;
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

        final ConcurrentHashMap<Integer, Map<String, String>> nodeIdsToStoreDirs = new ConcurrentHashMap<Integer, Map<String, String>>();
        final List<Exception> failures = new ArrayList<Exception>();

        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(voldemortState)
           && acquireRebalancingPermit(metadataStore.getNodeId())) {
            // free permit for RebalanceAsyncOperation to acquire
            releaseRebalancingPermit(metadataStore.getNodeId());
            // for (RebalancePartitionsInfo stealInfo :
            // rebalancerState.getAll()) {
            for(final RebalancePartitionsInfoLiveCycle stealInfoLiveCycle: rebalancerState.getAll()) {
                final RebalancePartitionsInfo stealInfo = stealInfoLiveCycle.getRebalancePartitionsInfo();

                // free permit here for rebalanceLocalNode to acquire.
                if(acquireRebalancingPermit(stealInfo.getDonorId())) {
                    releaseRebalancingPermit(stealInfo.getDonorId());

                    try {
                        logger.warn("Rebalance server found incomplete rebalancing attempt, restarting rebalancing task "
                                    + stealInfo);
                        if(stealInfo.getAttempt() < voldemortConfig.getMaxRebalancingAttempt()) {
                            attemptRebalance(stealInfoLiveCycle);
                            // nodeIdsToStoreDirs.put(stealInfo.getDonorId(),
                            // stealInfo.getDonorNodeROStoreToDir());
                            // nodeIdsToStoreDirs.put(stealInfo.getStealerId(),
                            // stealInfo.getStealerNodeROStoreToDir());
                        } else {
                            logger.warn("Rebalancing for rebalancing task " + stealInfo
                                        + " failed multiple times (max. attemps:"
                                        + voldemortConfig.getMaxRebalancingAttempt()
                                        + "), Aborting more trials.");

                            metadataStore.cleanRebalancingState(stealInfo);
                        }
                    } catch(Exception e) {
                        logger.error("RebalanceService rebalancing attempt " + stealInfo
                                     + " failed with exception - " + e.getMessage(), e);
                        failures.add(e);
                    }
                }
            }

            if(failures.size() == 0 && nodeIdsToStoreDirs.size() > 0) {
                ExecutorService swapExecutors = RebalanceUtils.createExecutors(nodeIdsToStoreDirs.size());
                for(final Integer nodeId: nodeIdsToStoreDirs.keySet()) {
                    swapExecutors.submit(new Runnable() {

                        public void run() {
                            Map<String, String> storeDirs = nodeIdsToStoreDirs.get(nodeId);

                            AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                                           metadataStore.getCluster(),
                                                                                           4,
                                                                                           2);
                            try {
                                logger.info("Swapping read-only stores on node " + nodeId);
                                // adminClient.swapStoresAndCleanState(nodeId,
                                // storeDirs);
                                logger.info("Successfully swapped on node " + nodeId);
                            } catch(Exception e) {
                                logger.error("Failed swapping on node " + nodeId, e);
                            } finally {
                                adminClient.stop();
                            }

                        }
                    });
                }

                try {
                    RebalanceUtils.executorShutDown(swapExecutors,
                                                    (int) voldemortConfig.getRebalancingTimeoutSec());
                } catch(Exception e) {
                    logger.error("Interrupted swapping executor - " + e.getMessage(), e);
                    return;
                }
            }
        }
    }

    private void attemptRebalance(final RebalancePartitionsInfoLiveCycle stealInfoLiveCycle) {
        final RebalancePartitionsInfo rebalancePartitionsInfo = stealInfoLiveCycle.getRebalancePartitionsInfo();
        rebalancePartitionsInfo.setAttempt(rebalancePartitionsInfo.getAttempt() + 1);
        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadataStore.getCluster(),
                                                                       4,
                                                                       2);
        try {
            int rebalanceAsyncId = rebalanceNode(stealInfoLiveCycle);
            adminClient.waitForCompletion(rebalancePartitionsInfo.getStealerId(),
                                          rebalanceAsyncId,
                                          voldemortConfig.getRebalancingTimeoutSec(),
                                          TimeUnit.SECONDS);
        } finally {
            adminClient.stop();
            adminClient = null;
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
                        addRebalancingState(info);
                    }
                } else {
                    for(RebalancePartitionsInfo info: rebalancePartitionsInfo) {
                        deleteRebalancingState(info);
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
     * Add the steal information to the rebalancer state
     * 
     * @param stealInfo The steal information to add
     */
    private void addRebalancingState(final RebalancePartitionsInfo stealInfo) {
        metadataStore.writeLock.lock();
        try {
            // Move into rebalancing state
            if(ByteUtils.getString(metadataStore.get(MetadataStore.SERVER_STATE_KEY, null)
                                                .get(0)
                                                .getValue(),
                                   "UTF-8").compareTo(VoldemortState.NORMAL_SERVER.toString()) == 0) {
                metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                                  VoldemortState.REBALANCING_MASTER_SERVER);
            }

            // Add the steal information
            RebalancerState rebalancerState = metadataStore.getRebalancerState();
            if(!rebalancerState.update(stealInfo)) {
                throw new VoldemortException("Could not add steal information since a plan for the same donor node "
                                             + stealInfo.getDonorId() + " already exists");
            }
            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, rebalancerState);
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

    /**
     * Delete the partition steal information from the rebalancer state
     * 
     * @param stealInfo The steal information to delete
     */
    private void deleteRebalancingState(final RebalancePartitionsInfo stealInfo) {
        metadataStore.writeLock.lock();
        try {

            RebalancerState rebalancerState = metadataStore.getRebalancerState();
            rebalancerState.remove(stealInfo);
            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, rebalancerState);

            // Move it back to normal state
            if(rebalancerState.isEmpty()) {
                metadataStore.put(MetadataStore.SERVER_STATE_KEY, VoldemortState.NORMAL_SERVER);
            }
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

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
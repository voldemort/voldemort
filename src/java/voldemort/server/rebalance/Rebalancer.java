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
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.RebalancePartitionsInfoLifeCycleStatus;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

/**
 * Service responsible for rebalancing.
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
                                                    voldemortConfig.getRebalancingTimeout());
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
            int rebalanceAsyncId = rebalanceLocalNode(stealInfoLiveCycle);
            adminClient.waitForCompletion(rebalancePartitionsInfo.getStealerId(),
                                          rebalanceAsyncId,
                                          voldemortConfig.getRebalancingTimeout(),
                                          TimeUnit.SECONDS);
        } finally {
            adminClient.stop();
            adminClient = null;
        }
    }

    /**
     * Rebalance logic at single node level <br>
     * a) Acquires a permit for the donor node <br>
     * b) Changes the state if required <br>
     * c) Starts the async operations <br>
     * 
     * @param stealInfo Rebalance partition information.
     * @return taskId for asynchronous task.
     */
    public int rebalanceLocalNode(final RebalancePartitionsInfoLiveCycle stealInfoLiveCycle) {
        final RebalancePartitionsInfo stealInfo = stealInfoLiveCycle.getRebalancePartitionsInfo();

        if(!acquireRebalancingPermit(stealInfo.getDonorId())) {
            final RebalancerState rebalancerState = metadataStore.getRebalancerState();
            final RebalancePartitionsInfoLiveCycle info = rebalancerState.find(stealInfo.getDonorId());
            if(info != null
               && RebalancePartitionsInfoLifeCycleStatus.RUNNING.equals(info.getStatus())) {
                throw new AlreadyRebalancingException("Node "
                                                      + metadataStore.getCluster()
                                                                     .getNodeById(info.getRebalancePartitionsInfo()
                                                                                      .getStealerId())
                                                      + " is already rebalancing from "
                                                      + info.getRebalancePartitionsInfo()
                                                            .getDonorId() + " rebalanceInfo:"
                                                      + info);
            }
        }

        // check and set State
        checkCurrentState(stealInfoLiveCycle);
        setRebalancingState(stealInfo, RebalancePartitionsInfoLifeCycleStatus.NEW);

        int requestId = asyncService.getUniqueRequestId();

        asyncService.submitOperation(requestId, new RebalanceAsyncOperation(this,
                                                                            voldemortConfig,
                                                                            metadataStore,
                                                                            requestId,
                                                                            stealInfo));

        return requestId;
    }

    protected void setRebalancingState(final RebalancePartitionsInfo stealInfo,
                                       final RebalancePartitionsInfoLifeCycleStatus status) {
        metadataStore.writeLock.lock();
        try {
            metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                              VoldemortState.REBALANCING_MASTER_SERVER);
            RebalancerState rebalancerState = metadataStore.getRebalancerState();
            rebalancerState.add(stealInfo, status);
            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, rebalancerState);
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

    private void checkCurrentState(final RebalancePartitionsInfoLiveCycle stealInfoLiveCycle) {
        metadataStore.readLock.lock();

        if(logger.isDebugEnabled()) {
            logger.debug("Checking current state: " + metadataStore.getServerState().name() + " - "
                         + stealInfoLiveCycle);
        }

        try {
            if(metadataStore.getServerState().equals(VoldemortState.REBALANCING_MASTER_SERVER)) {
                RebalancerState rebalancerState = metadataStore.getRebalancerState();

                final RebalancePartitionsInfo stealInfo = stealInfoLiveCycle.getRebalancePartitionsInfo();
                final int donorId = stealInfo.getDonorId();

                // In case that the server crashed then the metadata will be re
                // initialized
                final RebalancePartitionsInfoLiveCycle previousLiveCycle = rebalancerState.find(donorId);

                if(previousLiveCycle != null
                   && RebalancePartitionsInfoLifeCycleStatus.RUNNING.equals(previousLiveCycle.getStatus())) {
                    throw new VoldemortException("Server " + metadataStore.getNodeId()
                                                 + " is already rebalancing from: "
                                                 + stealInfoLiveCycle
                                                 + " rejecting rebalance request:" + stealInfo);
                } else {
                    if(logger.isInfoEnabled()) {
                        logger.info("Found previous " + previousLiveCycle
                                    + " - about to be executed.");
                    }
                }

            }
        } finally {
            metadataStore.readLock.unlock();
        }
    }
}
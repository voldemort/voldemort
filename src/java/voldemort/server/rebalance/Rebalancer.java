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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

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

    public void start() {}

    public void stop() {}

    private boolean acquireRebalancingPermit(int donorNodeId) {
        return rebalancePermits.add(donorNodeId);
    }

    protected void releaseRebalancingPermit(int donorNodeId) {
        if(!rebalancePermits.remove(donorNodeId))
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

        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(voldemortState)) {
            for(RebalancePartitionsInfo stealInfo: rebalancerState.getAll()) {
                // free permit here for rebalanceLocalNode to acquire.
                if(acquireRebalancingPermit(stealInfo.getDonorId())) {
                    releaseRebalancingPermit(stealInfo.getDonorId());

                    try {
                        logger.warn("Rebalance server found incomplete rebalancing attempt, restarting rebalancing task "
                                    + stealInfo);

                        if(stealInfo.getAttempt() < voldemortConfig.getMaxRebalancingAttempt()) {
                            attemptRebalance(stealInfo);
                            // TODO: Swap the read-only store
                        } else {
                            logger.warn("Rebalancing for rebalancing task " + stealInfo
                                        + " failed multiple times, Aborting more trials.");
                            metadataStore.cleanRebalancingState(stealInfo);
                        }
                    } catch(Exception e) {
                        logger.error("RebalanceService rebalancing attempt " + stealInfo
                                     + " failed with exception", e);
                    }
                }
            }
        }
    }

    private void attemptRebalance(RebalancePartitionsInfo stealInfo) {
        stealInfo.setAttempt(stealInfo.getAttempt() + 1);
        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadataStore.getCluster(),
                                                                       4,
                                                                       2);
        try {
            int rebalanceAsyncId = rebalanceLocalNode(stealInfo);
            adminClient.waitForCompletion(stealInfo.getStealerId(),
                                          rebalanceAsyncId,
                                          voldemortConfig.getAdminSocketTimeout(),
                                          TimeUnit.SECONDS);
        } finally {
            adminClient.stop();
        }
    }

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node
     * {@link RebalancePartitionsInfo#getDonorId()} for partitionList
     * {@link RebalancePartitionsInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.<br>
     * On success clean all states it changed
     * 
     * @param stealInfo Rebalance partition information.
     * @return taskId for asynchronous task.
     */
    public int rebalanceLocalNode(final RebalancePartitionsInfo stealInfo) {
        if(!acquireRebalancingPermit(stealInfo.getDonorId())) {
            RebalancerState rebalancerState = metadataStore.getRebalancerState();
            RebalancePartitionsInfo info = rebalancerState.find(stealInfo.getDonorId());
            if(info != null) {
                throw new AlreadyRebalancingException("Node "
                                                      + metadataStore.getCluster()
                                                                     .getNodeById(info.getStealerId())
                                                      + " is already rebalancing from "
                                                      + info.getDonorId() + " rebalanceInfo:"
                                                      + info);
            }
        }

        // check and set State
        checkCurrentState(stealInfo);
        setRebalancingState(stealInfo);

        // get max parallel store rebalancing allowed
        final int maxParallelStoresRebalancing = (-1 != voldemortConfig.getMaxParallelStoresRebalancing()) ? voldemortConfig.getMaxParallelStoresRebalancing()
                                                                                                          : stealInfo.getUnbalancedStoreList()
                                                                                                                     .size();

        int requestId = asyncService.getUniqueRequestId();

        asyncService.submitOperation(requestId,
                                     new RebalanceAsyncOperation(this,
                                                                 voldemortConfig,
                                                                 metadataStore,
                                                                 requestId,
                                                                 stealInfo,
                                                                 maxParallelStoresRebalancing));

        return requestId;
    }

    protected void setRebalancingState(RebalancePartitionsInfo stealInfo) {
        metadataStore.writeLock.lock();
        try {
            metadataStore.put(MetadataStore.SERVER_STATE_KEY,
                              VoldemortState.REBALANCING_MASTER_SERVER);
            RebalancerState rebalancerState = metadataStore.getRebalancerState();
            rebalancerState.add(stealInfo);
            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, rebalancerState);
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

    private void checkCurrentState(RebalancePartitionsInfo stealInfo) {
        metadataStore.readLock.lock();
        try {
            if(metadataStore.getServerState().equals(VoldemortState.REBALANCING_MASTER_SERVER)) {
                RebalancerState rebalancerState = metadataStore.getRebalancerState();
                RebalancePartitionsInfo info = rebalancerState.find(stealInfo.getDonorId());

                if(info != null) {
                    throw new VoldemortException("Server " + metadataStore.getNodeId()
                                                 + " is already rebalancing from: " + info
                                                 + " rejecting rebalance request:" + stealInfo);
                }
            }
        } finally {
            metadataStore.readLock.unlock();
        }
    }

}
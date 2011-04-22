package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.AlreadyRebalancingException;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Immutable class that executes a {@link RebalancePartitionsInfo} instance.
 * 
 */
class RebalanceTask implements Runnable {

    private static final Logger logger = Logger.getLogger(RebalanceTask.class);

    private final static int INVALID_REBALANCE_ID = -1;

    private final RebalancePartitionsInfo stealInfo;

    private final List<Exception> exceptions = new ArrayList<Exception>();
    private final Set<Integer> participatingNodesId;
    private final CountDownLatch gate;

    public RebalanceTask(final RebalancePartitionsInfo stealInfo,
                         final Set<Integer> participatingNodesId,
                         CountDownLatch gate) {
        this.stealInfo = deepCopy(stealInfo);
        this.participatingNodesId = participatingNodesId;
        this.gate = gate;
    }

    private RebalancePartitionsInfo deepCopy(final RebalancePartitionsInfo stealInfo) {
        String jsonString = stealInfo.toJsonString();
        return RebalancePartitionsInfo.create(jsonString);
    }

    public boolean hasErrors() {
        return !exceptions.isEmpty();
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    /**
     * Does an atomic commit or revert for the intended partitions ownership
     * changes and modifies adminClient with the updatedCluster.<br>
     * Creates new cluster metadata by moving partitions list passed in as
     * parameter rebalanceStealInfo and propagates it to all nodes.<br>
     * Revert all changes if failed to copy on required nodes (stealer and
     * donor).<br>
     * Holds a lock until the commit/revert finishes.
     * 
     * @param stealerNode Node copy data from
     * @param rebalanceStealInfo Current rebalance sub task
     * @throws Exception If we are unable to propagate the cluster definition to
     *         donor and stealer
     */
    private void commitClusterChanges(Node stealerNode, RebalancePartitionsInfo rebalanceStealInfo)
            throws Exception {
        synchronized(adminClient) {
            Cluster currentCluster = adminClient.getAdminClientCluster();
            Node donorNode = currentCluster.getNodeById(rebalanceStealInfo.getDonorId());

            Versioned<Cluster> latestCluster = RebalanceUtils.getLatestCluster(Arrays.asList(donorNode.getId(),
                                                                                             rebalanceStealInfo.getStealerId()),
                                                                               adminClient);
            VectorClock latestClock = (VectorClock) latestCluster.getVersion();

            // apply changes and create new updated cluster.
            // use steal master partitions to update cluster increment clock
            // version on stealerNodeId
            Cluster updatedCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                         stealerNode,
                                                                         donorNode,
                                                                         rebalanceStealInfo.getStealMasterPartitions());
            latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
            try {
                // propagates changes to all nodes.
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                Arrays.asList(stealerNode.getId(),
                                                              rebalanceStealInfo.getDonorId()));

                // set new cluster in adminClient
                adminClient.setAdminClientCluster(updatedCluster);
            } catch(Exception e) {
                // revert cluster changes.
                updatedCluster = currentCluster;
                latestClock.incrementVersion(stealerNode.getId(), System.currentTimeMillis());
                RebalanceUtils.propagateCluster(adminClient,
                                                updatedCluster,
                                                latestClock,
                                                new ArrayList<Integer>());
                logger.error("Exception during comming changes in the cluster.xml - "
                             + e.getMessage(), e);
                throw e;
            }

            adminClient.setAdminClientCluster(updatedCluster);
        }
    }

    private int startNodeRebalancing(RebalancePartitionsInfo rebalanceSubTask) {
        int nTries = 0;
        AlreadyRebalancingException exception = null;

        while(nTries < MAX_TRIES) {
            nTries++;
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("rebalancing node for: " + rebalanceSubTask);
                }
                int asyncOperationId = adminClient.rebalanceNode(rebalanceSubTask);
                if(logger.isDebugEnabled()) {
                    logger.debug("asyncOperationId: " + asyncOperationId + "for : "
                                 + rebalanceSubTask);
                }

                return asyncOperationId;
            } catch(AlreadyRebalancingException e) {
                logger.info("Node " + rebalanceSubTask.getStealerId()
                            + " is currently rebalancing will wait till it finish.");
                adminClient.waitForCompletion(rebalanceSubTask.getStealerId(),
                                              MetadataStore.SERVER_STATE_KEY,
                                              VoldemortState.NORMAL_SERVER.toString(),
                                              rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                              TimeUnit.SECONDS);
                exception = e;
            }
        }

        throw new VoldemortException("Failed to start rebalancing at node "
                                     + rebalanceSubTask.getStealerId() + " with rebalanceInfo:"
                                     + rebalanceSubTask, exception);
    }

    public void run() {
        boolean countDownSuccessful = false;
        int rebalanceAsyncId = INVALID_REBALANCE_ID;
        final int stealerNodeId = stealInfo.getStealerId();
        participatingNodesId.add(stealerNodeId);
        participatingNodesId.add(stealInfo.getDonorId());

        try {
            rebalanceAsyncId = startNodeRebalancing(stealInfo);

            if(logger.isInfoEnabled()) {
                logger.info("Commiting cluster changes, Async ID: " + rebalanceAsyncId
                            + ", rebalancing for stealerNode: " + stealerNodeId
                            + " with rebalanceInfo: " + stealInfo);
            }

            try {
                commitClusterChanges(adminClient.getAdminClientCluster().getNodeById(stealerNodeId),
                                     stealInfo);
            } catch(Exception e) {
                // Only when the commit fails do this.
                if(INVALID_REBALANCE_ID != rebalanceAsyncId) {
                    adminClient.stopAsyncRequest(stealInfo.getStealerId(), rebalanceAsyncId);
                    logger.error("Commiting the cluster has failed. Async ID:" + rebalanceAsyncId);
                }
                throw new VoldemortRebalancingException("Impossible to commit cluster for rebalanceAsyncId: "
                                                        + rebalanceAsyncId);
            }

            // Right after committing the cluster, do a count-down, so the
            // other threads can run without waiting for this to finish.
            // Remember that "adminClient.waitForCompletion" is a blocking
            // call.
            if(gate != null) {
                gate.countDown();
                countDownSuccessful = true;
            }

            if(logger.isInfoEnabled()) {
                logger.info("Waitting ForCompletion for rebalanceAsyncId:" + rebalanceAsyncId);
            }

            adminClient.waitForCompletion(stealInfo.getStealerId(),
                                          rebalanceAsyncId,
                                          rebalanceConfig.getRebalancingClientTimeoutSeconds(),
                                          TimeUnit.SECONDS);
            if(logger.isInfoEnabled()) {
                logger.info("Succesfully finished rebalance for rebalanceAsyncId:"
                            + rebalanceAsyncId);
            }

        } catch(UnreachableStoreException e) {
            exceptions.add(e);
            logger.error("StealerNode " + stealerNodeId
                         + " is unreachable, please make sure it is up and running. - "
                         + e.getMessage(), e);
        } catch(VoldemortRebalancingException e) {
            exceptions.add(e);
            logger.error("Rebalance failed - " + e.getMessage(), e);
        } catch(Exception e) {
            exceptions.add(e);
            logger.error("Rebalance failed - " + e.getMessage(), e);
        } finally {
            // If the any exception happened before the call to count-down
            // then put it down here.
            if(gate != null && !countDownSuccessful) {
                gate.countDown();
            }
        }
    }
}
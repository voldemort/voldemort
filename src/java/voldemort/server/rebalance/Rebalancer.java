package voldemort.server.rebalance;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.RebalanceUtils;

import com.google.common.collect.ImmutableList;

public class Rebalancer implements Runnable {

    private final static Logger logger = Logger.getLogger(Rebalancer.class);

    private final AtomicBoolean rebalancePermit = new AtomicBoolean(false);
    private final MetadataStore metadataStore;
    private final AsyncOperationService asyncService;
    private final VoldemortConfig voldemortConfig;
    private final Map<Integer, AtomicBoolean> rebalancePermitMap = new HashMap<Integer, AtomicBoolean>();

    public Rebalancer(MetadataStore metadataStore,
                      VoldemortConfig voldemortConfig,
                      AsyncOperationService asyncService) {
        this.metadataStore = metadataStore;
        this.asyncService = asyncService;
        this.voldemortConfig = voldemortConfig;
    }

    public void start() {
    // add startup time stuff here.
    }

    public void stop() {}

    private boolean acquireRebalancingPermit(int donorNodeId) {
        synchronized(rebalancePermitMap) {
            if (!rebalancePermitMap.containsKey(donorNodeId)) {
                rebalancePermitMap.put(donorNodeId, new AtomicBoolean(false));
            }
            AtomicBoolean rebalancePermit = rebalancePermitMap.get(donorNodeId);
            if(rebalancePermit.compareAndSet(false, true)) {
                rebalancePermitMap.put(donorNodeId, rebalancePermit);
                return true;
            }
        }

        return false;
    }

    private void releaseRebalancingPermit(int donorNodeId) {
        synchronized(rebalancePermitMap) {
            if(!rebalancePermitMap.get(donorNodeId).compareAndSet(true, false)) {
                throw new VoldemortException("Invalid state rebalancePermit must be true here.");
            }
        }
    }

    public void run() {
        logger.debug("rebalancer run() called.");
        if(VoldemortState.REBALANCING_MASTER_SERVER.equals(metadataStore.getServerState())) {

            // TODO: parallelize this. Right now, when we recover from a crash we still do one 
            // {@link RebalancePartitionsInfo} a time.
            List<RebalancePartitionsInfo> stealInfoList = metadataStore.getRebalancingStealInfo();
            for (RebalancePartitionsInfo stealInfo: stealInfoList) {
                // free permit here for rebalanceLocalNode to acquire.
                if (acquireRebalancingPermit(stealInfo.getDonorId())) {
                    releaseRebalancingPermit(stealInfo.getDonorId());
                    try {
                        logger.warn("Rebalance server found incomplete rebalancing attempt, restarting rebalancing task "
                                    + stealInfo);

                        if(stealInfo.getAttempt() < voldemortConfig.getMaxRebalancingAttempt()) {
                            attemptRebalance(stealInfo);
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
        int rebalanceAsyncId = rebalanceLocalNode(stealInfo);

        adminClient.waitForCompletion(stealInfo.getStealerId(),
                                      rebalanceAsyncId,
                                      voldemortConfig.getAdminSocketTimeout(),
                                      TimeUnit.SECONDS);
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
            for (RebalancePartitionsInfo info: metadataStore.getRebalancingStealInfo()) {
                if (info.getDonorId() == stealInfo.getDonorId()) {
                    throw new AlreadyRebalancingException("Node "
                                                          + metadataStore.getCluster()
                                                                          .getNodeById(info.getStealerId())
                                                          + " is already rebalancing from "
                                                          + info.getDonorId() + " rebalanceInfo:" + info);
                }
            }
        }

        // check and set State
        checkCurrentState(metadataStore, stealInfo);
        setRebalancingState(metadataStore, stealInfo);

        // get max parallel store rebalancing allowed
        final int maxParallelStoresRebalancing = (-1 != voldemortConfig.getMaxParallelStoresRebalancing()) ? voldemortConfig.getMaxParallelStoresRebalancing()
                                                                                                          : stealInfo.getUnbalancedStoreList()
                                                                                                                     .size();

        int requestId = asyncService.getUniqueRequestId();

        asyncService.submitOperation(requestId,
                                    new AsyncOperation(requestId, "Rebalance Operation:"
                                                                  + stealInfo.toString()) {

                                        private List<Integer> rebalanceStatusList = new ArrayList<Integer>();
                                        AdminClient adminClient = null;
                                        final ExecutorService executors = createExecutors(maxParallelStoresRebalancing);

                                        @Override
                                        public void operate() throws Exception {
                                            adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                                               metadataStore.getCluster(),
                                                                                               maxParallelStoresRebalancing * 4,
                                                                                               maxParallelStoresRebalancing * 2);
                                            final List<Exception> failures = new ArrayList<Exception>();
                                            try {
                                                logger.info("starting rebalancing task" + stealInfo);

                                                for(final String storeName: ImmutableList.copyOf(stealInfo.getUnbalancedStoreList())) {

                                                    executors.submit(new Runnable() {

                                                        public void run() {
                                                            try {
                                                                rebalanceStore(storeName,
                                                                               adminClient,
                                                                               stealInfo);

                                                                List<String> tempUnbalancedStoreList = new ArrayList<String>(stealInfo.getUnbalancedStoreList());
                                                                tempUnbalancedStoreList.remove(storeName);
                                                                stealInfo.setUnbalancedStoreList(tempUnbalancedStoreList);
                                                                setRebalancingState(metadataStore,
                                                                                    stealInfo);
                                                            } catch(Exception e) {
                                                                logger.error("rebalanceSubTask:"
                                                                             + stealInfo
                                                                             + " failed for store:"
                                                                             + storeName, e);
                                                                failures.add(e);
                                                            }
                                                        }
                                                    });

                                                }

                                                waitForShutdown();

                                                if(stealInfo.getUnbalancedStoreList().isEmpty()) {
                                                    logger.info("Rebalancer: rebalance "
                                                                + stealInfo
                                                                + " completed successfully.");
                                                    // clean state only if
                                                    // successfull.
                                                    // operation, not all operations.
                                                    metadataStore.cleanRebalancingState(stealInfo);
                                                } else {
                                                    throw new VoldemortRebalancingException("Failed to rebalance task "
                                                                                                    + stealInfo,
                                                                                            failures);
                                                }

                                            } finally {
                                                // free the permit in all cases.
                                                releaseRebalancingPermit(stealInfo.getDonorId());
                                                adminClient.stop();
                                                adminClient = null;
                                            }
                                        }

                                        private void waitForShutdown() {
                                            try {
                                                executors.shutdown();
                                                executors.awaitTermination(voldemortConfig.getAdminSocketTimeout(),
                                                                           TimeUnit.SECONDS);
                                            } catch(InterruptedException e) {
                                                logger.error("Interrupted while awaiting termination for executors.",
                                                             e);
                                            }
                                        }

                                        @Override
                                        public void stop() {
                                            updateStatus("stop() called on rebalance operation !!");
                                            if(null != adminClient) {
                                                for(int asyncID: rebalanceStatusList) {
                                                    adminClient.stopAsyncRequest(metadataStore.getNodeId(),
                                                                                 asyncID);
                                                }
                                            }

                                            executors.shutdownNow();
                                        }

                                        private void rebalanceStore(String storeName,
                                                                    AdminClient adminClient,
                                                                    RebalancePartitionsInfo stealInfo)
                                                throws Exception {
                                            logger.info("starting partitions migration for store:"
                                                        + storeName);
                                            int asyncId = adminClient.migratePartitions(stealInfo.getDonorId(),
                                                                                        metadataStore.getNodeId(),
                                                                                        storeName,
                                                                                        stealInfo.getPartitionList(),
                                                                                        null);
                                            rebalanceStatusList.add(asyncId);

                                            adminClient.waitForCompletion(metadataStore.getNodeId(),
                                                                          asyncId,
                                                                          voldemortConfig.getAdminSocketTimeout(),
                                                                          TimeUnit.SECONDS);

                                            rebalanceStatusList.remove((Object) asyncId);

                                            if(stealInfo.getDeletePartitionsList().size() > 0) {
                                                adminClient.deletePartitions(stealInfo.getDonorId(),
                                                                             storeName,
                                                                             stealInfo.getDeletePartitionsList(),
                                                                             null);
                                                logger.debug("Deleted partitions "
                                                             + stealInfo.getDeletePartitionsList()
                                                             + " from donorNode:"
                                                             + stealInfo.getDonorId()
                                                             + " for store " + storeName);
                                            }

                                            logger.info("partitions migration for store:"
                                                        + storeName + " completed.");
                                        }
                                    });

        return requestId;
    }

    private void setRebalancingState(MetadataStore metadataStore, RebalancePartitionsInfo stealInfo) {
        synchronized (metadataStore.getLock()) {
            metadataStore.put(MetadataStore.SERVER_STATE_KEY, VoldemortState.REBALANCING_MASTER_SERVER);
            List<RebalancePartitionsInfo> stealInfoList = metadataStore.getRebalancingStealInfo();

            int index = -1;
            for (int i=0; i < stealInfoList.size(); i++) {
                if (stealInfoList.get(i).getDonorId() == stealInfo.getDonorId()) {
                    index = i;
                    break;
                }
            }
            if (index != -1) {
                stealInfoList.remove(index);
                stealInfoList.add(index, stealInfo);
            } else {
                stealInfoList.add(stealInfo);
            }

            metadataStore.put(MetadataStore.REBALANCING_STEAL_INFO, stealInfoList);
        }
    }

    private void checkCurrentState(MetadataStore metadataStore, RebalancePartitionsInfo stealInfo) {
        synchronized (metadataStore.getLock()) {
            if(metadataStore.getServerState().equals(VoldemortState.REBALANCING_MASTER_SERVER)
               && Iterables.any(metadataStore.getRebalancingStealInfo(), new DonorIdPredicate(stealInfo.getDonorId())))
                throw new VoldemortException("Server " + metadataStore.getNodeId()
                                             + " is already rebalancing from:"
                                             + Joiner.on(",").join(metadataStore.getRebalancingStealInfo())
                                             + " rejecting rebalance request:" + stealInfo);
        }
    }

    private ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    private final static class DonorIdPredicate implements Predicate<RebalancePartitionsInfo> {
        private final int donorId;

        public DonorIdPredicate(int donorId) {
            this.donorId = donorId;
        }

        public boolean apply(RebalancePartitionsInfo rebalancePartitionsInfo) {
            return donorId == rebalancePartitionsInfo.getDonorId();
        }
    }
}
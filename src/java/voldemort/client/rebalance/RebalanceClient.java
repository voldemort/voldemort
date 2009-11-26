package voldemort.client.rebalance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.rebalancing.RedirectingStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class RebalanceClient {

    private static Logger logger = Logger.getLogger(RebalanceClient.class);

    private final ExecutorService executor;
    private final AdminClient adminClient;

    public RebalanceClient(String bootstrapUrl, RebalanceClientConfig config) {
        this.adminClient = new ProtoBuffAdminClientRequestFormat(bootstrapUrl, config);
        this.executor = Executors.newFixedThreadPool(config.getMaxParallelRebalancingNodes());
    }

    public RebalanceClient(Cluster cluster, RebalanceClientConfig config) {
        this.adminClient = new ProtoBuffAdminClientRequestFormat(cluster, config);
        this.executor = Executors.newFixedThreadPool(config.getMaxParallelRebalancingNodes());
    }

    private Version getIncrementedVersion(int randomNodeId, String clusterStateKey, int requesterID) {
        Version currentVersion = adminClient.getRemoteMetadata(randomNodeId, clusterStateKey)
                                            .getVersion();
        return ((VectorClock) currentVersion).incremented(requesterID, System.currentTimeMillis());
    }

    private void waitToFinish(Semaphore semaphore, int numberOfNodes) {
        try {
            semaphore.acquire(numberOfNodes);
        } catch(InterruptedException e) {
            logger.warn("Thread interrupted while waiting for semaphore to acquire.", e);
        }
    }

    /**
     * Voldemort online rebalancing mechanism. <br>
     * Compares the provided currentCluster and targetCluster and makes a list
     * of partitions need to be transferred <br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param storeName : store to be rebalanced
     * @param currentCluster: currentCluster configuration.
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final String storeName,
                          final Cluster currentCluster,
                          final Cluster targetCluster) {
        // update cluster info for adminClient
        adminClient.setCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        Map<Integer, Map<Integer, List<Integer>>> stealPartitionsMap = RebalanceUtils.getStealPartitionsMap(currentCluster,
                                                                                                            targetCluster);
        logger.info(RebalanceUtils.getStealPartitionsMapAsString(stealPartitionsMap));

        // TODO : rest of me
    }

    /**
     * Rebalance logic at single node level.<br>
     * <imp> should be called by the rebalancing node itself</imp><br>
     * Attempt to rebalance from node {@link RebalanceStealInfo#getDonorId()}
     * for partitionList {@link RebalanceStealInfo#getPartitionList()}
     * <p>
     * Force Sets serverState to rebalancing, Sets stealInfo in MetadataStore,
     * fetch keys from remote node and upsert them locally.
     * 
     * @param metadataStore
     * @param stealInfo
     * @return taskId for asynchronous task.
     */
    public int rebalancePartitionAtNode(MetadataStore metadataStore, RebalanceStealInfo stealInfo) {
        return 1;
    }

    private RebalanceStealInfo updateStealInfo(MetadataStore metadata, RebalanceStealInfo stealInfo) {
        stealInfo.setAttempt(stealInfo.getAttempt() + 1);
        VectorClock currentVersion = (VectorClock) metadata.get(MetadataStore.REBALANCING_STEAL_INFO)
                                                           .get(0)
                                                           .getVersion();
        metadata.put(MetadataStore.REBALANCING_STEAL_INFO,
                     new Versioned<Object>(stealInfo,
                                           currentVersion.incremented(metadata.getNodeId(),
                                                                      System.currentTimeMillis())));
        return stealInfo;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void stop() {
        adminClient.stop();
    }
}

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
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class DefaultRebalanceClient implements RebalanceClient {

    private static Logger logger = Logger.getLogger(DefaultRebalanceClient.class);

    private final String bootstrapURL;
    private final RebalanceClientConfig config;
    private final ExecutorService executor;
    private final AdminClient adminClient;

    public DefaultRebalanceClient(String bootstrapUrl, RebalanceClientConfig config) {
        this.bootstrapURL = bootstrapUrl;
        this.config = config;
        this.adminClient = new ProtoBuffAdminClientRequestFormat(bootstrapURL, config);
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

    public void rebalance(String storeName, Cluster currentCluster, Cluster targetCluster) {
        // update cluster info for adminClient
        adminClient.setCluster(currentCluster);

        if(!RebalanceUtils.getClusterRebalancingToken()) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance sleep and retry ...");
        }

        Map<Integer, Map<Integer, List<Integer>>> stealPartitionsMap = RebalanceUtils.getStealPartitionsMap(currentCluster,
                                                                                                            targetCluster);
        logger.info(RebalanceUtils.getStealPartitionsMapAsString(stealPartitionsMap));

        // TODO
    }

    public void stop() {
        adminClient.stop();
    }
}

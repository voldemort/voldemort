package voldemort.client.rebalance;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
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
        this.executor = Executors.newFixedThreadPool(config.getMaxThreads());
    }

    private boolean getClusterRebalancingToken(int requesterID) {
        logger.info("Starting Cluster Rebalancing token Request.");
        Cluster cluster = adminClient.getCluster();

        // get Current Version.
        int randomNodeId = (int) (Math.random() * cluster.getNumberOfNodes());
        final Version incrementedVersion = getIncrementedVersion(randomNodeId,
                                                                 MetadataStore.CLUSTER_STATE_KEY,
                                                                 requesterID);

        // parallel request is not required but what the hell :)
        final Map<Integer, Boolean> succeededMap = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
        final AtomicInteger succeeded = new AtomicInteger(0);
        final Semaphore semaphore = new Semaphore(cluster.getNumberOfNodes());
        for(final Node node: cluster.getNodes())
            this.executor.submit(new Runnable() {

                public void run() {
                    try {
                        semaphore.acquire();
                        succeededMap.put(node.getId(), false);
                        adminClient.updateRemoteClusterState(node.getId(),
                                                             VoldemortState.REBALANCING_CLUSTER);
                        succeeded.incrementAndGet();
                        succeededMap.put(node.getId(), true);
                    } catch(Exception e) {
                        logger.error("Failed to get rebalancing permit from Node:" + node);
                    } finally {
                        semaphore.release();
                    }
                }
            });

        waitToFinish(semaphore, cluster.getNumberOfNodes());

        int succeedThreshold = (int) (config.getClusterMajorityThresholdPercentage() * cluster.getNumberOfNodes());
        if(succeeded.get() >= succeedThreshold)
            return true;

        // rollback all changes.
        for(Entry<Integer, Boolean> entry: succeededMap.entrySet())
            if(entry.getValue())
                adminClient.updateRemoteClusterState(entry.getKey(),
                                                     VoldemortState.REBALANCING_CLUSTER);

        return false;
    }

    private Version getIncrementedVersion(int randomNodeId, String clusterStateKey, int requesterID) {
        Version currentVersion = adminClient.getRemoteMetadata(randomNodeId, clusterStateKey)
                                            .getVersion();
        return ((VectorClock) currentVersion).incremented(requesterID, System.currentTimeMillis());
    }

    private void resetChangedState(Map<Integer, Boolean> succeededMap,
                                   VoldemortState state,
                                   AdminClient adminClient,
                                   Version version) {
    // do parallel request ?
    }

    private void waitToFinish(Semaphore semaphore, int numberOfNodes) {
        try {
            semaphore.acquire(numberOfNodes);
        } catch(InterruptedException e) {
            logger.warn("Thread interrupted while waiting for semaphore to acquire.", e);
        }
    }

    public void rebalance(int requesterNodeId,
                          String storeName,
                          Cluster currentCluster,
                          Cluster targetCluster) {
        // update cluster info for adminClient
        adminClient.setCluster(currentCluster);

        if(!getClusterRebalancingToken(requesterNodeId)) {
            throw new VoldemortException("Failed to get Cluster permission to rebalance.");
        }

        adminClient.stop();
    }
}

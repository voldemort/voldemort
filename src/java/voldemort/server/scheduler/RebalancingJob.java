package voldemort.server.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

/**
 * A job that checks each key to see if it belongs on the current node. If it
 * does not, it does a PUT on the key to assure that it is written to its proper
 * home and then deletes it from the local node.
 * 
 * @author jay
 * 
 */
public class RebalancingJob implements Runnable {

    private static Logger logger = Logger.getLogger(RebalancingJob.class);

    private final int localNodeId;
    private final RoutingStrategy routingStrategy;
    private final Map<String, StorageEngine<byte[], byte[]>> localEngines;
    private final Map<String, Store<byte[], byte[]>> remoteStores;

    public RebalancingJob(int localNodeId,
                          RoutingStrategy routingStrategy,
                          Map<String, StorageEngine<byte[], byte[]>> engines,
                          Map<String, Store<byte[], byte[]>> remoteStores) {
        this.localNodeId = localNodeId;
        this.localEngines = engines;
        this.remoteStores = remoteStores;
        this.routingStrategy = routingStrategy;
    }

    public void run() {
        logger.info("Rebalancing all keys...");
        int totalRebalanced = 0;
        long start = System.currentTimeMillis();
        for(StorageEngine<byte[], byte[]> engine: localEngines.values()) {
            logger.info("Rebalancing " + engine.getName());
            Store<byte[], byte[]> remote = this.remoteStores.get(engine.getName());
            ClosableIterator<Entry<byte[], Versioned<byte[]>>> iterator = engine.entries();
            int rebalanced = 0;
            long currStart = System.currentTimeMillis();
            while(iterator.hasNext()) {
                Entry<byte[], Versioned<byte[]>> entry = iterator.next();
                if(needsRebalancing(entry.getKey())) {
                    remote.put(entry.getKey(), entry.getValue());
                    engine.delete(entry.getKey(), entry.getValue().getVersion());
                    rebalanced++;
                }
            }
            totalRebalanced += rebalanced;
            long ellapsedSeconds = (System.currentTimeMillis() - currStart) / Time.MS_PER_SECOND;
            logger.info("Rebalancing of store " + engine.getName() + " completed in "
                        + ellapsedSeconds + " seconds.");
            logger.info(rebalanced + " keys rebalanced.");
        }
        long ellapsedSeconds = (System.currentTimeMillis() - start) / Time.MS_PER_SECOND;
        logger.info("Rebalancing complete for all stores in " + ellapsedSeconds + " seconds.");
        logger.info(totalRebalanced + " keys rebalanced in total.");
    }

    private boolean needsRebalancing(byte[] key) {
        List<Node> responsible = routingStrategy.routeRequest(key);
        for(Node n: responsible)
            if(n.getId() == localNodeId)
                return false;
        return true;
    }

}

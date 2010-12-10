package voldemort.server.scheduler.slop;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class RepairJob implements Runnable {

    private final static Logger logger = Logger.getLogger(RepairJob.class.getName());

    public static List<String> blackList = Arrays.asList("mysql", "krati", "read-only");

    private final Semaphore repairPermits;
    private final StoreRepository storeRepo;
    private final MetadataStore metadataStore;

    public RepairJob(StoreRepository storeRepo, MetadataStore metadataStore, Semaphore repairPermits) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.repairPermits = Utils.notNull(repairPermits);
    }

    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            logger.error("Cannot run repair job since cluster is rebalancing");
            return;
        }

        Date startTime = new Date();
        logger.info("Started repair job at " + startTime);

        acquireRepairPermit();
        try {
            // Get routing factory
            RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();

            // Get slop store
            StorageEngine<ByteArray, Slop, byte[]> slopStorageEngine = storeRepo.getSlopStore()
                                                                                .asSlopStore();
            for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {
                if(isWritableStore(storeDef)) {
                    logger.info("Repairing store " + storeDef.getName());
                    StorageEngine<ByteArray, byte[], byte[]> engine = storeRepo.getStorageEngine(storeDef.getName());
                    ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = engine.entries();

                    // Lets generate routing strategy for this storage engine
                    RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                                   metadataStore.getCluster());

                    while(iterator.hasNext()) {
                        Pair<ByteArray, Versioned<byte[]>> keyAndVal;
                        keyAndVal = iterator.next();
                        List<Node> nodes = routingStrategy.routeRequest(keyAndVal.getFirst().get());

                        if(!hasDestination(nodes)) {
                            for(Node node: nodes) {
                                Slop slop = new Slop(storeDef.getName(),
                                                     Slop.Operation.PUT,
                                                     keyAndVal.getFirst(),
                                                     keyAndVal.getSecond().getValue(),
                                                     null,
                                                     node.getId(),
                                                     new Date());
                                Versioned<Slop> slopVersioned = new Versioned<Slop>(slop,
                                                                                    keyAndVal.getSecond()
                                                                                             .getVersion());
                                slopStorageEngine.put(slop.makeKey(), slopVersioned, null);
                            }
                        }
                    }
                    logger.info("Completed store " + storeDef.getName());
                }
            }
        } finally {
            this.repairPermits.release();
            logger.info("Completed repair job started at " + startTime);
        }

    }

    private boolean hasDestination(List<Node> nodes) {
        for(Node node: nodes) {
            if(node.getId() == metadataStore.getNodeId()) {
                return true;
            }
        }
        return false;
    }

    private boolean isWritableStore(StoreDefinition storeDef) {
        if(!storeDef.isView() && !blackList.contains(storeDef.getType())) {
            return true;
        } else {
            return false;
        }
    }

    private void acquireRepairPermit() {
        logger.info("Acquiring lock to perform repair job ");
        try {
            this.repairPermits.acquire();
        } catch(InterruptedException e) {
            throw new IllegalStateException("Repair job interrupted while waiting for permit.", e);
        }
    }

}

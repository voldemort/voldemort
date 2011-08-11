package voldemort.server.scheduler.slop;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
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

import com.google.common.collect.Maps;

public class RepairJob implements Runnable {

    private final static int DELETE_BATCH_SIZE = 1000;
    private final static Logger logger = Logger.getLogger(RepairJob.class.getName());

    public final static List<String> blackList = Arrays.asList("mysql", "krati", "read-only");

    private final Semaphore repairPermits;
    private final StoreRepository storeRepo;
    private final MetadataStore metadataStore;
    private final Map<String, Long> storeStats;
    private final boolean deleteOnly;
    private final int deleteBatchSize;

    public RepairJob(StoreRepository storeRepo,
                     MetadataStore metadataStore,
                     Semaphore repairPermits,
                     boolean deleteOnly,
                     int deleteBatchSize) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.repairPermits = Utils.notNull(repairPermits);
        this.storeStats = Maps.newHashMap();
        this.deleteOnly = deleteOnly;
        this.deleteBatchSize = deleteBatchSize;
    }

    public RepairJob(StoreRepository storeRepo, MetadataStore metadataStore, Semaphore repairPermits) {
        this(storeRepo, metadataStore, repairPermits, true, DELETE_BATCH_SIZE);
    }

    public RepairJob(StoreRepository storeRepo,
                     MetadataStore metadataStore,
                     Semaphore repairPermits,
                     boolean deleteOnly) {
        this(storeRepo, metadataStore, repairPermits, deleteOnly, DELETE_BATCH_SIZE);
    }

    @JmxOperation(description = "Start the Repair Job thread", impact = MBeanOperationInfo.ACTION)
    public void startRepairJob() {
        run();
    }

    @JmxOperation(description = "Get repair slops per store", impact = MBeanOperationInfo.ACTION)
    public long getRepairSlopsPerStore(String storeName) {
        if(!storeStats.containsKey(storeName))
            return 0L;
        return storeStats.get(storeName);
    }

    @JmxGetter(name = "totalRepairSlops", description = "total repair slops")
    public long totalRepairSlops() {
        Long totalRepairSlops = new Long(0);
        for(Long repairSlops: storeStats.values()) {
            totalRepairSlops += repairSlops;
        }
        return totalRepairSlops;
    }

    public void resetStats(Map<String, Long> storeStatistics) {
        storeStats.putAll(storeStatistics);
    }

    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            logger.error("Cannot run repair job since Voldemort server is not in normal state");
            return;
        }

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;

        Date startTime = new Date();
        boolean terminatedEarly = false;
        logger.info("Started repair job at " + startTime);

        Map<String, Long> localStats = Maps.newHashMap();
        for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {
            localStats.put(storeDef.getName(), 0L);
        }

        if(!acquireRepairPermit())
            return;
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
                    iterator = engine.entries();

                    // Lets generate routing strategy for this storage engine
                    RoutingStrategy routingStrategy = routingStrategyFactory.updateRoutingStrategy(storeDef,
                                                                                                   metadataStore.getCluster());
                    long repairSlops = 0L;
                    long numDeletedKeys = 0;
                    long numScannedKeys = 0;
                    while(iterator.hasNext()) {
                        Pair<ByteArray, Versioned<byte[]>> keyAndVal;
                        keyAndVal = iterator.next();
                        List<Node> nodes = routingStrategy.routeRequest(keyAndVal.getFirst().get());

                        if(!hasDestination(nodes)) {
                            if(!deleteOnly) {
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
                                    repairSlops++;
                                }
                            }
                            engine.delete(keyAndVal.getFirst(), keyAndVal.getSecond().getVersion());
                            numDeletedKeys++;
                        }
                        numScannedKeys++;
                        if(numScannedKeys % deleteBatchSize == 0)
                            logger.info("#Scanned:" + numScannedKeys + " #Deleted:"
                                        + numDeletedKeys);
                    }
                    closeIterator(iterator);
                    localStats.put(storeDef.getName(), repairSlops);
                    logger.info("Completed store " + storeDef.getName());
                }
            }
        } catch(Exception e) {
            logger.error(e, e);
            terminatedEarly = true;
        } finally {
            closeIterator(iterator);

            if(!terminatedEarly) {
                resetStats(localStats);
            }
            this.repairPermits.release();
            logger.info("Completed repair job started at " + startTime);
        }

    }

    private void closeIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator) {
        try {
            if(iterator != null)
                iterator.close();
        } catch(Exception e) {
            logger.error("Error in closing iterator", e);
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

    private boolean acquireRepairPermit() {
        logger.info("Acquiring lock to perform repair job ");
        if(this.repairPermits.tryAcquire()) {
            logger.info("Acquired lock to perform repair job ");
            return true;
        } else {
            logger.error("Aborting Repair Job since another instance is already running! ");
            return false;
        }
    }

}

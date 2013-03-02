package voldemort.server.storage;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class RepairJob implements Runnable {

    private final static int DELETE_BATCH_SIZE = 10000;
    private final static Logger logger = Logger.getLogger(RepairJob.class.getName());

    public final static List<String> blackList = Arrays.asList("krati",
                                                               ReadOnlyStorageConfiguration.TYPE_NAME);

    private final ScanPermitWrapper repairPermits;
    private final StoreRepository storeRepo;
    private final MetadataStore metadataStore;
    private final int deleteBatchSize;

    public RepairJob(StoreRepository storeRepo,
                     MetadataStore metadataStore,
                     ScanPermitWrapper repairPermits,
                     int deleteBatchSize) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.repairPermits = Utils.notNull(repairPermits);
        this.deleteBatchSize = deleteBatchSize;
    }

    public RepairJob(StoreRepository storeRepo,
                     MetadataStore metadataStore,
                     ScanPermitWrapper repairPermits) {
        this(storeRepo, metadataStore, repairPermits, DELETE_BATCH_SIZE);
    }

    @JmxOperation(description = "Start the Repair Job thread", impact = MBeanOperationInfo.ACTION)
    public void startRepairJob() {
        run();
    }

    public void run() {

        // don't try to run slop pusher job when rebalancing
        if(!metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            logger.error("Cannot run repair job since Voldemort server is not in normal state");
            return;
        }

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;

        Date startTime = new Date();
        logger.info("Started repair job at " + startTime);

        Map<String, Long> localStats = Maps.newHashMap();
        for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {
            localStats.put(storeDef.getName(), 0L);
        }
        AtomicLong progress = new AtomicLong(0);
        if(!acquireRepairPermit(progress))
            return;
        try {
            // Get routing factory
            RoutingStrategyFactory routingStrategyFactory = new RoutingStrategyFactory();

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
                    while(iterator.hasNext()) {
                        Pair<ByteArray, Versioned<byte[]>> keyAndVal;
                        keyAndVal = iterator.next();
                        List<Node> nodes = routingStrategy.routeRequest(keyAndVal.getFirst().get());

                        if(!hasDestination(nodes)) {
                            engine.delete(keyAndVal.getFirst(), keyAndVal.getSecond().getVersion());
                            numDeletedKeys++;
                        }
                        long itemsScanned = progress.incrementAndGet();
                        if(itemsScanned % deleteBatchSize == 0)
                            logger.info("#Scanned:" + itemsScanned + " #Deleted:" + numDeletedKeys);
                    }
                    closeIterator(iterator);
                    localStats.put(storeDef.getName(), repairSlops);
                    logger.info("Completed store " + storeDef.getName());
                }
            }
        } catch(Exception e) {
            logger.error(e, e);
        } finally {
            closeIterator(iterator);
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

    private boolean acquireRepairPermit(AtomicLong progress) {
        logger.info("Acquiring lock to perform repair job ");
        if(this.repairPermits.tryAcquire(progress)) {
            logger.info("Acquired lock to perform repair job ");
            return true;
        } else {
            logger.error("Aborting Repair Job since another instance is already running! ");
            return false;
        }
    }

}

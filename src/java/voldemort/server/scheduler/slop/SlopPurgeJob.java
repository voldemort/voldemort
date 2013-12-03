package voldemort.server.scheduler.slop;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.StoreRepository;
import voldemort.server.storage.DataMaintenanceJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * Async job to clean up slops accumulated on the nodes, based on the following
 * criteria
 * 
 * 1. Slops destined for a given list of nodes
 * 
 * 2. Slops destined for nodes in a given zone.
 * 
 * 3. Slops destined for a given store.
 * 
 * Note that the slop will be dropped if ANY of three filters match. Also, since
 * the job just makes a single pass over the slop store, and new slops could
 * potentially be coming in continuously, we may need to run this repeatedly to
 * make ABSOLUTELY sure that there are no slops left matching the given filter
 * criteria.
 * 
 */
public class SlopPurgeJob extends DataMaintenanceJob {

    private final static Logger logger = Logger.getLogger(SlopPurgeJob.class.getName());

    private List<Integer> nodesToPurge;
    private int zoneToPurge;
    private List<String> storesToPurge;

    public SlopPurgeJob(StoreRepository storeRepo,
                        MetadataStore metadataStore,
                        ScanPermitWrapper repairPermits,
                        int maxKeysScannedPerSecond) {
        super(storeRepo, metadataStore, repairPermits, maxKeysScannedPerSecond);
    }

    public void setFilter(List<Integer> nodesToPurge, int zoneToPurge, List<String> storesToPurge) {
        this.nodesToPurge = nodesToPurge;
        this.zoneToPurge = zoneToPurge;
        this.storesToPurge = storesToPurge;
    }

    @Override
    public void operate() throws Exception {
        logger.info("Purging slops that match any of the following. 1) Nodes:" + nodesToPurge
                    + ", 2) Zone:" + zoneToPurge + ", 3) Stores:" + storesToPurge);

        SlopStorageEngine slopStorageEngine = storeRepo.getSlopStore();
        StorageEngine<ByteArray, Slop, byte[]> slopStore = slopStorageEngine.asSlopStore();
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = slopStore.entries();
        Set<Integer> nodesInPurgeZone = metadataStore.getCluster().getNodeIdsInZone(zoneToPurge);

        try {
            while(iterator.hasNext()) {

                Pair<ByteArray, Versioned<Slop>> keyAndVal = iterator.next();
                Versioned<Slop> versioned = keyAndVal.getSecond();
                Slop slop = versioned.getValue();

                // Determine if the slop qualifies for purging..
                boolean purge = false;
                if(nodesToPurge.contains(slop.getNodeId())) {
                    purge = true;
                } else if(nodesInPurgeZone.contains(slop.getNodeId())) {
                    purge = true;
                } else if(storesToPurge.contains(slop.getStoreName())) {
                    purge = true;
                }

                // if any one of the filters were met, delete
                if(purge) {
                    numKeysUpdatedThisRun.incrementAndGet();
                    slopStorageEngine.delete(keyAndVal.getFirst(), versioned.getVersion());
                }
                numKeysScannedThisRun.incrementAndGet();
                throttler.maybeThrottle(1);

                if(numKeysScannedThisRun.get() % STAT_RECORDS_INTERVAL == 0) {
                    logger.info("#Scanned:" + numKeysScannedThisRun + " #PurgedSlops:"
                                + numKeysUpdatedThisRun);
                }
            }
        } catch(Exception e) {
            logger.error("Error while purging slops", e);
        }

        logger.info("Completed purging slops. " + "#Scanned:" + numKeysScannedThisRun
                    + " #PurgedSlops:" + numKeysUpdatedThisRun);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public String getJobName() {
        return "SlopPurgeJob";
    }

    @JmxGetter(name = "numSlopsPurges", description = "Returns number of slops purged")
    public synchronized long getSlopsPurged() {
        return totalKeysUpdated + numKeysUpdatedThisRun.get();
    }
}

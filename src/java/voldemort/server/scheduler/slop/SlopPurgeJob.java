package voldemort.server.scheduler.slop;

import org.apache.log4j.Logger;

import voldemort.server.StoreRepository;
import voldemort.server.storage.DataMaintenanceJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.metadata.MetadataStore;

public class SlopPurgeJob extends DataMaintenanceJob {

    private final static Logger logger = Logger.getLogger(SlopPurgeJob.class.getName());

    private int nodeId;
    private int zoneId;

    public SlopPurgeJob(StoreRepository storeRepo,
                        MetadataStore metadataStore,
                        ScanPermitWrapper repairPermits,
                        int maxKeysScannedPerSecond) {
        super(storeRepo, metadataStore, repairPermits, maxKeysScannedPerSecond);
    }

    public void setFilter(int nodeId, int zoneId) {
        this.nodeId = nodeId;
        this.zoneId = zoneId;
    }

    @Override
    public void operate() throws Exception {
        logger.info("Purging slops for node :" + nodeId + "/ zonedId:" + zoneId);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public String getJobName() {
        return "SlopPurgeJob";
    }
}

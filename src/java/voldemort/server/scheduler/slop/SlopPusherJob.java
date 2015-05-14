package voldemort.server.scheduler.slop;

import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * 
 * TODO there is potentially lot more the two slop pushers can share.
 * 
 */
public abstract class SlopPusherJob {

    protected final StoreRepository storeRepo;
    protected final MetadataStore metadataStore;
    protected final FailureDetector failureDetector;
    protected final ScanPermitWrapper repairPermits;
    protected final VoldemortConfig voldemortConfig;

    SlopPusherJob(StoreRepository storeRepo,
                  MetadataStore metadataStore,
                  FailureDetector failureDetector,
                  VoldemortConfig voldemortConfig,
                  ScanPermitWrapper repairPermits) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.repairPermits = Utils.notNull(repairPermits);
        this.failureDetector = failureDetector;
        this.voldemortConfig = voldemortConfig;
    }

    /**
     * A slop is dead if the destination node or the store does not exist
     * anymore on the cluster.
     * 
     * @param slop
     * @return
     */
    protected boolean isSlopDead(Cluster cluster, Set<String> storeNames, Slop slop) {
        // destination node , no longer exists
        if(!cluster.getNodeIds().contains(slop.getNodeId())) {
            return true;
        }

        // destination store, no longer exists
        if(!storeNames.contains(slop.getStoreName())) {
            return true;
        }

        // else. slop is alive
        return false;
    }

    /**
     * Handle slop for nodes that are no longer part of the cluster. It may not
     * always be the case. For example, shrinking a zone or deleting a store.
     */
    protected void handleDeadSlop(SlopStorageEngine slopStorageEngine,
                                  Pair<ByteArray, Versioned<Slop>> keyAndVal) {
        Versioned<Slop> versioned = keyAndVal.getSecond();
        // If configured to delete the dead slop
        if(voldemortConfig.getAutoPurgeDeadSlops()) {
            slopStorageEngine.delete(keyAndVal.getFirst(), versioned.getVersion());

            if(getLogger().isDebugEnabled()) {
                getLogger().debug("Auto purging dead slop :" + versioned.getValue());
            }
        } else {
            // Keep ignoring the dead slops
            if(getLogger().isDebugEnabled()) {
                getLogger().debug("Ignoring dead slop :" + versioned.getValue());
            }
        }
    }

    public abstract Logger getLogger();
}

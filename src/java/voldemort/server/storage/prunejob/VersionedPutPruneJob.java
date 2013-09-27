/*
 * Copyright 2008-2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.server.storage.prunejob;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.StoreRepository;
import voldemort.server.storage.DataMaintenanceJob;
import voldemort.server.storage.KeyLockHandle;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;

import com.google.common.primitives.Ints;

/**
 * Voldemort supports a "versioned" put interface, where the user can provide a
 * vector clock, generated outside of Voldemort. A common practice is to create
 * a vector clock with entries for all current replicas of a key with the
 * timestamp as the value.
 * 
 * For example, if a key replicates to A,B servers, then the put issued at time
 * t1, will have the clock [A:t1, B:t1].
 * 
 * The problem with this approach is that after rebalancing the replicas change
 * and subsequent such "versioned" puts will conflict for sure, with old
 * versions, leading to disk bloat.
 * 
 * For example, if the key now replicates to C,D servers, then the put issued at
 * time t2, will have the clock [C:t2, D:t2]. this conflicts with [A:t1, B:t1]
 * and the space occupied by the old version is never reclaimed
 * 
 * Run this job IF and ONLY IF, you are doing something like above. This job
 * sifts through all the data for a store and fixes multiple versions, by
 * pruning vector clocks to contain only entries for the current replicas.
 * 
 * Ergo, This has the following effect
 * 
 * 1. For keys that were hit with some online traffic during rebalancing, we
 * have multiple versions already. In these cases, it will effectively throw
 * away the old version
 * 
 * 2. For keys that were untouched since rebalancing, there will be just one
 * version on disk and the job will empty out entries in the clock that belong
 * to old replicas, such that the subsequent write will overwrite this version,
 * while reads can still read the old value in the meantime
 * 
 * Caveats/Cornercases:
 * 
 * 1.While fixing a key, the client could write a value in that tiny tiny
 * window. In such a rare event, the client write could be overwritten by the
 * fixer, resulting in loss of that write.
 * 
 * 2. The scan over the database is not guaranteed to hit "new" keys inserted
 * during the run. But, in this case, the new keys will be based off current
 * replicas anyway and we are good.
 * 
 * 
 * NOTE: Voldemort uses "sparse" vector clocks for the regular put interface,
 * that let's Voldemort pick out a vector clock. This job is NOT NECESSARY if
 * you are only doing regular puts. In fact, even if you are using "versioned"
 * puts with "dense" clocks filled with a monotonically increasing number (like
 * timestamp), you are fine.
 * 
 */
@SuppressWarnings("deprecation")
public class VersionedPutPruneJob extends DataMaintenanceJob {

    private final static Logger logger = Logger.getLogger(VersionedPutPruneJob.class.getName());

    private String storeName;

    public VersionedPutPruneJob(StoreRepository storeRepo,
                                MetadataStore metadataStore,
                                ScanPermitWrapper repairPermits,
                                int maxKeysScannedPerSecond) {
        super(storeRepo, metadataStore, repairPermits, maxKeysScannedPerSecond);
    }

    public void setStoreName(String storeName) {
        if(!storeRepo.hasLocalStore(storeName)) {
            throw new VoldemortException("Store " + storeName + " does not exist on the server");
        }
        this.storeName = storeName;
    }

    @Override
    public void operate() throws Exception {
        StoreDefinition storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(metadataStore.getStoreDefList(),
                                                                                   storeName);
        if(storeDef == null) {
            throw new VoldemortException("Unknown store " + storeName);
        }

        if(isWritableStore(storeDef)) {
            // Lets generate routing strategy for this storage engine
            StoreRoutingPlan routingPlan = new StoreRoutingPlan(metadataStore.getCluster(),
                                                                storeDef);
            logger.info("Pruning store " + storeDef.getName());
            StorageEngine<ByteArray, byte[], byte[]> engine = storeRepo.getStorageEngine(storeDef.getName());
            iterator = engine.keys();

            long itemsScanned = 0;
            long numPrunedKeys = 0;
            while(iterator.hasNext()) {
                ByteArray key = iterator.next();

                KeyLockHandle<byte[]> lockHandle = null;
                try {
                    lockHandle = engine.getAndLock(key);
                    List<Versioned<byte[]>> vals = lockHandle.getValues();
                    List<Integer> keyReplicas = routingPlan.getReplicationNodeList(routingPlan.getMasterPartitionId(key.get()));
                    MutableBoolean didPrune = new MutableBoolean(false);
                    List<Versioned<byte[]>> prunedVals = pruneNonReplicaEntries(vals,
                                                                                keyReplicas,
                                                                                didPrune);
                    // Only write something back if some pruning actually
                    // happened. Optimization to reduce load on storage
                    if(didPrune.booleanValue()) {
                        List<Versioned<byte[]>> resolvedVals = VectorClockUtils.resolveVersions(prunedVals);
                        // TODO this is only implemented for BDB for now
                        lockHandle.setValues(resolvedVals);
                        engine.putAndUnlock(key, lockHandle);
                        numPrunedKeys = this.numKeysUpdatedThisRun.incrementAndGet();
                    } else {
                        // if we did not prune, still need to let go of the lock
                        engine.releaseLock(lockHandle);
                    }
                    itemsScanned = this.numKeysScannedThisRun.incrementAndGet();
                    throttler.maybeThrottle(Ints.checkedCast(itemsScanned));
                    if(itemsScanned % STAT_RECORDS_INTERVAL == 0)
                        logger.info("#Scanned:" + itemsScanned + " #Pruned:" + numPrunedKeys);
                } catch(Exception e) {
                    throw e;
                } finally {
                    if(lockHandle != null && !lockHandle.isClosed()) {
                        engine.releaseLock(lockHandle);
                    }
                }
            }
            logger.info("Completed store " + storeDef.getName() + " #Scanned:" + itemsScanned
                        + " #Pruned:" + numPrunedKeys);
        }
    }

    /**
     * Remove all non replica clock entries from the list of versioned values
     * provided
     * 
     * @param vals list of versioned values to prune replicas from
     * @param keyReplicas list of current replicas for the given key
     * @param didPrune flag to mark if we did actually prune something
     * @return pruned list
     */
    public static List<Versioned<byte[]>> pruneNonReplicaEntries(List<Versioned<byte[]>> vals,
                                                                 List<Integer> keyReplicas,
                                                                 MutableBoolean didPrune) {
        List<Versioned<byte[]>> prunedVals = new ArrayList<Versioned<byte[]>>(vals.size());
        for(Versioned<byte[]> val: vals) {
            VectorClock clock = (VectorClock) val.getVersion();
            List<ClockEntry> clockEntries = new ArrayList<ClockEntry>();
            for(ClockEntry clockEntry: clock.getEntries()) {
                if(keyReplicas.contains((int) clockEntry.getNodeId())) {
                    clockEntries.add(clockEntry);
                } else {
                    didPrune.setValue(true);
                }
            }
            prunedVals.add(new Versioned<byte[]>(val.getValue(),
                                                 new VectorClock(clockEntries, clock.getTimestamp())));
        }
        return prunedVals;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected String getJobName() {
        return "prune job";
    }

    @JmxGetter(name = "numKeysPruned", description = "Returns number of keys pruned")
    public synchronized long getKeysPruned() {
        return totalKeysUpdated + numKeysUpdatedThisRun.get();
    }
}

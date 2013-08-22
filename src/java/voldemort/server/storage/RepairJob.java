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
package voldemort.server.storage;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;

import com.google.common.primitives.Ints;

/**
 * This is a background job that should be run after successful rebalancing. The
 * job deletes all data that does not belong to the server.
 * 
 * FIXME VC RepairJob is a non intuitive name. Need to rename this.
 */
public class RepairJob extends DataMaintenanceJob {

    private final static Logger logger = Logger.getLogger(RepairJob.class.getName());

    public RepairJob(StoreRepository storeRepo,
                     MetadataStore metadataStore,
                     ScanPermitWrapper repairPermits,
                     int maxKeysScannedPerSecond) {
        super(storeRepo, metadataStore, repairPermits, maxKeysScannedPerSecond);
    }

    @JmxOperation(description = "Start the Repair Job thread", impact = MBeanOperationInfo.ACTION)
    public void startRepairJob() {
        run();
    }

    @Override
    public void operate() throws Exception {
        for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {
            if(isWritableStore(storeDef)) {
                // Lets generate routing strategy for this storage engine
                StoreRoutingPlan routingPlan = new StoreRoutingPlan(metadataStore.getCluster(),
                                                                    storeDef);
                logger.info("Repairing store " + storeDef.getName());
                StorageEngine<ByteArray, byte[], byte[]> engine = storeRepo.getStorageEngine(storeDef.getName());
                iterator = engine.keys();

                long itemsScanned = 0;
                long numDeletedKeys = 0;
                while(iterator.hasNext()) {
                    ByteArray key = iterator.next();

                    if(!routingPlan.checkKeyBelongsToNode(key.get(), metadataStore.getNodeId())) {
                        /**
                         * Blow away the entire key with all its versions..
                         * FIXME VC MySQL storage engine does not seem to honor
                         * null versions
                         */
                        engine.delete(key, null);
                        numDeletedKeys = this.numKeysUpdatedThisRun.incrementAndGet();
                    }
                    itemsScanned = this.numKeysScannedThisRun.incrementAndGet();
                    // Throttle the itemsScanned
                    throttler.maybeThrottle(Ints.checkedCast(itemsScanned));
                    if(itemsScanned % STAT_RECORDS_INTERVAL == 0) {
                        logger.info("#Scanned:" + itemsScanned + " #Deleted:" + numDeletedKeys);
                    }
                }
                closeIterator(iterator);
                logger.info("Completed store " + storeDef.getName() + " #Scanned:" + itemsScanned
                            + " #Deleted:" + numDeletedKeys);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected String getJobName() {
        return "repair job";
    }

    @JmxGetter(name = "numKeysDeleted", description = "Returns number of keys deleted")
    public synchronized long getKeysDeleted() {
        return totalKeysUpdated + numKeysUpdatedThisRun.get();
    }
}

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

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.StoreRepository;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Utils;

/**
 * Base class for jobs that do some maintenance on the data stored in the server
 * 
 * 
 */
public abstract class DataMaintenanceJob implements Runnable {

    public final static List<String> BLACKLISTED_STORAGE_TYPES = Arrays.asList(ReadOnlyStorageConfiguration.TYPE_NAME);

    protected final static int STAT_RECORDS_INTERVAL = 10000;
    protected final ScanPermitWrapper scanPermits;
    protected final StoreRepository storeRepo;
    protected final MetadataStore metadataStore;
    protected ClosableIterator<ByteArray> iterator = null;
    protected AtomicLong numKeysScannedThisRun;
    protected AtomicLong numKeysUpdatedThisRun;
    protected long totalKeysScanned = 0;
    protected long totalKeysUpdated = 0;
    protected AtomicBoolean isRunning;
    protected final EventThrottler throttler;

    public DataMaintenanceJob(StoreRepository storeRepo,
                              MetadataStore metadataStore,
                              ScanPermitWrapper scanPermits,
                              int maxRatePerSecond) {
        this.storeRepo = storeRepo;
        this.metadataStore = metadataStore;
        this.scanPermits = Utils.notNull(scanPermits);
        this.numKeysScannedThisRun = new AtomicLong(0);
        this.numKeysUpdatedThisRun = new AtomicLong(0);
        this.isRunning = new AtomicBoolean(false);
        this.throttler = new EventThrottler(maxRatePerSecond);
    }

    public DataMaintenanceJob(StoreRepository storeRepo,
                              MetadataStore metadataStore,
                              ScanPermitWrapper scanPermits) {
        this(storeRepo, metadataStore, scanPermits, Integer.MAX_VALUE);
    }

    @Override
    public void run() {
        // don't do maintenance when the server is already not normal
        if(!isServerNormal()) {
            getLogger().error("Cannot run " + getJobName()
                              + " since Voldemort server is not in normal state");
            return;
        }

        isRunning.set(true);
        Date startTime = new Date();
        getLogger().info("Started " + getJobName() + " at " + startTime);

        if(!acquireScanPermit()) {
            isRunning.set(false);
            return;
        }

        // actually operate the job
        try {
            operate();
        } catch(Exception e) {
            getLogger().error("Error running " + getJobName(), e);
        } finally {
            closeIterator(iterator);
            this.scanPermits.release(this.getClass().getCanonicalName());
            resetStats();
            getLogger().info("Completed " + getJobName() + " started at " + startTime);
            isRunning.set(false);
        }
    }

    abstract public void operate() throws Exception;

    abstract protected Logger getLogger();

    abstract protected String getJobName();

    private boolean isServerNormal() {
        return metadataStore.getServerStateUnlocked()
                            .equals(MetadataStore.VoldemortState.NORMAL_SERVER);
    }

    protected boolean isWritableStore(StoreDefinition storeDef) {
        if(!storeDef.isView() && !BLACKLISTED_STORAGE_TYPES.contains(storeDef.getType())) {
            return true;
        } else {
            return false;
        }
    }

    private boolean acquireScanPermit() {
        getLogger().info("Acquiring lock to perform " + getJobName());
        if(this.scanPermits.tryAcquire(this.numKeysScannedThisRun,
                                       this.numKeysUpdatedThisRun,
                                       this.getClass().getCanonicalName())) {
            getLogger().info("Acquired lock to perform " + getJobName());
            return true;
        } else {
            getLogger().error("Aborting " + getJobName()
                              + " since another instance is already running! ");
            return false;
        }
    }

    protected void closeIterator(ClosableIterator<ByteArray> iterator) {
        try {
            if(iterator != null) {
                iterator.close();
                iterator = null;
            }
        } catch(Exception e) {
            getLogger().error("Error in closing iterator", e);
        }
    }

    /**
     * Determines whether this job is already runnning.. Note that this only
     * protects against scheduling the same job twice by mistake from Admin tool
     * (most practical use case)
     * 
     * Note: There can still be a race when two threads find this value to be
     * false and both attempt to execute the job
     * 
     * @return
     */
    public AtomicBoolean getIsRunning() {
        return isRunning;
    }

    @JmxGetter(name = "numKeysScanned", description = "Returns number of keys scanned")
    public synchronized long getKeysScanned() {
        return totalKeysScanned + numKeysScannedThisRun.get();
    }

    protected synchronized void resetStats() {
        totalKeysScanned += numKeysScannedThisRun.get();
        numKeysScannedThisRun.set(0);
        totalKeysUpdated += numKeysUpdatedThisRun.get();
        numKeysUpdatedThisRun.set(0);
    }
}

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

package voldemort.server.scheduler;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Expire old data
 * 
 * 
 */
public class DataCleanupJob<K, V, T> implements Runnable {

    private static final Logger logger = Logger.getLogger(DataCleanupJob.class);

    private final StorageEngine<K, V, T> store;
    private final ScanPermitWrapper cleanupPermits;
    private final long maxAgeMs;
    private final Time time;
    private final EventThrottler throttler;
    private long totalEntriesScanned = 0;
    private AtomicLong scanProgressThisRun;
    private long totalEntriesDeleted = 0;
    private AtomicLong deleteProgressThisRun;
    private MetadataStore metadataStore;

    public DataCleanupJob(StorageEngine<K, V, T> store,
                          ScanPermitWrapper cleanupPermits,
                          long maxAgeMs,
                          Time time,
                          EventThrottler throttler,
                          MetadataStore metadataStore) {
        this.store = Utils.notNull(store);
        this.cleanupPermits = Utils.notNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
        this.throttler = throttler;
        this.scanProgressThisRun = new AtomicLong(0);
        this.deleteProgressThisRun = new AtomicLong(0);
        this.metadataStore = metadataStore;
    }

    private boolean isServerInNormalState() {
        if(metadataStore != null
           && metadataStore.getServerStateUnlocked() != VoldemortState.NORMAL_SERVER) {
            logger.info("Datacleanup on store " + store.getName()
                        + " skipped since server is not normal..");
            return false;
        }
        return true;
    }

    private boolean isServerInOfflineState() {
        if(metadataStore != null
           && metadataStore.getServerStateUnlocked() != VoldemortState.OFFLINE_SERVER) {
            logger.info("Datacleanup on store " + store.getName()
                        + " skipped since server is not offline..");
            return false;
        }
        return true;
    }

    @Override
    public void run() {

        // if the server is neither normal nor offline , skip this run.
        if(!isServerInNormalState() && !isServerInOfflineState()) {
            return;
        }

        acquireCleanupPermit(scanProgressThisRun, deleteProgressThisRun);
        store.beginBatchModifications();

        ClosableIterator<Pair<K, Versioned<V>>> iterator = null;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            long now = time.getMilliseconds();
            iterator = store.entries();

            while(iterator.hasNext()) {
                // check if we have been interrupted
                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Datacleanup job halted.");
                    return;
                }

                final long INETERVAL = 10000;
                long entriesScanned = scanProgressThisRun.get();
                if(entriesScanned % INETERVAL == 0) {
                    if(!isServerInNormalState() && !isServerInOfflineState()) {
                        return;
                    }
                }

                scanProgressThisRun.incrementAndGet();
                Pair<K, Versioned<V>> keyAndVal = iterator.next();
                VectorClock clock = (VectorClock) keyAndVal.getSecond().getVersion();
                if(now - clock.getTimestamp() > maxAgeMs) {
                    store.delete(keyAndVal.getFirst(), clock);
                    final long entriesDeleted = this.deleteProgressThisRun.incrementAndGet();
                    if(logger.isDebugEnabled() && entriesDeleted % INETERVAL == 0) {
                        logger.debug("Deleted item " + this.deleteProgressThisRun.get());
                    }
                }

                // throttle on number of entries.
                throttler.maybeThrottle(1);
            }
            // log the total items scanned, so we will get an idea of data
            // growth in a cheap, periodic way
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; "
                        + this.deleteProgressThisRun.get() + " items deleted. "
                        + scanProgressThisRun.get() + " items scanned");

        } catch(Exception e) {
            logger.error("Error in data cleanup job for store " + store.getName() + ": ", e);
        } finally {
            closeIterator(iterator);
            logger.info("Releasing lock  after data cleanup on \"" + store.getName() + "\".");
            this.cleanupPermits.release(this.getClass().getCanonicalName());
            synchronized(this) {
                totalEntriesScanned += scanProgressThisRun.get();
                scanProgressThisRun.set(0);
                totalEntriesDeleted += deleteProgressThisRun.get();
                deleteProgressThisRun.set(0);
            }
            store.endBatchModifications();
        }
    }

    private void closeIterator(ClosableIterator<Pair<K, Versioned<V>>> iterator) {
        try {
            if(iterator != null)
                iterator.close();
        } catch(Exception e) {
            logger.error("Error in closing iterator " + store.getName() + " ", e);
        }
    }

    private void acquireCleanupPermit(AtomicLong scanProgress, AtomicLong deleteProgress) {
        logger.info("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire(scanProgress, deleteProgress, this.getClass()
                                                                          .getCanonicalName());
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.",
                                            e);
        }
    }

    @JmxGetter(name = "numEntriesScanned", description = "Returns number of entries scanned")
    public synchronized long getEntriesScanned() {
        return totalEntriesScanned + scanProgressThisRun.get();
    }

    @JmxGetter(name = "numEntriesDeleted", description = "Returns number of entries deleted")
    public synchronized long getEntriesDeleted() {
        return totalEntriesDeleted + deleteProgressThisRun.get();
    }
}

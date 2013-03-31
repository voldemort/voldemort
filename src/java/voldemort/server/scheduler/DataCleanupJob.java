/*
 * Copyright 2008-2009 LinkedIn, Inc
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
    private AtomicLong progressThisRun;

    public DataCleanupJob(StorageEngine<K, V, T> store,
                          ScanPermitWrapper cleanupPermits,
                          long maxAgeMs,
                          Time time,
                          EventThrottler throttler) {
        this.store = Utils.notNull(store);
        this.cleanupPermits = Utils.notNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
        this.throttler = throttler;
        this.progressThisRun = new AtomicLong(0);
    }

    public void run() {
        acquireCleanupPermit(progressThisRun);
        store.beginBatchModifications();

        ClosableIterator<Pair<K, Versioned<V>>> iterator = null;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            int deleted = 0;
            long now = time.getMilliseconds();
            iterator = store.entries();

            while(iterator.hasNext()) {
                // check if we have been interrupted
                if(Thread.currentThread().isInterrupted()) {
                    logger.info("Datacleanup job halted.");
                    return;
                }
                progressThisRun.incrementAndGet();
                Pair<K, Versioned<V>> keyAndVal = iterator.next();
                VectorClock clock = (VectorClock) keyAndVal.getSecond().getVersion();
                if(now - clock.getTimestamp() > maxAgeMs) {
                    store.delete(keyAndVal.getFirst(), clock);
                    deleted++;
                    if(deleted % 10000 == 0)
                        logger.debug("Deleted item " + deleted);
                }

                // throttle on number of entries.
                throttler.maybeThrottle(1);
            }
            // log the total items scanned, so we will get an idea of data
            // growth in a cheap, periodic way
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; " + deleted
                        + " items deleted. " + progressThisRun.get() + " items scanned");

        } catch(Exception e) {
            logger.error("Error in data cleanup job for store " + store.getName() + ": ", e);
        } finally {
            closeIterator(iterator);
            logger.info("Releasing lock  after data cleanup on \"" + store.getName() + "\".");
            this.cleanupPermits.release();
            synchronized(this) {
                totalEntriesScanned += progressThisRun.get();
                progressThisRun.set(0);
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

    private void acquireCleanupPermit(AtomicLong progress) {
        logger.info("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire(progress);
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.",
                                            e);
        }
    }

    @JmxGetter(name = "numEntriesScanned", description = "Returns number of entries scanned")
    public synchronized long getEntriesScanned() {
        return totalEntriesScanned + progressThisRun.get();
    }
}

package voldemort.server.scheduler;

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import voldemort.store.Entry;
import voldemort.store.StorageEngine;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * Expire old data
 * 
 * @author jay
 * 
 */
public class DataCleanupJob<K, V> implements Runnable {

    private static final Logger logger = Logger.getLogger(DataCleanupJob.class);

    private final StorageEngine<K, V> store;
    private final Semaphore cleanupPermits;
    private final long maxAgeMs;
    private final Time time;

    public DataCleanupJob(StorageEngine<K, V> store,
                          Semaphore cleanupPermits,
                          long maxAgeMs,
                          Time time) {
        this.store = Objects.nonNull(store);
        this.cleanupPermits = Objects.nonNull(cleanupPermits);
        this.maxAgeMs = maxAgeMs;
        this.time = time;
    }

    public void run() {
        acquireCleanupPermit();
        ClosableIterator<Entry<K, Versioned<V>>> iterator;
        try {
            logger.info("Starting data cleanup on store \"" + store.getName() + "\"...");
            int deleted = 0;
            long now = time.getMilliseconds();
            iterator = store.entries();
            try {
                while(iterator.hasNext()) {
                    // check if we have been interrupted
                    if(Thread.currentThread().isInterrupted()) {
                        logger.info("Datacleanup job halted.");
                        return;
                    }

                    Entry<K, Versioned<V>> entry = iterator.next();
                    VectorClock clock = (VectorClock) entry.getValue().getVersion();
                    if(now - clock.getTimestamp() > maxAgeMs) {
                        store.delete(entry.getKey(), clock);
                        deleted++;
                    }
                }
            } catch(RuntimeException e) {
                iterator.close();
                logger.error("Error during data cleanup", e);
                throw e;
            } finally {
                if(iterator != null)
                    iterator.close();
            }
            logger.info("Data cleanup on store \"" + store.getName() + "\" is complete; " + deleted
                        + " items deleted.");
        } finally {
            this.cleanupPermits.release();
        }
    }

    private void acquireCleanupPermit() {
        logger.debug("Acquiring lock to perform data cleanup on \"" + store.getName() + "\".");
        try {
            this.cleanupPermits.acquire();
        } catch(InterruptedException e) {
            throw new IllegalStateException("Datacleanup interrupted while waiting for cleanup permit.");
        }
    }

}

package voldemort.client;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.versioning.Versioned;

/*
 * The AsyncMetadataVersionManager is used to track the Metadata version on the
 * cluster and if necessary Re-bootstrap the client.
 * 
 * During initialization, it will retrieve the current version of the store (or
 * the entire stores.xml depending upon granularity) and then periodically check
 * whether this has been updated. During init if the initial version turns out
 * to be null, it means that no change has been done to that store since it was
 * created. In this case, we assume version '0'.
 */

public class AsyncMetadataVersionManager implements Runnable {

    private final Logger logger = Logger.getLogger(this.getClass());
    private Versioned<Long> currentVersion;
    private final SystemStore<String, Long> sysStore;
    private final String systemKey = "stores.xml";
    private volatile boolean isRunning;
    private final Callable<Void> storeClientThunk;
    private long asyncMetadataCheckInterval;

    // Random delta generator
    final int DELTA_MAX = 1000;
    Random randomGenerator = new Random(System.currentTimeMillis());

    public AsyncMetadataVersionManager(SystemStore<String, Long> systemStore,
                                       long asyncMetadataCheckInterval,
                                       Callable<Void> storeClientThunk) {
        this(null, systemStore, asyncMetadataCheckInterval, storeClientThunk);
    }

    public AsyncMetadataVersionManager(Versioned<Long> initialVersion,
                                       SystemStore<String, Long> systemStore,
                                       long asyncMetadataCheckInterval,
                                       Callable<Void> storeClientThunk) {
        this.sysStore = systemStore;
        if(initialVersion == null) {
            this.currentVersion = sysStore.getSysStore("stores.xml");

            // If the received store version is null, assume version 0
            if(currentVersion == null)
                currentVersion = new Versioned<Long>((long) 0);
        } else {
            currentVersion = initialVersion;
        }

        // Initialize and start the background check thread
        isRunning = true;

        Thread checkVersionThread = new Thread(this, "AsyncVersionCheckThread");
        checkVersionThread.setDaemon(true);
        checkVersionThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread t, Throwable e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error("Uncaught exception in Metadata Version check thread:", e);
            }
        });

        this.storeClientThunk = storeClientThunk;
        this.asyncMetadataCheckInterval = asyncMetadataCheckInterval;
        checkVersionThread.start();

    }

    public void destroy() {
        isRunning = false;
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted() && isRunning) {
            try {
                Thread.sleep(asyncMetadataCheckInterval);
            } catch(InterruptedException e) {
                break;
            }

            Versioned<Long> newVersion = this.sysStore.getSysStore(systemKey);

            // If version obtained is null, the store is untouched. Continue
            if(newVersion == null) {
                logger.debug("Metadata unchanged after creation ...");
                continue;
            }

            logger.info("MetadataVersion check => Obtained " + systemKey + " version : "
                        + newVersion);

            if(!newVersion.equals(currentVersion)) {
                logger.info("Metadata version mismatch detected.");

                // Determine a random delta delay between 0 to 1000 (ms)
                int delta = randomGenerator.nextInt(DELTA_MAX);

                try {
                    logger.info("Sleeping for delta : " + delta + " (ms) before re-bootstrapping.");
                    Thread.sleep(delta);
                } catch(InterruptedException e) {
                    break;
                }

                // Invoke callback for bootstrap
                try {
                    this.storeClientThunk.call();
                } catch(Exception e) {
                    e.printStackTrace();
                }

                // Update the current version
                currentVersion = newVersion;
            }
        }
    }
}

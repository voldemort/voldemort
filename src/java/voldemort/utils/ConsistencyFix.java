/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Versioned;

public class ConsistencyFix {

    private static final Logger logger = Logger.getLogger(ConsistencyFix.class);

    private final String storeName;
    private final AdminClient adminClient;
    private final StoreInstance storeInstance;
    private final Stats stats;
    private final long perServerIOPSLimit;
    private final ConcurrentMap<Integer, EventThrottler> putThrottlers;

    ConsistencyFix(String url, String storeName, long progressBar, long perServerIOPSLimit) {
        this.storeName = storeName;
        logger.info("Connecting to bootstrap server: " + url);
        this.adminClient = new AdminClient(url, new AdminClientConfig(), 0);
        Cluster cluster = adminClient.getAdminClientCluster();
        logger.info("Cluster determined to be: " + cluster.getName());

        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        List<StoreDefinition> storeDefs = storeDefinitions.getValue();
        StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                          storeName);
        logger.info("Store definition for store " + storeName + " has been determined.");

        storeInstance = new StoreInstance(cluster, storeDefinition);

        stats = new Stats(progressBar);

        this.perServerIOPSLimit = perServerIOPSLimit;
        this.putThrottlers = new ConcurrentHashMap<Integer, EventThrottler>();
    }

    public String getStoreName() {
        return storeName;
    }

    public StoreInstance getStoreInstance() {
        return storeInstance;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public Stats getStats() {
        return stats;
    }

    /**
     * Throttle put (repair) activity per server.
     * 
     * @param nodeId The node for which to possibly throttle put activity.
     */
    public void maybePutThrottle(int nodeId) {
        if(!putThrottlers.containsKey(nodeId)) {
            putThrottlers.putIfAbsent(nodeId, new EventThrottler(perServerIOPSLimit));
        }
        putThrottlers.get(nodeId).maybeThrottle(1);
    }

    /**
     * Status of the repair of a specific "bad key"
     */
    public enum Status {
        SUCCESS("success"),
        BAD_INIT("bad initialization of fix key"),
        FETCH_EXCEPTION("exception during fetch"),
        REPAIR_EXCEPTION("exception during repair");

        private final String name;

        private Status(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public String execute(int parallelism, String badKeyFileIn, String badKeyFileOut) {
        ExecutorService badKeyReaderService;
        ExecutorService badKeyWriterService;
        ExecutorService consistencyFixWorkers;

        // Create BadKeyWriter thread
        BlockingQueue<BadKeyResult> badKeyQOut = new ArrayBlockingQueue<BadKeyResult>(parallelism * 10);
        badKeyWriterService = Executors.newSingleThreadExecutor();
        badKeyWriterService.submit(new BadKeyWriter(badKeyFileOut, badKeyQOut));
        logger.info("Created badKeyWriter.");

        // Create ConsistencyFixWorker thread pool
        BlockingQueue<Runnable> blockingQ = new ArrayBlockingQueue<Runnable>(parallelism);
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        consistencyFixWorkers = new ThreadPoolExecutor(parallelism,
                                                       parallelism,
                                                       0L,
                                                       TimeUnit.MILLISECONDS,
                                                       blockingQ,
                                                       rejectedExecutionHandler);
        logger.info("Created ConsistencyFixWorker pool.");

        // Create BadKeyReader thread
        CountDownLatch allBadKeysReadLatch = new CountDownLatch(1);
        badKeyReaderService = Executors.newSingleThreadExecutor();
        badKeyReaderService.submit(new BadKeyReader(allBadKeysReadLatch,
                                                    badKeyFileIn,
                                                    this,
                                                    consistencyFixWorkers,
                                                    badKeyQOut));
        logger.info("Created badKeyReader.");

        try {
            allBadKeysReadLatch.await();

            badKeyReaderService.shutdown();
            badKeyReaderService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("Bad key reader service has shutdown.");

            consistencyFixWorkers.shutdown();
            consistencyFixWorkers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("All workers have shutdown.");

            // Poison the bad key writer to have it exit.
            badKeyQOut.put(new BadKeyResult());
            badKeyWriterService.shutdown();
            badKeyWriterService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("Bad key writer service has shutdown.");
        } catch(InterruptedException e) {
            logger.warn("InterruptedException caught.");
            if(logger.isDebugEnabled()) {
                e.printStackTrace();
            }
        } finally {
            adminClient.stop();
        }
        return stats.summary();
    }

    /**
     * Type with which to wrap a "bad key" that could not be repaired and so
     * needs to be written to output file. Has a "poison" value to effectively
     * signal end-of-stream.
     */
    public class BadKeyResult {

        private final String keyInHexFormat;
        private final Status fixKeyResult;
        private final boolean poison;

        /**
         * Common case constructor.
         */
        BadKeyResult(String keyInHexFormat, Status fixKeyResult) {
            this.keyInHexFormat = keyInHexFormat;
            this.fixKeyResult = fixKeyResult;
            this.poison = false;
        }

        /**
         * Constructs a "poison" object.
         */
        BadKeyResult() {
            this.keyInHexFormat = null;
            this.fixKeyResult = null;
            this.poison = true;
        }

        public boolean isPoison() {
            return poison;
        }

        public String getKey() {
            return keyInHexFormat;
        }

        public Status getResult() {
            return fixKeyResult;
        }
    }

    public class BadKeyReader implements Runnable {

        private final CountDownLatch latch;
        private final String badKeyFileIn;

        private final ConsistencyFix consistencyFix;
        private final ExecutorService consistencyFixWorkers;
        private final BlockingQueue<BadKeyResult> badKeyQOut;

        private BufferedReader fileReader;

        BadKeyReader(CountDownLatch latch,
                     String badKeyFileIn,
                     ConsistencyFix consistencyFix,
                     ExecutorService consistencyFixWorkers,
                     BlockingQueue<BadKeyResult> badKeyQOut) {
            this.latch = latch;
            this.badKeyFileIn = badKeyFileIn;

            this.consistencyFix = consistencyFix;
            this.consistencyFixWorkers = consistencyFixWorkers;
            this.badKeyQOut = badKeyQOut;

            try {
                fileReader = new BufferedReader(new FileReader(badKeyFileIn));
            } catch(IOException e) {
                Utils.croak("Failure to open input stream: " + e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                int counter = 0;
                for(String line = fileReader.readLine(); line != null; line = fileReader.readLine()) {
                    if(!line.isEmpty()) {
                        counter++;
                        logger.debug("BadKeyReader read line: key (" + line + ") and counter ("
                                     + counter + ")");
                        consistencyFixWorkers.submit(new ConsistencyFixWorker(line,
                                                                              consistencyFix,
                                                                              badKeyQOut));
                    }
                }
            } catch(IOException ioe) {
                logger.warn("IO exception reading badKeyFile " + badKeyFileIn + " : "
                            + ioe.getMessage());
            } finally {
                latch.countDown();
                try {
                    fileReader.close();
                } catch(IOException ioe) {
                    logger.warn("IOException during fileReader.close in BadKeyReader thread.");
                }
            }
        }
    }

    public class BadKeyWriter implements Runnable {

        private final String badKeyFileOut;
        private final BlockingQueue<BadKeyResult> badKeyQOut;

        private BufferedWriter fileWriter = null;

        BadKeyWriter(String badKeyFile, BlockingQueue<BadKeyResult> badKeyQOut) {
            this.badKeyFileOut = badKeyFile;
            this.badKeyQOut = badKeyQOut;

            try {
                fileWriter = new BufferedWriter(new FileWriter(badKeyFileOut));
            } catch(IOException e) {
                Utils.croak("Failure to open output file : " + e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                BadKeyResult badKeyResult = badKeyQOut.take();
                while(!badKeyResult.isPoison()) {
                    logger.debug("BadKeyWriter write key (" + badKeyResult.keyInHexFormat + ")");

                    fileWriter.write("BADKEY," + badKeyResult.keyInHexFormat + ","
                                     + badKeyResult.fixKeyResult.name() + "\n");
                    badKeyResult = badKeyQOut.take();
                }
            } catch(IOException ioe) {
                logger.warn("IO exception reading badKeyFile " + badKeyFileOut + " : "
                            + ioe.getMessage());
            } catch(InterruptedException ie) {
                logger.warn("Interrupted exception during writing of badKeyFile " + badKeyFileOut
                            + " : " + ie.getMessage());
            } finally {
                try {
                    fileWriter.close();
                } catch(IOException ioe) {
                    logger.warn("Interrupted exception during fileWriter.close:" + ioe.getMessage());
                }
            }
        }
    }

    public class Stats {

        final long progressBar;
        long count;
        long failures;
        long lastTimeMs;
        final long startTimeMs;

        Stats(long progressBar) {
            this.progressBar = progressBar;
            this.count = 0;
            this.failures = 0;
            this.lastTimeMs = System.currentTimeMillis();
            this.startTimeMs = lastTimeMs;
        }

        private synchronized String getPrettyQPS(long count, long ms) {
            long periodS = TimeUnit.MILLISECONDS.toSeconds(ms);
            double qps = (count * 1.0 / periodS);
            DecimalFormat df = new DecimalFormat("0.##");
            return df.format(qps);
        }

        public synchronized void incrementCount() {
            count++;
            if(count % progressBar == 0) {
                long nowTimeMs = System.currentTimeMillis();
                logger.info("Bad keys attempted to be processed count = " + count + " ("
                            + getPrettyQPS(progressBar, lastTimeMs - nowTimeMs) + " keys/second)");
                lastTimeMs = nowTimeMs;
            }
        }

        public synchronized void incrementFailures() {
            failures++;
            if(failures % progressBar == 0) {
                logger.info("Bad key failed to process count = " + failures);
            }
        }

        public synchronized String summary() {
            StringBuilder summary = new StringBuilder();
            summary.append("\n\n");
            summary.append("Consistency Fix Summary\n");
            summary.append("-----------------------\n");
            summary.append("Total keys processed: " + count + "\n");
            summary.append("Total keys processed that were not corrected: " + failures + "\n");
            long nowTimeMs = System.currentTimeMillis();

            summary.append("Keys per second processed: "
                           + getPrettyQPS(count, nowTimeMs - startTimeMs) + "\n");
            return summary.toString();
        }
    }
}

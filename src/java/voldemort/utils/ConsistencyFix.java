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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

// TODO: (refactor) Move to new directory voldemort/tools. Also move
// ConsistencyCheck, Rebalance, and possibly other tools (shells and so on).
// This would reduce the amount of different stuff in the utils directory.
public class ConsistencyFix {

    private static final Logger logger = Logger.getLogger(ConsistencyFix.class);

    private final String storeName;
    private final AdminClient adminClient;
    private final StoreInstance storeInstance;
    private final Stats stats;
    private final long perServerQPSLimit;
    private final ConcurrentMap<Integer, EventThrottler> putThrottlers;
    private final boolean dryRun;
    private final boolean parseOnly;

    ConsistencyFix(String url,
                   String storeName,
                   long progressBar,
                   long perServerQPSLimit,
                   boolean dryRun,
                   boolean parseOnly) {
        this.storeName = storeName;
        logger.info("Connecting to bootstrap server: " + url);
        this.adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig(), 0);
        Cluster cluster = adminClient.getAdminClientCluster();
        logger.info("Cluster determined to be: " + cluster.getName());

        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        List<StoreDefinition> storeDefs = storeDefinitions.getValue();
        StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                          storeName);
        logger.info("Store definition for store " + storeName + " has been determined.");

        storeInstance = new StoreInstance(cluster, storeDefinition);

        stats = new Stats(progressBar);

        this.perServerQPSLimit = perServerQPSLimit;
        this.putThrottlers = new ConcurrentHashMap<Integer, EventThrottler>();
        this.dryRun = dryRun;
        this.parseOnly = parseOnly;
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

    public void close() {
        adminClient.close();
    }

    public Stats getStats() {
        return stats;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public boolean isParseOnly() {
        return parseOnly;
    }

    /**
     * Throttle put (repair) activity per server.
     * 
     * @param nodeId The node for which to possibly throttle put activity.
     */
    public void maybePutThrottle(int nodeId) {
        if(!putThrottlers.containsKey(nodeId)) {
            putThrottlers.putIfAbsent(nodeId, new EventThrottler(perServerQPSLimit));
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

    public String execute(int parallelism,
                          String badKeyFileIn,
                          boolean orphanFormat,
                          String badKeyFileOut) {
        ExecutorService badKeyReaderService;
        ExecutorService badKeyWriterService;
        ExecutorService consistencyFixWorkers;

        // Create BadKeyWriter thread
        BlockingQueue<BadKeyStatus> badKeyQOut = new ArrayBlockingQueue<BadKeyStatus>(parallelism * 10);
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
        BadKeyReader badKeyReader = null;
        if(!orphanFormat) {
            badKeyReader = new BadKeyReader(allBadKeysReadLatch,
                                            badKeyFileIn,
                                            this,
                                            consistencyFixWorkers,
                                            badKeyQOut);
        } else {
            badKeyReader = new BadKeyOrphanReader(allBadKeysReadLatch,
                                                  badKeyFileIn,
                                                  this,
                                                  consistencyFixWorkers,
                                                  badKeyQOut);
        }
        badKeyReaderService.submit(badKeyReader);

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
            badKeyQOut.put(new BadKeyStatus());
            badKeyWriterService.shutdown();
            badKeyWriterService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logger.info("Bad key writer service has shutdown.");
        } catch(InterruptedException e) {
            logger.error("InterruptedException caught.");
            if(logger.isDebugEnabled()) {
                e.printStackTrace();
            }
        } finally {
            adminClient.close();
        }

        // Cobble together a status string for overall execution.
        StringBuilder sb = new StringBuilder();
        sb.append("\n\n");
        sb.append("Exit statuses of various threads:\n");
        sb.append("\tBadKeyReader: ");
        if(badKeyReader.hasException()) {
            sb.append("Had exception!\n");
        } else {
            sb.append("OK.\n");
        }
        sb.append("\tBadKeyWriter: ");
        if(badKeyReader.hasException()) {
            sb.append("Had exception!\n");
        } else {
            sb.append("OK.\n");
        }
        sb.append("\n\n");
        sb.append(stats.summary());

        return sb.toString();
    }

    /**
     * Type with which to wrap a "bad key"
     */
    public static class BadKey {

        private final String keyInHexFormat;
        private final String readerInput;

        BadKey(String keyInHexFormat, String readerInput) {
            this.keyInHexFormat = keyInHexFormat;
            this.readerInput = readerInput;
        }

        public String getKeyInHexFormat() {
            return keyInHexFormat;
        }

        public String getReaderInput() {
            return readerInput;
        }
    }

    /**
     * Type with which to wrap a "bad key" that could not be repaired and so
     * needs to be written to output file. Has a "poison" value to effectively
     * signal end-of-stream.
     */
    public static class BadKeyStatus {

        private final BadKey badKey;
        private final Status status;
        private final boolean poison;

        /**
         * Common case constructor.
         */
        BadKeyStatus(BadKey badKey, Status fixKeyResult) {
            this.badKey = badKey;
            this.status = fixKeyResult;
            this.poison = false;
        }

        /**
         * Constructs a "poison" object.
         */
        BadKeyStatus() {
            this.badKey = null;
            this.status = null;
            this.poison = true;
        }

        public boolean isPoison() {
            return poison;
        }

        public BadKey getBadKey() {
            return badKey;
        }

        public Status getStatus() {
            return status;
        }
    }

    public static class BadKeyReader implements Runnable {

        protected final CountDownLatch latch;
        protected final String badKeyFileIn;

        protected final ConsistencyFix consistencyFix;
        protected final ExecutorService consistencyFixWorkers;
        protected final BlockingQueue<BadKeyStatus> badKeyQOut;

        protected BufferedReader fileReader;
        protected boolean hasException;

        BadKeyReader(CountDownLatch latch,
                     String badKeyFileIn,
                     ConsistencyFix consistencyFix,
                     ExecutorService consistencyFixWorkers,
                     BlockingQueue<BadKeyStatus> badKeyQOut) {
            this.latch = latch;
            this.badKeyFileIn = badKeyFileIn;

            this.consistencyFix = consistencyFix;
            this.consistencyFixWorkers = consistencyFixWorkers;
            this.badKeyQOut = badKeyQOut;

            try {
                this.fileReader = new BufferedReader(new FileReader(badKeyFileIn));
            } catch(IOException e) {
                Utils.croak("Failure to open input stream: " + e.getMessage());
            }

            this.hasException = false;
        }

        @Override
        public void run() {
            try {
                int counter = 0;
                for(String keyLine = fileReader.readLine(); keyLine != null; keyLine = fileReader.readLine()) {
                    BadKey badKey = new BadKey(keyLine.trim(), keyLine);
                    if(!keyLine.isEmpty()) {
                        counter++;
                        logger.debug("BadKeyReader read line: key (" + keyLine + ") and counter ("
                                     + counter + ")");
                        if(!consistencyFix.isParseOnly()) {
                            consistencyFixWorkers.submit(new ConsistencyFixWorker(badKey,
                                                                                  consistencyFix,
                                                                                  badKeyQOut));
                        }
                    }
                }
            } catch(IOException ioe) {
                logger.error("IO exception reading badKeyFile " + badKeyFileIn + " : "
                             + ioe.getMessage());
                hasException = true;
            } finally {
                latch.countDown();
                try {
                    fileReader.close();
                } catch(IOException ioe) {
                    logger.warn("IOException during fileReader.close in BadKeyReader thread.");
                }
            }
        }

        boolean hasException() {
            return hasException;
        }
    }

    public static class BadKeyOrphanReader extends BadKeyReader {

        BadKeyOrphanReader(CountDownLatch latch,
                           String badKeyFileIn,
                           ConsistencyFix consistencyFix,
                           ExecutorService consistencyFixWorkers,
                           BlockingQueue<BadKeyStatus> badKeyQOut) {
            super(latch, badKeyFileIn, consistencyFix, consistencyFixWorkers, badKeyQOut);
        }

        /**
         * Parses a "version" string of the following format:
         * 
         * 'version(2:25, 25:2, 29:156) ts:1355451322089'
         * 
         * and converts this parsed value back into a VectorClock type. Note
         * that parsing is white space sensitive. I.e., trim the string first
         * and make skippy sure that the white space matches the above.
         * 
         * This method should not be necessary. VectorClock.toBytes() should be
         * used for serialization, *not* VectorClock.toString(). VectorClocks
         * serialized via toBytes can be deserialized via VectorClock(byte[]).
         * 
         * @param versionString
         * @return
         * @throws IOException
         */
        @Deprecated
        private VectorClock parseVersion(String versionString) throws IOException {
            List<ClockEntry> versions = new ArrayList<ClockEntry>();
            long timestamp = 0;

            String parsed[] = versionString.split(" ts:");
            logger.trace("parsed[0]: " + parsed[0]);
            if(parsed.length != 2) {
                throw new IOException("Could not parse vector clock: " + versionString);
            }
            timestamp = Long.parseLong(parsed[1]);
            // "version("
            // _01234567_
            // => 8 is the magic offset to elide "version("
            // '-1' gets rid of the last ")"
            String clockEntryList = parsed[0].substring(8, parsed[0].length() - 1);
            logger.trace("clockEntryList: <" + clockEntryList + ">");
            String parsedClockEntryList[] = clockEntryList.split(", ");
            for(int i = 0; i < parsedClockEntryList.length; ++i) {
                logger.trace("parsedClockEntry... : <" + parsedClockEntryList[i] + ">");
                String parsedClockEntry[] = parsedClockEntryList[i].split(":");
                if(parsedClockEntry.length != 2) {
                    throw new IOException("Could not parse ClockEntry: <" + parsedClockEntryList[i]
                                          + ">");
                }
                short nodeId = Short.parseShort(parsedClockEntry[0]);
                long version = Long.parseLong(parsedClockEntry[1]);
                logger.trace("clock entry parsed: <" + nodeId + "> : <" + version + ">");
                versions.add(new ClockEntry(nodeId, version));
            }

            return new VectorClock(versions, timestamp);
        }

        @Override
        public void run() {
            try {
                int counter = 0;
                for(String keyNumValsLine = fileReader.readLine(); keyNumValsLine != null; keyNumValsLine = fileReader.readLine()) {
                    String badKeyEntry = keyNumValsLine;

                    String keyNumVals = keyNumValsLine.trim();
                    if(!keyNumVals.isEmpty()) {
                        counter++;
                        String parsed[] = keyNumVals.split(",");
                        if(parsed.length != 2) {
                            throw new IOException("KeyNumVal line did not parse into two elements: "
                                                  + keyNumVals);
                        }
                        logger.trace("parsed[0]: <" + parsed[0] + ">, parsed[1] <" + parsed[1]
                                     + ">");
                        String key = parsed[0];
                        ByteArray keyByteArray = new ByteArray(ByteUtils.fromHexString(key));
                        int numVals = Integer.parseInt(parsed[1]);
                        logger.debug("BadKeyReader read line: key (" + key + ") and counter ("
                                     + counter + ") and numVals is (" + numVals + ")");

                        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>();
                        for(int i = 0; i < numVals; ++i) {
                            String valueVersionLine = fileReader.readLine();
                            badKeyEntry.concat(valueVersionLine);
                            String valueVersion = valueVersionLine.trim();

                            if(valueVersion.isEmpty()) {
                                throw new IOException("ValueVersion line was empty!");
                            }
                            parsed = valueVersion.split(",", 2);
                            if(parsed.length != 2) {
                                throw new IOException("ValueVersion line did not parse into two elements: "
                                                      + valueVersion);
                            }
                            byte[] value = ByteUtils.fromHexString(parsed[0]);
                            VectorClock vectorClock = parseVersion(parsed[1]);

                            values.add(new Versioned<byte[]>(value, vectorClock));
                        }
                        QueryKeyResult queryKeyResult = new QueryKeyResult(keyByteArray, values);
                        if(!consistencyFix.isParseOnly()) {
                            BadKey badKey = new BadKey(key, badKeyEntry);
                            consistencyFixWorkers.submit(new ConsistencyFixWorker(badKey,
                                                                                  consistencyFix,
                                                                                  badKeyQOut,
                                                                                  queryKeyResult));
                        }
                    }
                }
            } catch(Exception e) {
                logger.error("Exception reading badKeyFile " + badKeyFileIn + " : "
                             + e.getMessage());
                hasException = true;
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

    public static class BadKeyWriter implements Runnable {

        private final String badKeyFileOut;
        private final BlockingQueue<BadKeyStatus> badKeyQOut;

        private BufferedWriter fileWriter = null;
        private boolean hasException;

        BadKeyWriter(String badKeyFile, BlockingQueue<BadKeyStatus> badKeyQOut) {
            this.badKeyFileOut = badKeyFile;
            this.badKeyQOut = badKeyQOut;

            try {
                fileWriter = new BufferedWriter(new FileWriter(badKeyFileOut));
            } catch(IOException e) {
                Utils.croak("Failure to open output file : " + e.getMessage());
            }
            this.hasException = false;
        }

        @Override
        public void run() {
            try {
                BadKeyStatus badKeyStatus = badKeyQOut.take();
                while(!badKeyStatus.isPoison()) {
                    logger.debug("BADKEY," + badKeyStatus.getBadKey().getKeyInHexFormat() + ","
                                 + badKeyStatus.getStatus().name() + "\n");

                    fileWriter.write(badKeyStatus.getBadKey().getReaderInput());
                    badKeyStatus = badKeyQOut.take();
                }
            } catch(IOException ioe) {
                logger.error("IO exception writing badKeyFile " + badKeyFileOut + " : "
                             + ioe.getMessage());
                hasException = true;
            } catch(InterruptedException ie) {
                logger.error("Interrupted exception during writing of badKeyFile " + badKeyFileOut
                             + " : " + ie.getMessage());
                hasException = true;
            } finally {
                try {
                    fileWriter.close();
                } catch(IOException ioe) {
                    logger.warn("Interrupted exception during fileWriter.close:" + ioe.getMessage());
                }
            }
        }

        boolean hasException() {
            return hasException;
        }
    }

    public static class Stats {

        final long progressPeriodOps;
        long fixCount;
        long putCount;
        long failures;
        Map<Status, Long> failureDistribution;
        long oveCount; // ObsoleteVersionExceptions
        long lastTimeMs;
        final long startTimeMs;

        /**
         * 
         * @param progressPeriodOps Number of operations between progress bar
         *        updates.
         */
        Stats(long progressPeriodOps) {
            this.progressPeriodOps = progressPeriodOps;
            this.fixCount = 0;
            this.putCount = 0;
            this.failures = 0;
            this.failureDistribution = new HashMap<Status, Long>();
            this.oveCount = 0;
            this.lastTimeMs = System.currentTimeMillis();
            this.startTimeMs = lastTimeMs;
        }

        private synchronized String getPrettyQPS(long count, long ms) {
            long periodS = TimeUnit.MILLISECONDS.toSeconds(ms);
            double qps = (count * 1.0 / periodS);
            DecimalFormat df = new DecimalFormat("0.##");
            return df.format(qps);
        }

        public synchronized void incrementFixCount() {
            fixCount++;
            if(fixCount % progressPeriodOps == 0) {
                long nowTimeMs = System.currentTimeMillis();
                StringBuilder sb = new StringBuilder();
                sb.append("\nConsistencyFix Progress\n");
                sb.append("\tBad keys processed : " + fixCount
                          + " (during this progress period of " + progressPeriodOps + " ops)\n");
                sb.append("\tBad key processing rate : "
                          + getPrettyQPS(progressPeriodOps, nowTimeMs - lastTimeMs)
                          + " bad keys/second)\n");
                sb.append("\tServer-puts issued : " + putCount + " (since fixer started)\n");
                sb.append("\tObsoleteVersionExceptions encountered : " + oveCount
                          + " (since fixer started)\n");
                logger.info(sb.toString());
                lastTimeMs = nowTimeMs;
            }
        }

        public synchronized void incrementPutCount() {
            putCount++;
        }

        public synchronized void incrementObsoleteVersionExceptions() {
            oveCount++;
        }

        public synchronized void incrementFailures(Status status) {
            failures++;
            if(failures % progressPeriodOps == 0) {
                logger.info("Bad key failed to process count = " + failures);
            }
            if(!failureDistribution.containsKey(status)) {
                failureDistribution.put(status, 0L);
            }
            failureDistribution.put(status, failureDistribution.get(status) + 1);
        }

        public synchronized String summary() {
            StringBuilder summary = new StringBuilder();
            summary.append("\n\n");
            summary.append("Consistency Fix Summary\n");
            summary.append("-----------------------\n");
            summary.append("Total bad keys processed: " + fixCount + "\n");
            summary.append("Total server-puts issued: " + putCount + "\n");
            summary.append("Total ObsoleteVersionExceptions encountered: " + oveCount + "\n");
            summary.append("Total keys processed that were not corrected: " + failures + "\n");
            for(Status status: failureDistribution.keySet()) {
                summary.append("\t" + status + " : " + failureDistribution.get(status) + "\n");
            }

            long nowTimeMs = System.currentTimeMillis();
            summary.append("Keys per second processed: "
                           + getPrettyQPS(fixCount, nowTimeMs - startTimeMs) + "\n");

            return summary.toString();
        }
    }
}

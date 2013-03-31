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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;

/**
 * The KeySamplerCLI tool samples keys for every partition for every store on a
 * cluster. A distinct file of sampled keys is generated for each store.
 * 
 * By default, the "first" key of each partition is sampled. Optional arguments
 * control sampling more keys per partition.
 */
public class KeySamplerCLI {

    private static Logger logger = Logger.getLogger(KeySamplerCLI.class);

    private final static int DEFAULT_NODE_PARALLELISM = 8;
    private final static int DEFAULT_RECORDS_PER_PARTITION = 0; // INF
    private final static int DEFAULT_KEYS_PER_SECOND_LIMIT = 200;
    private final static int DEFAULT_PROGRESS_PERIOD_OPS = 1000;

    private final AdminClient adminClient;
    private final Cluster cluster;
    private final List<StoreDefinition> storeDefinitions;
    private final Map<String, StringBuilder> storeNameToKeyStringsMap;

    private final String outDir;

    private final List<Integer> partitionIds;

    private final ExecutorService nodeSamplerService;
    private final int recordsPerPartition;
    private final int keysPerSecondLimit;
    private final int progressPeriodOps;

    public KeySamplerCLI(String url,
                         String outDir,
                         List<String> storeNames,
                         List<Integer> partitionIds,
                         int nodeParallelism,
                         int recordsPerPartition,
                         int keysPerSecondLimit,
                         int progressPeriodOps) {
        if(logger.isInfoEnabled()) {
            logger.info("Connecting to bootstrap server: " + url);
        }
        this.adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());
        this.cluster = adminClient.getAdminClientCluster();
        this.storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0).getValue();
        this.storeNameToKeyStringsMap = new HashMap<String, StringBuilder>();
        for(StoreDefinition storeDefinition: storeDefinitions) {
            String storeName = storeDefinition.getName();
            if(storeNames != null) {
                if(!storeNames.contains(storeName)) {
                    logger.debug("Will not sample store "
                                 + storeName
                                 + " since it is not in list of storeNames provided on command line.");
                    continue;
                }
            }
            this.storeNameToKeyStringsMap.put(storeName, new StringBuilder());
        }

        if(storeNames != null) {
            List<String> badStoreNames = new LinkedList<String>();
            for(String storeName: storeNames) {
                if(!this.storeNameToKeyStringsMap.keySet().contains(storeName)) {
                    badStoreNames.add(storeName);
                }
            }
            if(badStoreNames.size() > 0) {
                Utils.croak("Some storeNames provided on the command line were not found on this cluster: "
                            + badStoreNames);
            }
        }

        this.outDir = outDir;

        this.partitionIds = partitionIds;

        this.nodeSamplerService = Executors.newFixedThreadPool(nodeParallelism);
        this.recordsPerPartition = recordsPerPartition;
        this.keysPerSecondLimit = keysPerSecondLimit;
        this.progressPeriodOps = progressPeriodOps;
    }

    public boolean sampleStores() {
        for(StoreDefinition storeDefinition: storeDefinitions) {
            if(storeNameToKeyStringsMap.keySet().contains(storeDefinition.getName())) {
                if(!sampleStore(storeDefinition)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static class NodeSampleResult {

        public final boolean success;
        public final String keysString;

        NodeSampleResult(boolean success, String keysString) {
            this.success = success;
            this.keysString = keysString;
        }
    }

    public class NodeSampler implements Callable<NodeSampleResult> {

        private final Node node;
        private final StoreDefinition storeDefinition;
        private final EventThrottler throttler;

        NodeSampler(Node node, StoreDefinition storeDefinition) {
            this.node = node;
            this.storeDefinition = storeDefinition;
            this.throttler = new EventThrottler(keysPerSecondLimit);
        }

        @Override
        public NodeSampleResult call() throws Exception {
            String storeName = storeDefinition.getName();
            StringBuilder hexKeysString = new StringBuilder();
            String nodeTag = node.getId() + " [" + node.getHost() + "]";

            List<Integer> nodePartitionIds = new ArrayList<Integer>(node.getPartitionIds());
            if(partitionIds != null) {
                nodePartitionIds.retainAll(partitionIds);
                if(nodePartitionIds.size() == 0) {
                    logger.info("No partitions to sample for store '" + storeName + "' on node "
                                + nodeTag);
                    return new NodeSampleResult(true, hexKeysString.toString());
                }
            }

            String infoTag = "store " + storeName + ", partitionIDs " + nodePartitionIds
                             + " on node " + nodeTag;
            logger.info("Starting sample --- " + infoTag);

            long startTimeMs = System.currentTimeMillis();

            try {
                Iterator<ByteArray> fetchIterator;
                fetchIterator = adminClient.bulkFetchOps.fetchKeys(node.getId(),
                                                                   storeName,
                                                                   nodePartitionIds,
                                                                   null,
                                                                   true,
                                                                   recordsPerPartition);
                long keyCount = 0;
                while(fetchIterator.hasNext()) {
                    ByteArray key = fetchIterator.next();
                    String hexKeyString = ByteUtils.toHexString(key.get());
                    hexKeysString.append(hexKeyString + "\n");
                    keyCount++;

                    throttler.maybeThrottle(1);

                    if(0 == keyCount % progressPeriodOps) {
                        if(logger.isInfoEnabled()) {
                            long durationS = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()
                                                                             - startTimeMs);
                            logger.info(infoTag + " --- " + keyCount + " keys sampled in "
                                        + durationS + " seconds.");
                        }
                    }
                }

                long expectedKeyCount = recordsPerPartition * node.getPartitionIds().size();
                if(keyCount < expectedKeyCount) {
                    logger.warn("Fewer keys (" + keyCount + ") than expected (" + expectedKeyCount
                                + ") returned --- " + infoTag);
                } else if(keyCount < recordsPerPartition) {
                    logger.warn("More keys (" + keyCount + ") than expected (" + expectedKeyCount
                                + ") returned --- " + infoTag);
                }

                logger.info("Finished sample --- " + infoTag);
                return new NodeSampleResult(true, hexKeysString.toString());
            } catch(VoldemortException ve) {
                logger.error("Failed to sample --- " + infoTag + " --- VoldemortException caught ("
                             + ve.getMessage() + ") caused by (" + ve.getCause().getMessage() + ")");
                throw ve;
            }
        }
    }

    public boolean sampleStore(StoreDefinition storeDefinition) {
        String storeName = storeDefinition.getName();
        String fileName = outDir + System.getProperty("file.separator") + storeName + ".keys";

        File file = new File(fileName);
        if(file.exists()) {
            logger.warn("Key file " + fileName + " already exists. Skipping sampling store "
                        + storeName + ".");
            return true;
        }

        Writer keyWriter = null;
        try {
            keyWriter = new FileWriter(file);

            Map<Node, Future<NodeSampleResult>> results = new HashMap<Node, Future<NodeSampleResult>>();
            for(Node node: cluster.getNodes()) {
                Future<NodeSampleResult> future = nodeSamplerService.submit(new NodeSampler(node,
                                                                                            storeDefinition));
                results.put(node, future);
            }

            boolean success = true;
            for(Node node: cluster.getNodes()) {
                Future<NodeSampleResult> future = results.get(node);
                if(!success) {
                    future.cancel(true);
                    continue;
                }

                try {
                    NodeSampleResult nodeSampleResult = future.get();
                    if(nodeSampleResult.success) {
                        keyWriter.write(nodeSampleResult.keysString);
                    } else {
                        success = false;
                        logger.error("Sampling on node " + node.getHost() + " of store "
                                     + storeDefinition.getName() + " failed.");
                    }
                } catch(ExecutionException ee) {
                    success = false;
                    logger.error("Encountered an execution exception on node " + node.getHost()
                                 + " while sampling " + storeName + ": " + ee.getMessage());
                    ee.printStackTrace();
                } catch(InterruptedException ie) {
                    success = false;
                    logger.error("Waiting for node " + node.getHost() + " to be sampled for store "
                                 + storeName + ", but was interrupted: " + ie.getMessage());
                }
            }
            return success;
        } catch(IOException e) {
            logger.error("IOException encountered for store " + storeName + " : " + e.getMessage());
            return false;
        } finally {
            try {
                keyWriter.close();
            } catch(IOException e) {
                logger.error("IOException caught while trying to close keyWriter for store "
                             + storeName + " : " + e.getMessage());
            }
        }
    }

    public void stop() {
        if(adminClient != null) {
            adminClient.close();
        }
        nodeSamplerService.shutdown();
    }

    /**
     * Return args parser
     * 
     * @return program parser
     * */
    private static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("out-dir",
                       "[REQUIRED] Directory in which to output the key files (named \"{storeName}.keys\".")
              .withRequiredArg()
              .describedAs("outputDirectory")
              .ofType(String.class);
        parser.accepts("store-names",
                       "Store names to sample. Comma delimited list or singleton. [Default: ALL]")
              .withRequiredArg()
              .describedAs("storeNames")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("partition-ids",
                       "Partition IDs to sample for each store. Comma delimited list or singleton. [Default: ALL]")
              .withRequiredArg()
              .describedAs("partitionIds")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("parallelism",
                       "Number of nodes to sample in parallel. [Default: "
                               + DEFAULT_NODE_PARALLELISM + " ]")
              .withRequiredArg()
              .describedAs("storeParallelism")
              .ofType(Integer.class);
        parser.accepts("records-per-partition",
                       "Number of keys sampled per partition. [Default: INF]")
              .withRequiredArg()
              .describedAs("recordsPerPartition")
              .ofType(Integer.class);
        parser.accepts("keys-per-second-limit",
                       "Number of keys sampled per second limit. [Default: "
                               + DEFAULT_KEYS_PER_SECOND_LIMIT + " ]")
              .withRequiredArg()
              .describedAs("keysPerSecondLimit")
              .ofType(Integer.class);
        parser.accepts("progress-period-ops",
                       "Number of operations between progress info is displayed. [Default: "
                               + DEFAULT_PROGRESS_PERIOD_OPS + " ]")
              .withRequiredArg()
              .describedAs("progressPeriodOps")
              .ofType(Integer.class);
        return parser;
    }

    /**
     * Print Usage to STDOUT
     */
    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("KeySamplerCLI Tool\n");
        help.append("  Sample keys from store-partitions. Output keys per store.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <bootstrap-url>\n");
        help.append("    --out-dir <outputDirectory>\n");
        help.append("  Optional:\n");
        help.append("    --store-names <storeName>[,<storeName>...]\n");
        help.append("    --partition-ids <partitionId>[,<partitionId>...]\n");
        help.append("    --parallelism <nodeParallelism>\n");
        help.append("    --records-per-partition <recordsPerPartition>\n");
        help.append("    --keys-per-second-limit <keysPerSecondLimit>\n");
        help.append("    --progress-period-ops <progressPeriodOps>\n");
        help.append("    --help\n");
        help.append("  Notes:\n");
        help.append("    To select ALL storeNames or partitionIds, you must\n");
        help.append("    not specify the pertinent optional argument.\n");
        help.append("    To select INF records per partitoin, either do not\n");
        help.append("    specify the argument, or specify a value <= 0.\n");
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = null;
        OptionSet options = null;
        try {
            parser = getParser();
            options = parser.parse(args);
        } catch(OptionException oe) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
            return;
        }

        /* validate options */
        if(options.hasArgument("help")) {
            parser.printHelpOn(System.out);
            printUsage();
            return;
        }
        if(!options.hasArgument("url") || !options.hasArgument("out-dir")) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Missing a required argument.");
            return;
        }

        String url = (String) options.valueOf("url");

        String outDir = (String) options.valueOf("out-dir");
        Utils.mkdirs(new File(outDir));

        List<String> storeNames = null;
        if(options.hasArgument("store-names")) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) options.valuesOf("store-names");
            storeNames = list;
        }

        List<Integer> partitionIds = null;
        if(options.hasArgument("partition-ids")) {
            @SuppressWarnings("unchecked")
            List<Integer> list = (List<Integer>) options.valuesOf("partition-ids");
            partitionIds = list;
        }

        Integer nodeParallelism = DEFAULT_NODE_PARALLELISM;
        if(options.hasArgument("parallelism")) {
            nodeParallelism = (Integer) options.valueOf("parallelism");
        }

        Integer recordsPerPartition = DEFAULT_RECORDS_PER_PARTITION;
        if(options.hasArgument("records-per-partition")) {
            recordsPerPartition = (Integer) options.valueOf("records-per-partition");
        }

        Integer keysPerSecondLimit = DEFAULT_KEYS_PER_SECOND_LIMIT;
        if(options.hasArgument("keys-per-second-limit")) {
            keysPerSecondLimit = (Integer) options.valueOf("keys-per-second-limit");
        }
        System.err.println("throttle: " + keysPerSecondLimit);

        Integer progressPeriodOps = DEFAULT_PROGRESS_PERIOD_OPS;
        if(options.hasArgument("progress-period-ops")) {
            progressPeriodOps = (Integer) options.valueOf("progress-period-ops");
        }

        KeySamplerCLI sampler = new KeySamplerCLI(url,
                                                  outDir,
                                                  storeNames,
                                                  partitionIds,
                                                  nodeParallelism,
                                                  recordsPerPartition,
                                                  keysPerSecondLimit,
                                                  progressPeriodOps);
        try {
            if(!sampler.sampleStores()) {
                logger.error("Some stores were not successfully sampled.");
            }
        } finally {
            sampler.stop();
        }
    }
}

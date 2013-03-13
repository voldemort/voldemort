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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    private final static int DEFAULT_MAX_RECORDS = 1;

    private final AdminClient adminClient;
    private final Cluster cluster;
    private final List<StoreDefinition> storeDefinitions;
    private final Map<String, StringBuilder> storeNameToKeyStringsMap;

    private final String outDir;
    private final ExecutorService nodeSamplerService;

    private final int maxRecords;

    public KeySamplerCLI(String url, String outDir, int nodeParallelism, int maxRecords) {
        if(logger.isInfoEnabled()) {
            logger.info("Connecting to bootstrap server: " + url);
        }
        this.adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());
        this.cluster = adminClient.getAdminClientCluster();
        this.storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0).getValue();
        this.storeNameToKeyStringsMap = new HashMap<String, StringBuilder>();
        for(StoreDefinition storeDefinition: storeDefinitions) {
            this.storeNameToKeyStringsMap.put(storeDefinition.getName(), new StringBuilder());
        }

        this.outDir = outDir;

        this.nodeSamplerService = Executors.newFixedThreadPool(nodeParallelism);

        this.maxRecords = maxRecords;
    }

    public boolean sampleStores() {
        for(StoreDefinition storeDefinition: storeDefinitions) {
            boolean success = sampleStore(storeDefinition);
            if(!success) {
                return false;
            }
        }
        return true;
    }

    public static class NodeSampleResult {

        public final boolean success;
        public final String keyString;

        NodeSampleResult(boolean success, String keyString) {
            this.success = success;
            // TODO: keysString versus keyString
            this.keyString = keyString;
        }
    }

    public class NodeSampler implements Callable<NodeSampleResult> {

        private final Node node;
        private final StoreDefinition storeDefinition;

        NodeSampler(Node node, StoreDefinition storeDefinition) {
            this.node = node;
            this.storeDefinition = storeDefinition;
        }

        @Override
        public NodeSampleResult call() throws Exception {
            boolean success = false;

            String storeName = storeDefinition.getName();
            StringBuilder hexKeyStrings = new StringBuilder();

            for(int partitionId: node.getPartitionIds()) {
                success = false;

                // TODO: real per-server throttling and/or make '100' a command
                // line argument.

                // Simple, lame throttling since thread is going at same node
                // repeatedly
                try {
                    Thread.sleep(100);
                } catch(InterruptedException e) {
                    logger.warn("Sleep throttling interrupted : " + e.getMessage());
                    e.printStackTrace();
                }

                String infoTag = "store " + storeName + ", partitionID " + partitionId
                                 + " on node " + node.getId() + " [" + node.getHost() + "]";
                logger.info("Starting sample --- " + infoTag);

                List<Integer> singlePartition = new ArrayList<Integer>();
                singlePartition.add(partitionId);

                // Wrap fetchKeys in backoff-and-retry loop
                int attempts = 0;
                int backoffMs = 1000;
                while(attempts < 5 && !success) {
                    try {
                        Iterator<ByteArray> fetchIterator;
                        fetchIterator = adminClient.bulkFetchOps.fetchKeys(node.getId(),
                                                                           storeName,
                                                                           singlePartition,
                                                                           null,
                                                                           true,
                                                                           maxRecords);
                        int keyCount = 0;
                        while(fetchIterator.hasNext()) {
                            ByteArray key = fetchIterator.next();
                            String hexKeyString = ByteUtils.toHexString(key.get());
                            hexKeyStrings.append(hexKeyString + "\n");
                            keyCount++;
                        }
                        if(keyCount < maxRecords) {
                            logger.warn("Fewer keys (" + keyCount + ") than requested ("
                                        + maxRecords + ") returned --- " + infoTag);
                        } else if(keyCount < maxRecords) {
                            logger.warn("More keys (" + keyCount + ") than requested ("
                                        + maxRecords + ") returned --- " + infoTag);
                        }
                        success = true;
                    } catch(VoldemortException ve) {
                        logger.warn("Caught VoldemortException and will retry (" + infoTag + "): "
                                    + ve.getMessage() + " --- " + ve.getCause().getMessage());
                        try {
                            Thread.sleep(backoffMs);
                            backoffMs *= 2;
                        } catch(InterruptedException e) {
                            logger.warn("Backoff-and-retry sleep interrupted : " + e.getMessage());
                            e.printStackTrace();
                            break;
                        }
                    }
                }
                if(!success) {
                    logger.error("Failed to sample --- " + infoTag);
                    break;
                }
            }
            return new NodeSampleResult(success, hexKeyStrings.toString());
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
                        keyWriter.write(nodeSampleResult.keyString);
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
        parser.accepts("parallelism",
                       "Number of nodes to sample in parallel. [Default: "
                               + DEFAULT_NODE_PARALLELISM + " ]")
              .withRequiredArg()
              .describedAs("storeParallelism")
              .ofType(Integer.class);
        parser.accepts("max-records",
                       "Number of keys sampled per partitoin. [Default: " + DEFAULT_MAX_RECORDS
                               + " ]")
              .withRequiredArg()
              .describedAs("maxRecords")
              .ofType(Integer.class);
        return parser;
    }

    /**
     * Print Usage to STDOUT
     */
    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("KeySamplerCLI Tool\n");
        help.append("  Find one key from each store-partition. Output keys per store.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <bootstrap-url>\n");
        help.append("    --out-dir <outputDirectory>\n");
        help.append("  Optional:\n");
        help.append("    --parallelism <nodeParallelism>\n");
        help.append("    --max-records <maxRecords>\n");
        help.append("    --help\n");
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    // TODO: (if needed) Add a "stores" option so that a subset of stores can be
    // done instead of all stores one-by-one.

    // TODO: (if needed) Add a "partitions" option so that a subset of
    // partitions can be done instead of all partitions.

    public static void main(String[] args) throws Exception {
        OptionSet options = null;
        try {
            options = getParser().parse(args);
        } catch(OptionException oe) {
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
            return;
        }

        /* validate options */
        if(options.hasArgument("help")) {
            printUsage();
            return;
        }
        if(!options.hasArgument("url") || !options.hasArgument("out-dir")) {
            printUsageAndDie("Missing a required argument.");
            return;
        }

        String url = (String) options.valueOf("url");

        String outDir = (String) options.valueOf("out-dir");
        Utils.mkdirs(new File(outDir));

        Integer nodeParallelism = DEFAULT_NODE_PARALLELISM;
        if(options.hasArgument("parallelism")) {
            nodeParallelism = (Integer) options.valueOf("parallelism");
        }

        Integer maxRecords = DEFAULT_MAX_RECORDS;
        if(options.hasArgument("max-records")) {
            maxRecords = (Integer) options.valueOf("max-records");
        }

        // TODO: Assuming "right thing" happens server-side, then do not need
        // the below warning...
        logger.warn("This tool is hard-coded to take advantage of servers that "
                    + "use PID style layout of data in BDB. "
                    + "Use fo this tool against other types of servers is undefined.");

        try {
            KeySamplerCLI sampler = new KeySamplerCLI(url, outDir, nodeParallelism, maxRecords);
            try {
                if(!sampler.sampleStores()) {
                    logger.error("Some stores were not successfully sampled.");
                }
            } finally {
                sampler.stop();
            }

        } catch(Exception e) {
            Utils.croak("Exception during key sampling: " + e.getMessage());
        }

    }
}

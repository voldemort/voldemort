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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.ReadRepairer;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ConsistencyFix {

    private static AdminClient adminClient = null;
    private static StoreInstance storeInstance = null;

    private static void init(Options options) {
        System.out.println("Connecting to bootstrap server: " + options.url);
        adminClient = new AdminClient(options.url, new AdminClientConfig(), 0);
        Cluster cluster = adminClient.getAdminClientCluster();
        System.out.println("Cluster determined to be: " + cluster.getName());

        System.out.println("Determining store definition for store: " + options.storeName);
        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        List<StoreDefinition> storeDefs = storeDefinitions.getValue();
        StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                          options.storeName);
        System.out.println("Store definition determined.");

        storeInstance = new StoreInstance(cluster, storeDefinition);
    }

    private static void stop() {
        adminClient.stop();
    }

    public static String getStoreName() {
        return storeInstance.getStoreDefinition().getName();
    }

    public static int getMasterPartitionId(String keyInHexFormat) throws DecoderException {
        byte[] key = Hex.decodeHex(keyInHexFormat.toCharArray());
        return storeInstance.getMasterPartitionId(key);
    }

    public static void printUsage() {
        System.out.println("Required arguments: \n" + "\t--url <url>\n" + "\t--store <storeName>\n"
                           + "\t--bad-key-file-in <FileNameOfInputListOfKeysToFix>\n"
                           + "\t--bad-key-file-out<FileNameOfOutputListOfKeysNotFixed>)\n");
    }

    public static void printUsage(String errMessage) {
        System.err.println("Error: " + errMessage);
        printUsage();
        System.exit(1);
    }

    private static class Options {

        public final static int defaultParallelism = 2;

        public String url = null;
        public String storeName = null;
        public String badKeyFileIn = null;
        public String badKeyFileOut = null;
        public int parallelism = 0;
        public boolean verbose = false;
    }

    /**
     * All the logic for parsing and validating options.
     * 
     * @param args
     * @return A struct containing validated options.
     * @throws IOException
     */
    private static ConsistencyFix.Options parseArgs(String[] args) {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url")
              .withRequiredArg()
              .describedAs("The bootstrap url.")
              .ofType(String.class);
        parser.accepts("store")
              .withRequiredArg()
              .describedAs("The store name.")
              .ofType(String.class);
        /*-
        parser.accepts("keys")
              .withRequiredArg()
              .withValuesSeparatedBy(',')
              .describedAs("List of keys. "
                           + "Each key must be in hexadecimal format. "
                           + "Each key must be separated only by a comma ',' without any white space.")
              .ofType(String.class);
         */
        parser.accepts("bad-key-file-in")
              .withRequiredArg()
              .describedAs("Name of bad-key-file-in. " + "Each key must be in hexadecimal format. "
                           + "Each key must be on a separate line in the file. ")
              .ofType(String.class);
        parser.accepts("bad-key-file-out")
              .withRequiredArg()
              .describedAs("Name of bad-key-file-out. "
                           + "Keys that are not mae consistent are output to this file.")
              .ofType(String.class);
        parser.accepts("parallelism")
              .withOptionalArg()
              .describedAs("Number of read and to repair in parallel. "
                           + "Up to 2X this value requests outstanding simultaneously. "
                           + "[Default value: " + Options.defaultParallelism + "]")
              .ofType(Integer.class);

        parser.accepts("verbose", "verbose");
        OptionSet optionSet = parser.parse(args);

        if(optionSet.hasArgument("help")) {
            try {
                parser.printHelpOn(System.out);
            } catch(IOException e) {
                e.printStackTrace();
            }
            printUsage();
            System.exit(0);
        }
        if(!optionSet.hasArgument("url")) {
            printUsage("Missing required 'url' argument.");
        }
        if(!optionSet.hasArgument("store")) {
            printUsage("Missing required 'store' argument.");
        }
        if(!optionSet.has("bad-key-file-in")) {
            printUsage("Missing required 'bad-key-file-in' argument.");
        }
        if(!optionSet.has("bad-key-file-out")) {
            printUsage("Missing required 'bad-key-file-out' argument.");
        }

        Options options = new Options();

        options.url = (String) optionSet.valueOf("url");
        options.storeName = (String) optionSet.valueOf("store");
        options.badKeyFileOut = (String) optionSet.valueOf("bad-key-file-out");
        options.badKeyFileIn = (String) optionSet.valueOf("bad-key-file-in");
        options.parallelism = Options.defaultParallelism;
        if(optionSet.has("parallelism")) {
            options.parallelism = (Integer) optionSet.valueOf("parallelism");
        }

        if(optionSet.has("verbose")) {
            options.verbose = true;
        }

        return options;
    }

    static public class BadKeyInput {

        private final String keyInHexFormat;
        private final boolean poison;

        BadKeyInput(String keyInHexFormat) {
            this.keyInHexFormat = keyInHexFormat;
            this.poison = false;
        }

        BadKeyInput() {
            this.keyInHexFormat = null;
            this.poison = true;
        }

        boolean isPoison() {
            return poison;
        }

        String getKey() {
            return keyInHexFormat;
        }
    }

    static class BadKeyReader implements Runnable {

        private final String badKeyFileIn;
        private final BlockingQueue<BadKeyInput> badKeyQIn;
        private BufferedReader fileReader;

        BadKeyReader(String badKeyFileIn, BlockingQueue<BadKeyInput> badKeyQIn) {
            this.badKeyFileIn = badKeyFileIn;
            this.badKeyQIn = badKeyQIn;
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
                        System.out.println("BadKeyReader read line: key (" + line
                                           + ") and counter (" + counter + ")");
                        badKeyQIn.put(new BadKeyInput(line));
                    }
                }
                System.out.println("BadKeyReader poisoning the pipeline");
                badKeyQIn.put(new BadKeyInput());
            } catch(IOException ioe) {
                System.err.println("IO exception reading badKeyFile " + badKeyFileIn + " : "
                                   + ioe.getMessage());
            } catch(InterruptedException ie) {
                System.err.println("Interrupted exception during reading of badKeyFile "
                                   + badKeyFileIn + " : " + ie.getMessage());
            } finally {
                try {
                    fileReader.close();
                } catch(IOException ioe) {
                    System.err.println("IOException during fileReader.close in BadKeyReader thread.");
                }
            }
            System.out.println("BadKeyReader is done.");
        }
    }

    static class BadKeyWriter implements Runnable {

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
                    System.out.println("BadKeyWriter write key (" + badKeyResult.keyInHexFormat);

                    fileWriter.write("BADKEY," + badKeyResult.keyInHexFormat + ","
                                     + badKeyResult.fixKeyResult.name() + "\n");
                    badKeyResult = badKeyQOut.take();
                }
            } catch(IOException ioe) {
                System.err.println("IO exception reading badKeyFile " + badKeyFileOut + " : "
                                   + ioe.getMessage());
            } catch(InterruptedException ie) {
                System.err.println("Interrupted exception during writing of badKeyFile "
                                   + badKeyFileOut + " : " + ie.getMessage());
            } finally {
                try {
                    fileWriter.close();
                } catch(IOException ioe) {
                    System.err.println("Interrupted exception during fileWriter.close:"
                                       + ioe.getMessage());
                }
            }
        }
    }

    static class KeyGetter implements Runnable {

        // TODO: Add stats shared across all getters (invocations, successes,
        // etc.)

        private final CountDownLatch latch;
        private final ExecutorService repairPuttersService;
        private final BlockingQueue<BadKeyInput> badKeyQIn;
        private final BlockingQueue<BadKeyResult> badKeyQOut;
        private final boolean verbose;

        KeyGetter(CountDownLatch latch,
                  ExecutorService repairPuttersService,
                  BlockingQueue<BadKeyInput> badKeyQIn,
                  BlockingQueue<BadKeyResult> badKeyQOut,
                  boolean verbose) {
            this.latch = latch;
            this.repairPuttersService = repairPuttersService;
            this.badKeyQIn = badKeyQIn;
            this.badKeyQOut = badKeyQOut;
            this.verbose = verbose;
        }

        private String myName() {
            return Thread.currentThread().getName();
        }

        @Override
        public void run() {
            int counter = 0;
            BadKeyInput badKeyInput = null;

            try {
                badKeyInput = badKeyQIn.take();

                while(!badKeyInput.isPoison()) {
                    counter++;
                    ConsistencyFix.doKeyGetStatus doKeyGetStatus = doKeyGet(badKeyInput.getKey(),
                                                                            verbose);

                    if(doKeyGetStatus.status == ConsistencyFixStatus.SUCCESS) {
                        repairPuttersService.submit(new RepairPutter(badKeyInput.getKey(),
                                                                     badKeyQOut,
                                                                     doKeyGetStatus.nodeValues,
                                                                     verbose));
                    } else {
                        badKeyQOut.put(new BadKeyResult(badKeyInput.getKey(), doKeyGetStatus.status));
                    }

                    badKeyInput = badKeyQIn.take();
                }
                // Done. Poison other KeyGetters!
                badKeyQIn.put(new BadKeyInput());
            } catch(InterruptedException ie) {
                System.err.println("KeyGetter thread " + myName() + " interruped.");
            } finally {
                latch.countDown();
            }
            System.err.println("Thread " + myName() + " has swallowed poison and has counter = "
                               + counter);
        }
    }

    static class RepairPutter implements Runnable {

        // TODO: Add stats shared across all putters (invocations, successes,
        // etc.)

        private final String keyInHexFormat;
        private final BlockingQueue<BadKeyResult> badKeyQOut;
        private final List<NodeValue<ByteArray, byte[]>> nodeValues;
        private final boolean verbose;

        RepairPutter(String keyInHexFormat,
                     BlockingQueue<BadKeyResult> badKeyQOut,
                     List<NodeValue<ByteArray, byte[]>> nodeValues,
                     boolean verbose) {
            this.keyInHexFormat = keyInHexFormat;
            this.badKeyQOut = badKeyQOut;
            this.nodeValues = nodeValues;
            this.verbose = verbose;
        }

        private String myName() {
            return Thread.currentThread().getName();
        }

        @Override
        public void run() {
            ConsistencyFixStatus consistencyFixStatus = doRepairPut(nodeValues, verbose);
            if(consistencyFixStatus != ConsistencyFixStatus.SUCCESS) {
                try {
                    badKeyQOut.put(new BadKeyResult(keyInHexFormat, consistencyFixStatus));
                } catch(InterruptedException ie) {
                    System.err.println("RepairPutter thread " + myName() + " interruped.");
                }
            }
        }
    }

    private static ExecutorService badKeyReaderService;
    private static ExecutorService badKeyWriterService;
    private static ExecutorService badKeyGetters;
    private static ExecutorService repairPutters;

    public static void main(String[] args) throws Exception {
        // TODO: 'new' ConsistencyFix rather than static everything / everywhere
        // TODO: Move all the inner classes out into sane files...
        // TODO: Rename this file to ConsistencyFixCLI.java
        Options options = parseArgs(args);

        init(options);
        System.out.println("Initialized the consistency fixer..");

        BlockingQueue<BadKeyInput> badKeyQIn = new ArrayBlockingQueue<BadKeyInput>(1000);
        badKeyReaderService = Executors.newSingleThreadExecutor();
        badKeyReaderService.submit(new BadKeyReader(options.badKeyFileIn, badKeyQIn));
        System.out.println("Created badKeyReader.");

        BlockingQueue<BadKeyResult> badKeyQOut = new ArrayBlockingQueue<BadKeyResult>(1000);
        badKeyWriterService = Executors.newSingleThreadExecutor();
        badKeyWriterService.submit(new BadKeyWriter(options.badKeyFileOut, badKeyQOut));
        System.out.println("Created badKeyWriter.");

        CountDownLatch latch = new CountDownLatch(options.parallelism);
        badKeyGetters = Executors.newFixedThreadPool(options.parallelism);
        repairPutters = Executors.newFixedThreadPool(options.parallelism);
        System.out.println("Created getters & putters.");

        for(int i = 0; i < options.parallelism; i++) {
            badKeyGetters.submit(new KeyGetter(latch,
                                               repairPutters,
                                               badKeyQIn,
                                               badKeyQOut,
                                               options.verbose));
        }

        latch.await();
        System.out.println("All badKeyGetters have completed.");

        badKeyReaderService.shutdown();
        badKeyReaderService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("Bad key reader service has shutdown.");

        badKeyGetters.shutdown();
        badKeyGetters.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("All badKeyGetters have shutdown.");

        repairPutters.shutdown();
        repairPutters.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("All repairPutters have shutdown.");

        badKeyQOut.put(new BadKeyResult()); // Poison the bad key writer.
        badKeyWriterService.shutdown();
        badKeyWriterService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        System.out.println("Bad key writer service has shutdown.");

        stop();
        System.out.println("Stopped the consistency fixer..");

    }

    static public enum ConsistencyFixStatus {
        SUCCESS("success"),
        BAD_INIT("bad initialization of fix key"),
        FETCH_EXCEPTION("exception during fetch"),
        REPAIR_EXCEPTION("exception during repair");

        private final String name;

        private ConsistencyFixStatus(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static public class BadKeyResult {

        private final String keyInHexFormat;
        private final ConsistencyFixStatus fixKeyResult;
        private final boolean poison;

        /**
         * Normal constructor
         * 
         * @param keyInHexFormat
         * @param fixKeyResult
         */
        BadKeyResult(String keyInHexFormat, ConsistencyFixStatus fixKeyResult) {
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

        public ConsistencyFixStatus getResult() {
            return fixKeyResult;
        }
    }

    /**
     * 
     * @param vInstance
     * @param nodeIdList
     * @param keyInHexFormat
     * @param verbose
     * @param nodeIdToKeyValues Effectively the output of this method. Must pass
     *        in a non-null object to be populated by this method.
     * @return FixKeyResult
     */
    private static ConsistencyFix.ConsistencyFixStatus doRead(final List<Integer> nodeIdList,
                                                              final byte[] keyInBytes,
                                                              final String keyInHexFormat,
                                                              boolean verbose,
                                                              Map<Integer, QueryKeyResult> nodeIdToKeyValues) {
        if(nodeIdToKeyValues == null) {
            if(verbose) {
                System.out.println("Aborting doRead due to bad init.");
            }
            return ConsistencyFixStatus.BAD_INIT;
        }

        if(verbose) {
            System.out.println("Reading key-values for specified key: " + keyInHexFormat);
        }
        ByteArray key = new ByteArray(keyInBytes);
        for(int nodeId: nodeIdList) {
            List<Versioned<byte[]>> values = null;
            try {
                values = adminClient.storeOps.getNodeKey(getStoreName(), nodeId, key);
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, values));
            } catch(VoldemortException ve) {
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, ve));
            }
        }

        return ConsistencyFixStatus.SUCCESS;
    }

    /**
     * 
     * @param nodeIdList
     * @param keyInHexFormat
     * @param verbose
     * @param nodeValues Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
    private static ConsistencyFix.ConsistencyFixStatus processReadReplies(final List<Integer> nodeIdList,
                                                                          final ByteArray keyAsByteArray,
                                                                          final String keyInHexFormat,
                                                                          boolean verbose,
                                                                          final Map<Integer, QueryKeyResult> nodeIdToKeyValues,
                                                                          List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(nodeValues == null) {
            if(verbose) {
                System.out.println("Aborting processReadReplies due to bad init.");
            }
            return ConsistencyFixStatus.BAD_INIT;
        }

        if(verbose) {
            System.out.println("Confirming all nodes (" + nodeIdList
                               + ") responded with key-values for specified key: " + keyInHexFormat);
        }
        boolean exceptionsEncountered = false;
        for(int nodeId: nodeIdList) {
            if(verbose) {
                System.out.println("\t Processing response from node with id:" + nodeId);
            }
            QueryKeyResult keyValue;
            if(nodeIdToKeyValues.containsKey(nodeId)) {
                if(verbose) {
                    System.out.println("\t... There was a key-value returned from node with id:"
                                       + nodeId);
                }
                keyValue = nodeIdToKeyValues.get(nodeId);

                if(keyValue.hasException()) {
                    if(verbose) {
                        System.out.println("\t... Exception encountered while fetching key "
                                           + keyInHexFormat + " from node with nodeId " + nodeId
                                           + " : " + keyValue.getException().getMessage());
                    }
                    exceptionsEncountered = true;
                } else {
                    if(keyValue.getValues().isEmpty()) {
                        if(verbose) {
                            System.out.println("\t... Adding null version to nodeValues");
                        }
                        Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                        nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                        keyValue.getKey(),
                                                                        versioned));

                    } else {
                        for(Versioned<byte[]> value: keyValue.getValues()) {
                            if(verbose) {
                                System.out.println("\t... Adding following version to nodeValues: "
                                                   + value.getVersion());
                            }
                            nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                            keyValue.getKey(),
                                                                            value));
                        }
                    }
                }
            } else {
                if(verbose) {
                    System.out.println("\t... No key-value returned from node with id:" + nodeId);
                    System.out.println("\t... Adding null version to nodeValues");
                }
                Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId, keyAsByteArray, versioned));
            }
        }
        if(exceptionsEncountered) {
            if(verbose) {
                System.out.println("Aborting fixKey because exceptions were encountered when fetching key-values.");
            }
            return ConsistencyFixStatus.FETCH_EXCEPTION;
        }

        return ConsistencyFixStatus.SUCCESS;
    }

    /**
     * Decide on the specific key-value to write everywhere.
     * 
     * @param verbose
     * @param nodeValues
     * @return The subset of entries from nodeValues that need to be repaired.
     */
    private static List<NodeValue<ByteArray, byte[]>> resolveReadConflicts(boolean verbose,
                                                                           final List<NodeValue<ByteArray, byte[]>> nodeValues) {

        // Some cut-paste-and-modify coding from
        // store/routed/action/AbstractReadRepair.java and
        // store/routed/ThreadPoolRoutedStore.java
        if(verbose) {
            System.out.println("Resolving conflicts in responses.");
        }
        ReadRepairer<ByteArray, byte[]> readRepairer = new ReadRepairer<ByteArray, byte[]>();
        List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            if(verbose) {
                System.out.println("\tAdding toReadRepair: key (" + v.getKey() + "), version ("
                                   + versioned.getVersion() + ")");
            }
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
        }

        if(verbose) {
            System.out.println("Repair work to be done:");
            for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
                System.out.println("\tRepair key " + nodeKeyValue.getKey() + "on node with id "
                                   + nodeKeyValue.getNodeId() + " for version "
                                   + nodeKeyValue.getVersion());
            }
        }
        return toReadRepair;
    }

    /**
     * 
     * @param toReadRepair Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @param verbose
     * @param vInstance
     * @return
     */
    private static ConsistencyFix.ConsistencyFixStatus doRepairPut(final List<NodeValue<ByteArray, byte[]>> toReadRepair,
                                                                   boolean verbose) {
        if(verbose) {
            System.out.println("Performing repair work:");
        }

        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            if(verbose) {
                System.out.println("\tDoing repair for node with id:" + nodeKeyValue.getNodeId());
            }
            try {
                adminClient.storeOps.putNodeKeyValue(getStoreName(), nodeKeyValue);
            } catch(ObsoleteVersionException ove) {
                // NOOP. Treat OVE as success.
            } catch(VoldemortException ve) {
                allRepairsSuccessful = false;
                System.out.println("\t... Repair of key " + nodeKeyValue.getKey()
                                   + "on node with id " + nodeKeyValue.getNodeId()
                                   + " for version " + nodeKeyValue.getVersion()
                                   + " failed because of exception : " + ve.getMessage());
            }
        }
        if(!allRepairsSuccessful) {
            if(verbose) {
                System.err.println("Aborting fixKey because exceptions were encountered when reparing key-values.");
                System.out.println("Fix failed...");
            }
            return ConsistencyFixStatus.REPAIR_EXCEPTION;
        }
        return ConsistencyFixStatus.SUCCESS;
    }

    public static class doKeyGetStatus {

        public final ConsistencyFixStatus status;
        public final List<NodeValue<ByteArray, byte[]>> nodeValues;

        doKeyGetStatus(ConsistencyFixStatus status, List<NodeValue<ByteArray, byte[]>> nodeValues) {
            this.status = status;
            this.nodeValues = nodeValues;
        }

        doKeyGetStatus(ConsistencyFixStatus status) {
            this.status = status;
            this.nodeValues = null;
        }
    }

    public static ConsistencyFix.doKeyGetStatus doKeyGet(String keyInHexFormat, boolean verbose) {
        if(verbose) {
            System.out.println("Performing consistency fix of key: " + keyInHexFormat);
        }

        // Initialization.
        byte[] keyInBytes;
        List<Integer> nodeIdList = null;
        int masterPartitionId = -1;
        try {
            keyInBytes = ByteUtils.fromHexString(keyInHexFormat);
            masterPartitionId = getMasterPartitionId(keyInHexFormat);
            nodeIdList = storeInstance.getReplicationNodeList(masterPartitionId);
        } catch(Exception exception) {
            if(verbose) {
                System.out.println("Aborting fixKey due to bad init.");
                exception.printStackTrace();
            }
            return new doKeyGetStatus(ConsistencyFixStatus.BAD_INIT);
        }
        ByteArray keyAsByteArray = new ByteArray(keyInBytes);

        // Read
        Map<Integer, QueryKeyResult> nodeIdToKeyValues = new HashMap<Integer, QueryKeyResult>();
        ConsistencyFixStatus fixKeyResult = ConsistencyFix.doRead(nodeIdList,
                                                                  keyInBytes,
                                                                  keyInHexFormat,
                                                                  verbose,
                                                                  nodeIdToKeyValues);
        if(fixKeyResult != ConsistencyFixStatus.SUCCESS) {
            return new doKeyGetStatus(fixKeyResult);
        }

        // Process read replies
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
        fixKeyResult = ConsistencyFix.processReadReplies(nodeIdList,
                                                         keyAsByteArray,
                                                         keyInHexFormat,
                                                         verbose,
                                                         nodeIdToKeyValues,
                                                         nodeValues);
        if(fixKeyResult != ConsistencyFixStatus.SUCCESS) {
            return new doKeyGetStatus(fixKeyResult);
        }

        // Resolve conflicts
        List<NodeValue<ByteArray, byte[]>> toReadRepair = ConsistencyFix.resolveReadConflicts(verbose,
                                                                                              nodeValues);

        return new doKeyGetStatus(ConsistencyFixStatus.SUCCESS, toReadRepair);
    }

}

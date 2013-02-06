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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        System.out.println("Required arguments: \n" + " --url <url>\n" + " --store <storeName>\n"
                           + " (--keys <keysInHexFormatSeparatedByComma "
                           + "| --key-file <FileNameOfInputListOfKeysToFix>)\n"
                           + "| --out-file <FileNameOfOutputListOfKeysNotFixed>)\n");
    }

    public static void printUsage(String errMessage) {
        System.err.println("Error: " + errMessage);
        printUsage();
        System.exit(1);
    }

    private static class Options {

        public String url = null;
        public String storeName = null;
        public String outFile = null;
        public List<String> keysInHexFormat = null;
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
        parser.accepts("keys")
              .withRequiredArg()
              .withValuesSeparatedBy(',')
              .describedAs("List of keys. "
                           + "Each key must be in hexadecimal format. "
                           + "Each key must be separated only by a comma ',' without any white space.")
              .ofType(String.class);
        parser.accepts("key-file")
              .withRequiredArg()
              .describedAs("Name of key-file. " + "Each key must be in hexadecimal format. "
                           + "Each key must be on a separate line in the file. ")
              .ofType(String.class);
        parser.accepts("out-file")
              .withRequiredArg()
              .describedAs("Name of out-file. "
                           + "Success/failure of each key is dumped to out-file. ")
              .ofType(String.class);

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
        if(!optionSet.has("keys") && !optionSet.has("key-file")) {
            printUsage("Missing required key-specifying argument: 'keys' or 'key-file'.");
        }
        if(optionSet.has("keys") && optionSet.has("key-file")) {
            printUsage("Please provide exactly one key-specifying argument: 'keys' or 'key-file'.");
        }
        if(!optionSet.has("out-file")) {
            printUsage("Missing required 'out-file' argument.");
        }

        Options options = new Options();

        if(optionSet.has("verbose")) {
            options.verbose = true;
        }

        options.url = (String) optionSet.valueOf("url");
        options.storeName = (String) optionSet.valueOf("store");
        options.outFile = (String) optionSet.valueOf("out-file");

        options.keysInHexFormat = new LinkedList<String>();
        if(optionSet.has("keys")) {
            @SuppressWarnings("unchecked")
            List<String> valuesOf = (List<String>) optionSet.valuesOf("keys");
            options.keysInHexFormat = valuesOf;
        }
        // TODO: Should I do something more iterator like for reading files of
        // keys? I suspect that we should not read millions(?) of keys into
        // memory before doing any actual work.
        if(optionSet.has("key-file")) {
            String keyFile = (String) optionSet.valueOf("key-file");
            System.err.println("Key file: " + keyFile);
            try {
                BufferedReader fileReader = new BufferedReader(new FileReader(keyFile));
                for(String line = fileReader.readLine(); line != null; line = fileReader.readLine()) {
                    if(!line.isEmpty()) {
                        options.keysInHexFormat.add(line);
                    }
                }
            } catch(IOException e) {
                Utils.croak("Failure to open input stream: " + e.getMessage());
            }
        }

        return options;
    }

    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);

        BufferedWriter fileWriter = null;
        try {
            fileWriter = new BufferedWriter(new FileWriter(options.outFile));
        } catch(IOException e) {
            Utils.croak("Failure to open ouput file '" + options.outFile + "': " + e.getMessage());
        }
        if(fileWriter == null) {
            Utils.croak("Failure to create BufferedWriter for ouput file '" + options.outFile + "'");
        }

        init(options);

        for(String keyInHexFormat: options.keysInHexFormat) {
            FixKeyResult fixKeyResult = fixKey(keyInHexFormat, options.verbose);
            if(fixKeyResult == FixKeyResult.SUCCESS) {
                System.out.println("Successfully processed " + keyInHexFormat);
            } else {
                fileWriter.write("BADKEY," + keyInHexFormat + "," + fixKeyResult.name());
            }
        }

        stop();
    }

    public enum FixKeyResult {
        SUCCESS("success"),
        BAD_INIT("bad initialization of fix key"),
        FETCH_EXCEPTION("exception during fetch"),
        REPAIR_EXCEPTION("exception during repair");

        private final String name;

        private FixKeyResult(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
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
    private static ConsistencyFix.FixKeyResult doRead(final List<Integer> nodeIdList,
                                                      final byte[] keyInBytes,
                                                      final String keyInHexFormat,
                                                      boolean verbose,
                                                      Map<Integer, QueryKeyResult> nodeIdToKeyValues) {
        if(nodeIdToKeyValues == null) {
            if(verbose) {
                System.out.println("Aborting doRead due to bad init.");
            }
            return FixKeyResult.BAD_INIT;
        }

        if(verbose) {
            System.out.println("Reading key-values for specified key: " + keyInHexFormat);
        }
        ByteArray key = new ByteArray(keyInBytes);
        // TODO: Do this asynchronously so that all requests are outstanding in
        // parallel. Will need to make nodeIdToKeyValues thread safe.
        for(int nodeId: nodeIdList) {
            List<Versioned<byte[]>> values = null;
            try {
                values = adminClient.storeOps.getNodeKey(getStoreName(), nodeId, key);
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, values));
            } catch(VoldemortException ve) {
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, ve));
            }
        }

        return FixKeyResult.SUCCESS;
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
    private static ConsistencyFix.FixKeyResult processReadReplies(final List<Integer> nodeIdList,
                                                                  final ByteArray keyAsByteArray,
                                                                  final String keyInHexFormat,
                                                                  boolean verbose,
                                                                  final Map<Integer, QueryKeyResult> nodeIdToKeyValues,
                                                                  List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(nodeValues == null) {
            if(verbose) {
                System.out.println("Aborting processReadReplies due to bad init.");
            }
            return FixKeyResult.BAD_INIT;
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
            return FixKeyResult.FETCH_EXCEPTION;
        }

        return FixKeyResult.SUCCESS;
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
     * @param vInstance
     * @param verbose
     * @param toReadRepair Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
    private static ConsistencyFix.FixKeyResult doWriteBack(boolean verbose,
                                                           final List<NodeValue<ByteArray, byte[]>> toReadRepair) {
        if(verbose) {
            System.out.println("Performing repair work:");
        }

        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            if(verbose) {
                System.out.println("\tDoing repair for node with id:" + nodeKeyValue.getNodeId());
            }
            // TODO: Do this asynchronously so that all requests are outstanding
            // in parallel.
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
            return FixKeyResult.REPAIR_EXCEPTION;
        }
        return FixKeyResult.SUCCESS;
    }

    public static ConsistencyFix.FixKeyResult fixKey(String keyInHexFormat, boolean verbose) {
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
            return FixKeyResult.BAD_INIT;
        }
        ByteArray keyAsByteArray = new ByteArray(keyInBytes);

        // Read
        Map<Integer, QueryKeyResult> nodeIdToKeyValues = new HashMap<Integer, QueryKeyResult>();
        FixKeyResult fixKeyResult = ConsistencyFix.doRead(nodeIdList,
                                                          keyInBytes,
                                                          keyInHexFormat,
                                                          verbose,
                                                          nodeIdToKeyValues);
        if(fixKeyResult != FixKeyResult.SUCCESS) {
            return fixKeyResult;
        }

        // Process read replies
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
        fixKeyResult = ConsistencyFix.processReadReplies(nodeIdList,
                                                         keyAsByteArray,
                                                         keyInHexFormat,
                                                         verbose,
                                                         nodeIdToKeyValues,
                                                         nodeValues);
        if(fixKeyResult != FixKeyResult.SUCCESS) {
            return fixKeyResult;
        }

        // Resolve conflicts
        List<NodeValue<ByteArray, byte[]>> toReadRepair = ConsistencyFix.resolveReadConflicts(verbose,
                                                                                              nodeValues);

        // Write back (if necessary)
        fixKeyResult = ConsistencyFix.doWriteBack(verbose, toReadRepair);
        if(fixKeyResult != FixKeyResult.SUCCESS) {
            return fixKeyResult;
        }

        // Success!
        if(verbose) {
            System.out.println("Fix for key " + keyInHexFormat + "completed successfully!!");
        }
        return FixKeyResult.SUCCESS;
    }
}

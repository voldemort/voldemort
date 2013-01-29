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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClient.QueryKeyResult;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.ReadRepairer;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ConsistencyFix {

    private static class ConsistencyFixContext {

        private final AdminClient adminClient;
        private final StoreInstance storeInstance;

        public ConsistencyFixContext(String url, String storeName) throws Exception {
            System.out.println("Connecting to bootstrap server: " + url);
            adminClient = new AdminClient(url, new AdminClientConfig(), 0);
            Cluster cluster = adminClient.getAdminClientCluster();
            System.out.println("Cluster determined to be: " + cluster.getName());

            System.out.println("Determining store definition for store: " + storeName);
            Versioned<List<StoreDefinition>> storeDefinitions = adminClient.getRemoteStoreDefList(0);
            List<StoreDefinition> storeDefs = storeDefinitions.getValue();
            StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                              storeName);
            System.out.println("Store definition determined.");

            storeInstance = new StoreInstance(cluster, storeDefinition);
        }

        public AdminClient getAdminClient() {
            return adminClient;
        }

        public StoreInstance getStoreInstance() {
            return storeInstance;
        }

        public String getStoreName() {
            return storeInstance.getStoreDefinition().getName();
        }

        public int getMasterPartitionId(String keyInHexFormat) throws DecoderException {
            byte[] key = Hex.decodeHex(keyInHexFormat.toCharArray());
            return storeInstance.getMasterPartitionId(key);
        }
    }

    public static void printUsage() {
        System.out.println("Required arguments: \n" + " --url <url>\n" + " --store <storeName>\n"
                           + " (--key <keyInHexFormat> | --keys <keysInHexFormatSeparatedByComma "
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
        parser.accepts("key")
              .withRequiredArg()
              .describedAs("The key in hexadecimal format.")
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
        if(!optionSet.has("key") && !optionSet.has("keys") && !optionSet.has("key-file")) {
            printUsage("Missing required key-specifying argument: 'key', 'keys', or 'key-file'.");
        }
        if((optionSet.has("key") && optionSet.has("keys"))
           || (optionSet.has("key") && optionSet.has("key-file"))
           || (optionSet.has("keys") && optionSet.has("key-file"))) {
            printUsage("Please provide exactly one key-specifying argument: 'key', 'keys', or 'key-file'.");
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
        if(optionSet.has("key")) {
            options.keysInHexFormat.add((String) optionSet.valueOf("key"));
        }
        if(optionSet.has("keys")) {
            @SuppressWarnings("unchecked")
            List<String> valuesOf = (List<String>) optionSet.valuesOf("keys");
            options.keysInHexFormat = valuesOf;
        }
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

        ConsistencyFixContext vInstance = new ConsistencyFixContext(options.url, options.storeName);
        for(String keyInHexFormat: options.keysInHexFormat) {
            FixKeyResult fixKeyResult = fixKey(vInstance, keyInHexFormat, options.verbose);
            if(fixKeyResult == FixKeyResult.SUCCESS) {
                System.out.println("Successfully processed " + keyInHexFormat);
            } else {
                fileWriter.write("BADKEY," + keyInHexFormat + "," + fixKeyResult.name());
            }
        }
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
    private static ConsistencyFix.FixKeyResult doRead(final ConsistencyFixContext vInstance,
                                                      final List<Integer> nodeIdList,
                                                      final byte[] keyInBytes,
                                                      final String keyInHexFormat,
                                                      boolean verbose,
                                                      Map<Integer, Iterator<QueryKeyResult>> nodeIdToKeyValues) {
        if(nodeIdToKeyValues == null) {
            if(verbose) {
                System.out.println("Aborting doRead due to bad init.");
            }
            return FixKeyResult.BAD_INIT;
        }

        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(new ByteArray(keyInBytes));

        if(verbose) {
            System.out.println("Reading key-values for specified key: " + keyInHexFormat);
        }
        for(int nodeId: nodeIdList) {
            Iterator<QueryKeyResult> keyValues;
            keyValues = vInstance.getAdminClient().queryKeys(nodeId,
                                                             vInstance.getStoreName(),
                                                             keys.iterator());
            nodeIdToKeyValues.put(nodeId, keyValues);
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
                                                                  final Map<Integer, Iterator<QueryKeyResult>> nodeIdToKeyValues,
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
            if(nodeIdToKeyValues.get(nodeId).hasNext()) {
                if(verbose) {
                    System.out.println("\t... There was a key-value returned from node with id:"
                                       + nodeId);
                }
                keyValue = nodeIdToKeyValues.get(nodeId).next();

                if(keyValue.exception != null) {
                    if(verbose) {
                        System.out.println("\t... Exception encountered while fetching key "
                                           + keyInHexFormat + " from node with nodeId " + nodeId
                                           + " : " + keyValue.exception.getMessage());
                    }
                    exceptionsEncountered = true;
                } else {
                    if(keyValue.values.isEmpty()) {
                        if(verbose) {
                            System.out.println("\t... Adding null version to nodeValues");
                        }
                        Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                        nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                        keyValue.key,
                                                                        versioned));

                    } else {
                        for(Versioned<byte[]> value: keyValue.values) {
                            if(verbose) {
                                System.out.println("\t... Adding following version to nodeValues: "
                                                   + value.getVersion());
                            }
                            nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                            keyValue.key,
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
     * 
     * @param verbose
     * @param nodeValues
     * @return The subset of entries from nodeValues that need to be repaired.
     */
    private static List<NodeValue<ByteArray, byte[]>> resolveReadConflicts(boolean verbose,
                                                                           final List<NodeValue<ByteArray, byte[]>> nodeValues) {

        // Decide on the specific key-value to write everywhere.
        // Some cut-paste-and-modify coding from AbstractReadRepair.java...
        if(verbose) {
            System.out.println("Resolving conflicts in responses.");
        }
        // TODO: Figure out if 'cloning' is necessary. It does not seem to be
        // necessary. See both store/routed/action/AbstractReadRepair.java and
        // store/routed/ThreadPoolRoutedStore.java for other copies of this
        // code. I think the cut-and-paste comment below may just be confusing.
        // We need to "clone" the subset of the nodeValues that we actually want
        // to repair. But, I am not sure we need to clone the versioned part of
        // each copied object.
        ReadRepairer<ByteArray, byte[]> readRepairer = new ReadRepairer<ByteArray, byte[]>();
        List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        // TODO: Remove/clean up this comment (and possibly the two copies of
        // this comment in the code.
        /*
         * We clone after computing read repairs in the assumption that the
         * output will be smaller than the input. Note that we clone the
         * version, but not the key or value as the latter two are not mutated.
         */
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            if(verbose) {
                System.out.println("\tAdding toReadRepair: key (" + v.getKey() + "), version ("
                                   + versioned.getVersion() + ")");
            }
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
            /*-
             * The below code seems to work in lieu of the above line. So, not sure
             * why it is necessary to construct new versioned object above based
             * on cloned timestamp.
             * 
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(),
                                                              v.getKey(),
                                                              v.getVersioned()));
             */
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
    private static ConsistencyFix.FixKeyResult doWriteBack(final ConsistencyFixContext vInstance,
                                                           boolean verbose,
                                                           final List<NodeValue<ByteArray, byte[]>> toReadRepair) {
        if(verbose) {
            System.out.println("Performing repair work:");
        }

        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            if(verbose) {
                System.out.println("\tDoing repair for node with id:" + nodeKeyValue.getNodeId());
            }
            Exception e = vInstance.getAdminClient().repairEntry(vInstance.getStoreName(),
                                                                 nodeKeyValue);
            if(e != null) {
                if(verbose) {
                    System.out.println("\t... Repair of key " + nodeKeyValue.getKey()
                                       + "on node with id " + nodeKeyValue.getNodeId()
                                       + " for version " + nodeKeyValue.getVersion()
                                       + " failed because of exception : " + e.getMessage());
                }
                allRepairsSuccessful = false;
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

    // TODO: Decide between design based on a key-by-key fix and a design based
    // on something that looks more like stream processing. E.g., See
    // AdminClient.updateEntries and
    // DonorBasedRebalancePusherSlave for ideas.
    // TODO: As a follow on, need to decide
    // if queryKeys should offer a queryKey interface, and/or if repairEntry
    // ought to offer a repairEntries interface.
    public static ConsistencyFix.FixKeyResult fixKey(ConsistencyFixContext vInstance,
                                                     String keyInHexFormat,
                                                     boolean verbose) {
        if(verbose) {
            System.out.println("Performing consistency fix of key: " + keyInHexFormat);
        }

        // Initialization.
        byte[] keyInBytes;
        List<Integer> nodeIdList = null;
        int masterPartitionId = -1;
        try {
            keyInBytes = ByteUtils.fromHexString(keyInHexFormat);
            masterPartitionId = vInstance.getMasterPartitionId(keyInHexFormat);
            nodeIdList = vInstance.getStoreInstance().getReplicationNodeList(masterPartitionId);
        } catch(Exception exception) {
            if(verbose) {
                System.out.println("Aborting fixKey due to bad init.");
                exception.printStackTrace();
            }
            return FixKeyResult.BAD_INIT;
        }
        ByteArray keyAsByteArray = new ByteArray(keyInBytes);

        // Read
        Map<Integer, Iterator<QueryKeyResult>> nodeIdToKeyValues = new HashMap<Integer, Iterator<QueryKeyResult>>();
        FixKeyResult fixKeyResult = ConsistencyFix.doRead(vInstance,
                                                          nodeIdList,
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
        fixKeyResult = ConsistencyFix.doWriteBack(vInstance, verbose, toReadRepair);
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

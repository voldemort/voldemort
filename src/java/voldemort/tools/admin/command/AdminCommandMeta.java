/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools.admin.command;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaType;
import voldemort.store.readonly.ReadOnlyFileEntry;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.system.SystemStoreConstants;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Implements all meta commands.
 */
public class AdminCommandMeta extends AbstractAdminCommand {

    private static final String METAKEY_ALL = "all";

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("check")) {
            SubCommandMetaCheck.executeCommand(args);
        } else if(subCmd.equals("clear-rebalance")) {
            SubCommandMetaClearRebalance.executeCommand(args);
        } else if(subCmd.equals("get")) {
            SubCommandMetaGet.executeCommand(args);
        } else if(subCmd.equals("get-ro")) {
            SubCommandMetaGetRO.executeCommand(args);
        } else if(subCmd.equals("set")) {
            SubCommandMetaSet.executeCommand(args);
        } else if(subCmd.equals("sync-version")) {
            SubCommandMetaSyncVersion.executeCommand(args);
        } else if(subCmd.equals("check-version")) {
            SubCommandMetaCheckVersion.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Meta Commands");
        stream.println("----------------------------------");
        stream.println("check             Check if metadata is consistent across all nodes.");
        stream.println("clear-rebalance   Remove metadata related to rebalancing.");
        stream.println("get               Get metadata from nodes.");
        stream.println("get-ro            Get read-only metadata from nodes and stores.");
        stream.println("set               Set metadata on nodes.");
        stream.println("sync-version      Synchronize metadata versions across all nodes.");
        stream.println("check-version     Verify metadata versions on all the cluster nodes.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help meta <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("check")) {
            SubCommandMetaCheck.printHelp(stream);
        } else if(subCmd.equals("clear-rebalance")) {
            SubCommandMetaClearRebalance.printHelp(stream);
        } else if(subCmd.equals("get")) {
            SubCommandMetaGet.printHelp(stream);
        } else if(subCmd.equals("get-ro")) {
            SubCommandMetaGetRO.printHelp(stream);
        } else if(subCmd.equals("set")) {
            SubCommandMetaSet.printHelp(stream);
        } else if(subCmd.equals("sync-version")) {
            SubCommandMetaSyncVersion.printHelp(stream);
        } else if(subCmd.equals("check-version")) {
            SubCommandMetaCheckVersion.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * meta check command
     */
    public static class SubCommandMetaCheck extends AbstractAdminCommand {

        private static final String OPT_HEAD_META_CHECK = "meta-check";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_META_CHECK, "metadata keys to be checked")
                  .withOptionalArg()
                  .describedAs("meta-key-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta check - Check if metadata is consistent across all nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta check (<meta-key-list> | all) -u <url>");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid meta keys are:");
            stream.println("    " + MetadataStore.CLUSTER_KEY);
            stream.println("    " + MetadataStore.STORES_KEY);
            stream.println("    " + MetadataStore.SERVER_STATE_KEY);
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and checks if metadata is consistent across all
         * nodes.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> metaKeys = null;
            String url = null;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_CHECK);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_META_CHECK);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            metaKeys = (List<String>) options.valuesOf(OPT_HEAD_META_CHECK);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);


            // execute command
            if(metaKeys.size() == 0
               || (metaKeys.size() == 1 && metaKeys.get(0).equals(METAKEY_ALL))) {
                metaKeys = Lists.newArrayList();
                metaKeys.add(MetadataStore.CLUSTER_KEY);
                metaKeys.add(MetadataStore.STORES_KEY);
                metaKeys.add(MetadataStore.SERVER_STATE_KEY);
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            doMetaCheck(adminClient, metaKeys);
        }

        private static void addMetadataValue(Map<Object, List<String>> allValues,
                                             Object metadataValue,
                                             String node) {
            if(allValues.containsKey(metadataValue) == false) {
                allValues.put(metadataValue, new ArrayList<String>());
            }
            allValues.get(metadataValue).add(node);
        }

        private static Boolean checkDiagnostics(String keyName,
                                                Map<Object, List<String>> metadataValues,
                                                Collection<String> allNodeNames) {

            Collection<String> nodesInResult = new ArrayList<String>();
            Boolean checkResult = true;

            if(metadataValues.size() == 1) {
                Map.Entry<Object, List<String>> entry = metadataValues.entrySet().iterator().next();
                nodesInResult.addAll(entry.getValue());
            } else {
                // Some nodes have different set of data than the others.
                checkResult = false;
                int groupCount = 0;
                for(Map.Entry<Object, List<String>> entry: metadataValues.entrySet()) {
                    groupCount++;
                    String keyToPrint = entry.getKey().toString() ;
                    if(keyToPrint.length() > 50) {
                      keyToPrint = keyToPrint.substring(0, 50) + "...truncated";
                    }
                    System.err.println("Nodes with same value " + keyToPrint +" for " + keyName + ". Id :"
                                       + groupCount);
                    nodesInResult.addAll(entry.getValue());
                    for(String nodeName: entry.getValue()) {
                        System.err.println("Node " + nodeName);
                    }
                    System.out.println();
                }
            }

            // Some times when a store could be missing from one of the nodes
            // In that case the map will have only one value, but total number
            // of nodes will be lesser. The following code handles that.

            // removeAll modifies the list that is being called on. so create a
            // copy
            Collection<String> nodesDiff = new ArrayList<String>(allNodeNames.size());
            nodesDiff.addAll(allNodeNames);
            nodesDiff.removeAll(nodesInResult);

            if(nodesDiff.size() > 0) {
                checkResult = false;
                for(String nodeName: nodesDiff) {
                    System.err.println(keyName + " is missing in the Node " + nodeName);
                }
            }
            return checkResult;
        }

        private static byte[] toBytes(int i) {
            byte[] result = new byte[4];

            result[0] = (byte) (i >> 24);
            result[1] = (byte) (i >> 16);
            result[2] = (byte) (i >> 8);
            result[3] = (byte) (i /*>> 0*/);

            return result;
        }

        private static ByteArray generateByteArrayKey(AdminClient adminClient,
                                               StoreDefinition storeDef,
                                               int nodeId) {

            Cluster cluster = adminClient.getAdminClientCluster();
            RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster);
            
            final int MAX_NUM_TO_SEARCH = 10000000;
            for(int i = 0; i < MAX_NUM_TO_SEARCH ; i ++) {
                byte[] serializedKey = toBytes(i);
                List<Node> nodesServingKey = routingStrategy.routeRequest(serializedKey);
                for(Node node: nodesServingKey) {
                    if(node.getId() == nodeId) {
                        return new ByteArray(serializedKey);
                    }
                }
            }
            
            throw new VoldemortException("Could not find a key that maps to the node in the range 0.."
                                         + MAX_NUM_TO_SEARCH);
        }

        private static boolean checkGetStore(AdminClient adminClient,
                                         StoreDefinition storeDef,
                                         Node node) {

            if (node.getPartitionIds().size() == 0) {
              // No Partitions are assigned to this node.
              return true;
            }

            String storeName = storeDef.getName();
            try {
                ByteArray randomKey = generateByteArrayKey(adminClient, storeDef, node.getId());
                adminClient.storeOps.getNodeKey(storeName, node.getId(), randomKey);
            } catch(Exception e) {
                System.out.println(" Error doing sample key get from Store " + storeName
                                   + node.briefToString() + " Error " + e.getMessage());
                e.printStackTrace();
                return false;
            }
            return true;
        }

        private static void validateROFiles(AdminClient adminClient,
                                            Collection<Node> allNodes,
                                            Map<String, Integer> roStoreToReplicationFactorMap) {
            if(roStoreToReplicationFactorMap.size() == 0)
                return;

            boolean checkResult = true;
            boolean isSizePresent = false;
            for(Map.Entry<String, Integer> storeAndReplicationFactor: roStoreToReplicationFactorMap.entrySet()) {
                String storeName = storeAndReplicationFactor.getKey();
                Integer replicationFactor = storeAndReplicationFactor.getValue();
                if(replicationFactor == 1) {
                    System.out.println(" Store "
                                       + storeName
                                       + " has replication factor of 1, skipping consistency check across nodes");
                    continue;
                }
                Map<ReadOnlyFileEntry, List<String>> fileToNodesMap = new TreeMap<ReadOnlyFileEntry, List<String>>();
                for(Node node: allNodes) {
                    List<ReadOnlyFileEntry> fileEntries = null;
                    try {
                        fileEntries = adminClient.readonlyOps.getROStorageFileMetadata(node.getId(),
                                                                                       storeName);
                    } catch(Exception e) {
                        System.out.println(" Error retrieving files for ReadOnly Store "
                                           + storeName + node.briefToString()
                                           + " Error " + e.getMessage());
                        e.printStackTrace();
                        checkResult = false;
                        // Once one node throws an exception, it is going to
                        // spew lots of missing files
                        break;
                    }

                    for(ReadOnlyFileEntry entry: fileEntries) {
                       if(entry.getSize() > 0) {
                         isSizePresent = true;
                       }
                        if(fileToNodesMap.containsKey(entry) == false) {
                            fileToNodesMap.put(entry, new ArrayList<String>());
                        }
                        fileToNodesMap.get(entry).add(node.briefToString());
                    }
                }

                for(Map.Entry<ReadOnlyFileEntry, List<String>> fileToNodes: fileToNodesMap.entrySet()) {
                    ReadOnlyFileEntry entry = fileToNodes.getKey();
                    List<String> nodes = fileToNodes.getValue();
                    if(nodes.size() == replicationFactor) {
                        // The file has the required replication factor.
                        continue;
                    } else if(isSizePresent && entry.getSize() == 0){
                       // 0 - sized files are created on the fly, when data and index is empty.
                       // Old protocol does not send size, but if sizes are present, then
                       // do not report the empty files
                      continue;                     
                    } else {
                        System.out.println("    Store " + storeName + " File " + entry
                                           + " is expected in " + replicationFactor
                                           + " nodes, but present only in "
                                           + Arrays.toString(nodes.toArray()));
                        checkResult = false;
                    }
                }
            }
            
            System.out.println("RO File Validation check : " + (checkResult ? "PASSED" : "FAILED"));
        }

        /**
         * Checks if metadata is consistent across all nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param metaKeys List of metakeys to check
         * 
         */
        public static void doMetaCheck(AdminClient adminClient, List<String> metaKeys) {
            Map<String, Integer> roStoreToReplicationFactorMap = new HashMap<String, Integer>();
            Collection<Node> allNodes = adminClient.getAdminClientCluster().getNodes();

            for(String key: metaKeys) {
                Map<String, Map<Object, List<String>>> storeDefToNodeMap = new HashMap<String, Map<Object, List<String>>>(); 
                Map<Object, List<String>> metadataNodeValueMap = new HashMap<Object, List<String>>();
                
                Collection<String> allNodeNames = new ArrayList<String>();

                Boolean checkResult = true;
                for(Node node: allNodes) {
                    String nodeName = "Host '" + node.getHost() + "' : ID " + node.getId();
                    allNodeNames.add(nodeName);

                    Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                key);
                    if(versioned == null || versioned.getValue() == null) {
                        throw new VoldemortException("Value returned from node " + node.getId()
                                                     + " was null");
                    } else if(key.compareTo(MetadataStore.STORES_KEY) == 0) {
                        List<StoreDefinition> storeDefinitions = new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue()));
                        for(StoreDefinition storeDef: storeDefinitions) {
                            String storeName = storeDef.getName();

                            if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                                roStoreToReplicationFactorMap.put(storeName, storeDef.getReplicationFactor());
                            }

                            if(storeDefToNodeMap.containsKey(storeName) == false) {
                                storeDefToNodeMap.put(storeName,
                                                      new HashMap<Object, List<String>>());
                            }
                            Map<Object, List<String>> storeDefMap = storeDefToNodeMap.get(storeName);
                            addMetadataValue(storeDefMap, storeDef, nodeName);
                            
                            // Try a random Get against one key for store/Node
                            checkGetStore(adminClient, storeDef, node);
                        }
                    } else {
                        if(key.compareTo(MetadataStore.CLUSTER_KEY) == 0
                           || key.compareTo(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML) == 0) {
                            Cluster cluster = new ClusterMapper().readCluster(new StringReader(versioned.getValue()));
                            addMetadataValue(metadataNodeValueMap, cluster, nodeName);
                        } else if(key.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                            VoldemortState voldemortStateValue = VoldemortState.valueOf(versioned.getValue());
                            addMetadataValue(metadataNodeValueMap, voldemortStateValue, nodeName);
                        } else {
                            throw new VoldemortException("Incorrect metadata key");
                        }

                    }
                }

                if(metadataNodeValueMap.size() > 0) {
                    checkResult &= checkDiagnostics("Key " + key,
                                                    metadataNodeValueMap,
                                                    allNodeNames);
                }

                if(storeDefToNodeMap.size() > 0) {
                    for(Map.Entry<String, Map<Object, List<String>>> storeNodeValueEntry: storeDefToNodeMap.entrySet()) {
                        String storeName = storeNodeValueEntry.getKey();
                        Map<Object, List<String>> storeDefMap = storeNodeValueEntry.getValue();
                        checkResult &= checkDiagnostics("Store " + storeName,
                                                        storeDefMap,
                                                        allNodeNames);
                        for(QuotaType type : QuotaType.values()) {
                          Map<Object, List<String>> quotaNodeValues = new HashMap<Object, List<String>>();
                          String keyName = "Store:" + storeName + "_QuotaType_"+ type;
                          for(Node node: allNodes) {                          
                            String nodeName = "Host '" + node.getHost() + "' : ID " + node.getId();
                            Versioned<String> quotaValue = adminClient.quotaMgmtOps.getQuotaForNode(storeName, type, node.getId());
                            if(quotaValue == null || quotaValue.getValue() == null) {
                              continue;
                            }
                            addMetadataValue( quotaNodeValues , quotaValue.getValue() , nodeName);
                          }

                          if(quotaNodeValues.size() > 0) {
                            boolean result = checkDiagnostics(keyName,
                                quotaNodeValues,
                                allNodeNames);
                            checkResult &= result;
                          }
                        }
                    }
                }

                System.out.println(key + " metadata check : " + (checkResult ? "PASSED" : "FAILED"));
            }

            validateROFiles(adminClient, allNodes, roStoreToReplicationFactorMap);
        }

    }

    /**
     * meta clear-rebalance command
     */
    public static class SubCommandMetaClearRebalance extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta clear-rebalance - Remove metadata related to rebalancing");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta clear-rebalance -u <url> [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and removes metadata related to rebalancing.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Remove metadata related to rebalancing");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "remove metadata related to rebalancing")) {
                return;
            }
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            doMetaClearRebalance(adminClient, nodeIds);
        }

        /**
         * Removes metadata related to rebalancing.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to clear metadata after rebalancing
         * 
         */
        public static void doMetaClearRebalance(AdminClient adminClient, List<Integer> nodeIds) {
            AdminToolUtils.assertServerNotInOfflineState(adminClient, nodeIds);
            System.out.println("Setting " + MetadataStore.SERVER_STATE_KEY + " to "
                               + MetadataStore.VoldemortState.NORMAL_SERVER);
            doMetaSet(adminClient,
                      nodeIds,
                      MetadataStore.SERVER_STATE_KEY,
                      MetadataStore.VoldemortState.NORMAL_SERVER.toString());
            RebalancerState state = RebalancerState.create("[]");
            System.out.println("Cleaning up " + MetadataStore.REBALANCING_STEAL_INFO + " to "
                               + state.toJsonString());
            doMetaSet(adminClient,
                      nodeIds,
                      MetadataStore.REBALANCING_STEAL_INFO,
                      state.toJsonString());
            System.out.println("Cleaning up " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML
                               + " to empty string");
            doMetaSet(adminClient, nodeIds, MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, "");
        }
    }

    /**
     * meta get command
     */
    public static class SubCommandMetaGet extends AbstractAdminCommand {

        public static final String OPT_HEAD_META_GET = "meta-get";
        public static final String OPT_VERBOSE = "verbose";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_META_GET, "metadata keys to fetch")
                  .withOptionalArg()
                  .describedAs("meta-key-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsDir(parser);
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            parser.accepts(OPT_VERBOSE, "print all metadata");
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta get - Get metadata from nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta get (<meta-key-list> | all) -u <url> [-d <output-dir>]");
            stream.println("           [-n <node-id-list> | --all-nodes] [--verbose]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid meta keys are:");
            for(Object key: MetadataStore.METADATA_KEYS) {
                stream.println("    " + (String) key);
            }
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and gets metadata.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> metaKeys = null;
            String url = null;
            String dir = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean verbose = false;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_GET);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_META_GET);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            metaKeys = (List<String>) options.valuesOf(OPT_HEAD_META_GET);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_DIR)) {
                dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            }
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(OPT_VERBOSE)) {
                verbose = true;
            }

            // execute command
            File directory = AdminToolUtils.createDir(dir);
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            if(metaKeys.size() == 1 && metaKeys.get(0).equals(METAKEY_ALL)) {
                metaKeys = Lists.newArrayList();
                for(Object key: MetadataStore.METADATA_KEYS) {
                    metaKeys.add((String) key);
                }
            }

            doMetaGet(adminClient, nodeIds, metaKeys, directory, verbose);
        }

        /**
         * Gets metadata.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to fetch metadata from
         * @param metaKeys List of metadata to fetch
         * @param directory Directory to output to
         * @param verbose Tells whether to print metadata verbosely
         * @throws IOException
         */
        @SuppressWarnings({ "unchecked", "cast", "rawtypes" })
        public static void doMetaGet(AdminClient adminClient,
                                     Collection<Integer> nodeIds,
                                     List<String> metaKeys,
                                     File directory,
                                     Boolean verbose) throws IOException {
            Map<String, List<Node>> nodeMap = new HashMap<String, List<Node>>();
            Map<Node, Version> versionMap = new HashMap<Node, Version>();
            for(String key: metaKeys) {
                nodeMap.clear();
                versionMap.clear();
                System.out.println("Metadata: " + key);
                for(Integer nodeId: nodeIds) {
                    Versioned<String> versioned = null;
                    try {
                        versioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId, key);
                    } catch(Exception e) {
                        System.out.println("Error in retrieving " + e.getMessage());
                        System.out.println();
                        continue;
                    }
                    if(directory != null) {
                        FileUtils.writeStringToFile(new File(directory, key + "_" + nodeId),
                                                    ((versioned == null) ? ""
                                                                        : versioned.getValue()));
                    } else {
                        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
                        if(verbose) {
                            System.out.println(node.getHost() + ":" + nodeId);
                            if(versioned == null) {
                                System.out.println("null");
                            } else {
                                System.out.println(versioned.getVersion());
                                System.out.print(": ");
                                System.out.println(versioned.getValue());
                                System.out.println();
                            }
                        } else {
                            if(!nodeMap.containsKey(versioned.getValue())) {
                                nodeMap.put(versioned.getValue(), new ArrayList<Node>());
                            }
                            nodeMap.get(versioned.getValue()).add(node);
                            if(!versionMap.containsKey(node)) {
                                versionMap.put(node, versioned.getVersion());
                            }
                        }
                    }
                }
                if(!verbose && !nodeMap.isEmpty()) {
                    Iterator<Entry<String, List<Node>>> iter = nodeMap.entrySet().iterator();
                    while(iter.hasNext()) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        String metaValue = (String) entry.getKey();
                        List<Node> nodeList = (List<Node>) entry.getValue();
                        for(Node node: nodeList) {
                            System.out.println(node.getHost() + ":" + node.getId() + "   "
                                               + versionMap.get(node));
                        }
                        System.out.println(metaValue);
                        System.out.println();
                    }
                }
            }
        }
    }

    /**
     * meta get-ro command
     */
    public static class SubCommandMetaGetRO extends AbstractAdminCommand {

        public static final String OPT_HEAD_META_GET_RO = "meta-get-ro";

        public static final String KEY_MAX_VERSION = "max-version";
        public static final String KEY_CURRENT_VERSION = "current-version";
        public static final String KEY_STORAGE_FORMAT = "storage-format";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_META_GET_RO, "read-only metadata keys to fetch")
                  .withOptionalArg()
                  .describedAs("ro-meta-key-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser);
            AdminParserUtils.acceptsStoreMultiple(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta get-ro - Get read-only metadata from nodes and stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta get-ro (<ro-meta-key-list> | all) -s <store-name-list> -u <url>");
            stream.println("              [-n <node-id-list> | --all-nodes]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid read-only meta keys are:");
            stream.println("    " + KEY_MAX_VERSION);
            stream.println("    " + KEY_CURRENT_VERSION);
            stream.println("    " + KEY_STORAGE_FORMAT);
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and gets read-only metadata.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> metaKeys = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            List<String> storeNames = null;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_GET_RO);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_META_GET_RO);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);

            // load parameters
            metaKeys = (List<String>) options.valuesOf(OPT_HEAD_META_GET_RO);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            if(metaKeys.size() == 1 && metaKeys.get(0).equals(METAKEY_ALL)) {
                metaKeys = Lists.newArrayList();
                metaKeys.add(KEY_MAX_VERSION);
                metaKeys.add(KEY_CURRENT_VERSION);
                metaKeys.add(KEY_STORAGE_FORMAT);
            }

            doMetaGetRO(adminClient, nodeIds, storeNames, metaKeys);
        }

        /**
         * Gets read-only metadata.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to fetch read-only metadata from
         * @param storeNames Stores names to fetch read-only metadata from
         * @param metaKeys List of read-only metadata to fetch
         * @throws IOException
         */
        public static void doMetaGetRO(AdminClient adminClient,
                                       Collection<Integer> nodeIds,
                                       List<String> storeNames,
                                       List<String> metaKeys) throws IOException {
            for(String key: metaKeys) {
                System.out.println("Metadata: " + key);
                if(!key.equals(KEY_MAX_VERSION) && !key.equals(KEY_CURRENT_VERSION)
                   && !key.equals(KEY_STORAGE_FORMAT)) {
                    System.out.println("  Invalid read-only metadata key: " + key);
                } else {
                    for(Integer nodeId: nodeIds) {
                        String hostName = adminClient.getAdminClientCluster()
                                                     .getNodeById(nodeId)
                                                     .getHost();
                        System.out.println("  Node: " + hostName + ":" + nodeId);
                        if(key.equals(KEY_MAX_VERSION)) {
                            Map<String, Long> mapStoreToROVersion = adminClient.readonlyOps.getROMaxVersion(nodeId,
                                                                                                            storeNames);
                            for(String storeName: mapStoreToROVersion.keySet()) {
                                System.out.println("    " + storeName + ":"
                                                   + mapStoreToROVersion.get(storeName));
                            }
                        } else if(key.equals(KEY_CURRENT_VERSION)) {
                            Map<String, Long> mapStoreToROVersion = adminClient.readonlyOps.getROCurrentVersion(nodeId,
                                                                                                            storeNames);
                            for(String storeName: mapStoreToROVersion.keySet()) {
                                System.out.println("    " + storeName + ":"
                                                   + mapStoreToROVersion.get(storeName));
                            }
                        } else if(key.equals(KEY_STORAGE_FORMAT)) {
                            Map<String, String> mapStoreToROFormat = adminClient.readonlyOps.getROStorageFormat(nodeId,
                                                                                                                storeNames);
                            for(String storeName: mapStoreToROFormat.keySet()) {
                                System.out.println("    " + storeName + ":"
                                                   + mapStoreToROFormat.get(storeName));
                            }
                        }
                    }
                }
                System.out.println();
            }
        }
    }

    /**
     * meta set command
     */
    public static class SubCommandMetaSet extends AbstractAdminCommand {

        public static final String OPT_HEAD_META_SET = "meta-set";
        public static final String KEY_OFFLINE = "offline";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_META_SET, "metadata key-file pairs")
                  .withOptionalArg()
                  .describedAs("meta-key>=<meta-file")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta set - Set metadata on nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta set <meta-key>=<meta-value>[,<meta-key2>=<meta-value2>] -u <url>");
            stream.println("           [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  To set one metadata, please specify one of the following:");
            stream.println("    " + MetadataStore.CLUSTER_KEY
                           + " (meta-value is cluster.xml file path)");
            stream.println("    " + MetadataStore.STORES_KEY
                           + " (meta-value is stores.xml file path)");
            stream.println("    " + MetadataStore.SLOP_STREAMING_ENABLED_KEY);
            stream.println("    " + MetadataStore.PARTITION_STREAMING_ENABLED_KEY);
            stream.println("    " + MetadataStore.READONLY_FETCH_ENABLED_KEY);
            stream.println("    " + MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY);
            stream.println("    " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML);
            stream.println("    " + MetadataStore.REBALANCING_STEAL_INFO);
            stream.println("    " + KEY_OFFLINE);
            stream.println("  To set a pair of metadata values, valid meta keys are:");
            stream.println("    " + MetadataStore.CLUSTER_KEY
                           + " (meta-value is cluster.xml file path)");
            stream.println("    " + MetadataStore.STORES_KEY
                           + " (meta-value is stores.xml file path)");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        private static void printMessage(List<String> storeNames, String message, PrintStream stream) {
            if(storeNames.size() > 0) {
                stream.println(message + Arrays.toString(storeNames.toArray()));
            }
        }

        private static void printChangeStoreSummary(List<StoreDefinition> oldStoreDefs,
                                              List<StoreDefinition> newStoreDefs,
                                                    PrintStream stream) {
            Set<String> storeNamesUnion = new HashSet<String>();
            Map<String, StoreDefinition> oldStoreDefinitionMap = new HashMap<String, StoreDefinition>();
            Map<String, StoreDefinition> newStoreDefinitionMap = new HashMap<String, StoreDefinition>();
            
            List<String> newStores = new ArrayList<String>();
            List<String> deletedStores = new ArrayList<String>();
            List<String> schemaChangeStores = new ArrayList<String>();
            List<String> replicationFactorChangeStores = new ArrayList<String>();
            List<String> allOtherChanges = new ArrayList<String>();
            List<String> noChanges = new ArrayList<String>();

            for(StoreDefinition storeDef: oldStoreDefs) {
                String storeName = storeDef.getName();
                storeNamesUnion.add(storeName);
                oldStoreDefinitionMap.put(storeName, storeDef);
            }
            for(StoreDefinition storeDef: newStoreDefs) {
                String storeName = storeDef.getName();
                storeNamesUnion.add(storeName);
                newStoreDefinitionMap.put(storeName, storeDef);
            }
            for(String storeName: storeNamesUnion) {
                StoreDefinition oldStoreDef = oldStoreDefinitionMap.get(storeName);
                StoreDefinition newStoreDef = newStoreDefinitionMap.get(storeName);
                if(oldStoreDef == null && newStoreDef != null) {
                    newStores.add(newStoreDef.getName());
                } else if(oldStoreDef != null && newStoreDef == null) {
                    deletedStores.add(oldStoreDef.getName());
                } else if(oldStoreDef.equals(newStoreDef)) {
                    noChanges.add(oldStoreDef.getName());
                } else {
                    boolean isRecognizedChange = false;
                    if(!oldStoreDef.getKeySerializer().equals(newStoreDef.getKeySerializer())
                       || !oldStoreDef.getValueSerializer()
                                      .equals(newStoreDef.getValueSerializer())) {
                        schemaChangeStores.add(newStoreDef.getName());
                        isRecognizedChange = true;
                    }

                    boolean isZoneReplicationChanged = false;
                    Map<Integer,Integer> oldZoneReplication = oldStoreDef.getZoneReplicationFactor();
                    Map<Integer,Integer> newZoneReplication = newStoreDef.getZoneReplicationFactor();
                    if((oldZoneReplication == null && newZoneReplication != null)
                       || (oldZoneReplication != null && newZoneReplication == null)) {
                        isZoneReplicationChanged = true;
                    }
                     else if(oldZoneReplication != null && newZoneReplication != null
                              && !oldZoneReplication.equals(newZoneReplication)) {
                        isZoneReplicationChanged = true;
                    }

                    if(oldStoreDef.getReplicationFactor() != newStoreDef.getReplicationFactor()
                       || oldStoreDef.getRequiredReads() != newStoreDef.getRequiredReads()
                       || oldStoreDef.getRequiredWrites() != newStoreDef.getRequiredWrites()
                       || oldStoreDef.getZoneCountReads() != newStoreDef.getZoneCountReads()
                       || oldStoreDef.getZoneCountWrites() != newStoreDef.getZoneCountWrites()
                       || isZoneReplicationChanged) {
                        replicationFactorChangeStores.add(newStoreDef.getName());
                        isRecognizedChange = true;
                    }
                    if(isRecognizedChange == false) {
                        allOtherChanges.add(newStoreDef.getName());
                    }
                }
            }

            printMessage(deletedStores,
                         "***WARNING!!!! Use delete store command, this will leave the cluster in an inconsistent state. Deleted Stores ",
                         stream);
            printMessage(newStores,
                         "***WARNING!!!! Use add store command, this will leave the cluster in an inconsistent state. Added Stores ",
                         stream);
            printMessage(noChanges, "Unchanged Stores ", stream);
            printMessage(schemaChangeStores, "Schema modified stores ", stream);
            printMessage(replicationFactorChangeStores,
                         "Replication Factor Changed Stores ",
                         stream);
            printMessage(allOtherChanges, "All Other Changes in Stores ", stream);
        }

        /**
         * Parses command-line and sets metadata.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws Exception
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws Exception {

            OptionParser parser = getParser();

            // declare parameters
            List<String> meta = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean confirm = false;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_SET);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_META_SET);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            meta = AdminToolUtils.getValueList((List<String>) options.valuesOf(OPT_HEAD_META_SET),
                                               "=");
            if(meta.size() != 2 && meta.size() != 4) {
                throw new VoldemortException("Invalid metakey-metafile pairs.");
            }
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Set metadata");
            System.out.println("Metadata:");
            for(Integer i = 0; i < meta.size(); i += 2) {
                System.out.println("  set \'" + meta.get(i) + "\' from file \'" + meta.get(i + 1)
                                   + "\'");
            }
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            if(meta.size() == 2) {
                String metaKey = meta.get(0), metaValue = meta.get(1);
                String metaFile = metaValue.replace("~", System.getProperty("user.home"));

                if(metaKey.equals(MetadataStore.STORES_KEY)) {
                    if(!Utils.isReadableFile(metaFile)) {
                        throw new VoldemortException("Stores definition xml file path incorrect");
                    }
                    StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                    List<StoreDefinition> newStoreDefs = mapper.readStoreList(new File(metaFile));
                    StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                    // original metadata
                    Integer nodeIdToGetStoreXMLFrom = nodeIds.iterator().next();
                    Versioned<String> storesXML = adminClient.metadataMgmtOps.getRemoteMetadata(nodeIdToGetStoreXMLFrom,
                                                                                                MetadataStore.STORES_KEY);

                    List<StoreDefinition> oldStoreDefs = mapper.readStoreList(new StringReader(storesXML.getValue()));

                    printChangeStoreSummary(oldStoreDefs, newStoreDefs, System.err);
                    if(!AdminToolUtils.askConfirm(confirm, "set metadata")) {
                        return;
                    }

                    // execute command
                    doMetaSet(adminClient, nodeIds, metaKey, mapper.writeStoreList(newStoreDefs));
                    if(!allNodes) {
                        System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                           + "although this set-metadata oprations only goes to node: ");
                        for(Integer nodeId: nodeIds) {
                            System.err.println(nodeId);
                        }
                    }
                    doMetaUpdateVersionsOnStores(adminClient, oldStoreDefs, newStoreDefs);
                } else {

                    // execute command
                    if(!AdminToolUtils.askConfirm(confirm, "set metadata")) {
                        return;
                    }

                    if(metaKey.equals(MetadataStore.CLUSTER_KEY)
                       || metaKey.equals(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML)) {
                        if(!Utils.isReadableFile(metaFile)) {
                            throw new VoldemortException("Cluster xml file path incorrect");
                        }
                        ClusterMapper mapper = new ClusterMapper();
                        Cluster newCluster = mapper.readCluster(new File(metaFile));
                        doMetaSet(adminClient, nodeIds, metaKey, mapper.writeCluster(newCluster));
                    } else if(metaKey.equals(MetadataStore.SLOP_STREAMING_ENABLED_KEY)
                              || metaKey.equals(MetadataStore.PARTITION_STREAMING_ENABLED_KEY)
                              || metaKey.equals(MetadataStore.READONLY_FETCH_ENABLED_KEY)
                              || metaKey.equals(MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)) {
                        doMetaSet(adminClient, nodeIds, metaKey, metaValue);
                    } else if(metaKey.equals(KEY_OFFLINE)) {
                        boolean setOffline = Boolean.parseBoolean(metaValue);
                        if(setOffline && nodeIds.size() > 1) {
                            throw new VoldemortException("Setting more than one node to offline is not allowed.");
                        }
                        for(Integer nodeId: nodeIds) {
                            adminClient.metadataMgmtOps.setRemoteOfflineState(nodeId, setOffline);
                        }
                    } else if(metaKey.equals(MetadataStore.REBALANCING_STEAL_INFO)) {
                        if(!Utils.isReadableFile(metaFile)) {
                            throw new VoldemortException("Rebalancing steal info file path incorrect");
                        }
                        String rebalancingStealInfoJsonString = FileUtils.readFileToString(new File(metaFile));
                        RebalancerState state = RebalancerState.create(rebalancingStealInfoJsonString);
                        doMetaSet(adminClient, nodeIds, metaKey, state.toJsonString());
                    } else {
                        throw new VoldemortException("Incorrect metadata key");
                    }
                }
            } else if(meta.size() == 4) {
                // set metadata pair cluster.xml, stores.xml
                String clusterFile, storesFile;

                if(meta.get(0).equals(MetadataStore.CLUSTER_KEY)
                   && meta.get(2).equals(MetadataStore.STORES_KEY)) {
                    clusterFile = meta.get(1);
                    storesFile = meta.get(3);
                } else if(meta.get(0).equals(MetadataStore.STORES_KEY)
                          && meta.get(2).equals(MetadataStore.CLUSTER_KEY)) {
                    storesFile = meta.get(1);
                    clusterFile = meta.get(3);
                } else {
                    throw new VoldemortException("meta set-pair keys must be <cluster.xml, stores.xml>");
                }

                clusterFile = clusterFile.replace("~", System.getProperty("user.home"));
                storesFile = storesFile.replace("~", System.getProperty("user.home"));

                ClusterMapper clusterMapper = new ClusterMapper();
                StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();

                // original metadata
                Integer nodeIdToGetStoreXMLFrom = nodeIds.iterator().next();
                Versioned<String> storesXML = adminClient.metadataMgmtOps.getRemoteMetadata(nodeIdToGetStoreXMLFrom,
                                                                                            MetadataStore.STORES_KEY);

                List<StoreDefinition> oldStoreDefs = storeDefsMapper.readStoreList(new StringReader(storesXML.getValue()));

                if(!Utils.isReadableFile(clusterFile)) {
                    throw new VoldemortException("Cluster xml file path incorrect");
                }
                Cluster cluster = clusterMapper.readCluster(new File(clusterFile));

                if(!Utils.isReadableFile(storesFile)) {
                    throw new VoldemortException("Stores definition xml file path incorrect");
                }
                List<StoreDefinition> newStoreDefs = storeDefsMapper.readStoreList(new File(storesFile));

                StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                printChangeStoreSummary(oldStoreDefs, newStoreDefs, System.err);

                if(!AdminToolUtils.askConfirm(confirm, "set metadata")) {
                    return;
                }

                // execute command
                doMetaSetPair(adminClient,
                              nodeIds,
                              clusterMapper.writeCluster(cluster),
                              storeDefsMapper.writeStoreList(newStoreDefs));
                if(!allNodes) {
                    System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                       + "although this set-metadata oprations only goes to node: ");
                    for(Integer nodeId: nodeIds) {
                        System.err.println(nodeId);
                    }
                }
                doMetaUpdateVersionsOnStores(adminClient, oldStoreDefs, newStoreDefs);
            }
        }

        /**
         * Sets <cluster.xml,stores.xml> metadata pair atomically.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to set metadata
         * @param clusterValue Cluster value to set
         * @param storesValue Stores value to set
         */
        public static void doMetaSetPair(AdminClient adminClient,
                                         List<Integer> nodeIds,
                                         Object clusterValue,
                                         Object storesValue) {
            VectorClock updatedClusterVersion = null;
            VectorClock updatedStoresVersion = null;
            for(Integer nodeId: nodeIds) {
                if(updatedClusterVersion == null && updatedStoresVersion == null) {
                    updatedClusterVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                        MetadataStore.CLUSTER_KEY)
                                                                                     .getVersion();
                    updatedStoresVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                       MetadataStore.STORES_KEY)
                                                                                    .getVersion();
                } else {
                    updatedClusterVersion = updatedClusterVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                                                    MetadataStore.CLUSTER_KEY)
                                                                                                                 .getVersion());
                    updatedStoresVersion = updatedStoresVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                                                  MetadataStore.STORES_KEY)
                                                                                                               .getVersion());
                }

                // TODO: This will work for now but we should take a step back
                // and
                // think about a uniform clock for the metadata values.
                updatedClusterVersion = updatedClusterVersion.incremented(nodeIds.iterator().next(),
                                                                          System.currentTimeMillis());
                updatedStoresVersion = updatedStoresVersion.incremented(nodeIds.iterator().next(),
                                                                        System.currentTimeMillis());
            }
            adminClient.metadataMgmtOps.updateRemoteMetadataPair(nodeIds,
                                                                 MetadataStore.CLUSTER_KEY,
                                                                 Versioned.value(clusterValue.toString(),
                                                                                 updatedClusterVersion),
                                                                 MetadataStore.STORES_KEY,
                                                                 Versioned.value(storesValue.toString(),
                                                                                 updatedStoresVersion));
        }

        /**
         * Updates metadata versions on stores.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param oldStoreDefs List of old store definitions
         * @param newStoreDefs List of new store definitions
         */
        public static void doMetaUpdateVersionsOnStores(AdminClient adminClient,
                                                        List<StoreDefinition> oldStoreDefs,
                                                        List<StoreDefinition> newStoreDefs) {
            Set<String> storeNamesUnion = new HashSet<String>();
            Map<String, StoreDefinition> oldStoreDefinitionMap = new HashMap<String, StoreDefinition>();
            Map<String, StoreDefinition> newStoreDefinitionMap = new HashMap<String, StoreDefinition>();
            List<String> storesChanged = new ArrayList<String>();
            for(StoreDefinition storeDef: oldStoreDefs) {
                String storeName = storeDef.getName();
                storeNamesUnion.add(storeName);
                oldStoreDefinitionMap.put(storeName, storeDef);
            }
            for(StoreDefinition storeDef: newStoreDefs) {
                String storeName = storeDef.getName();
                storeNamesUnion.add(storeName);
                newStoreDefinitionMap.put(storeName, storeDef);
            }
            for(String storeName: storeNamesUnion) {
                StoreDefinition oldStoreDef = oldStoreDefinitionMap.get(storeName);
                StoreDefinition newStoreDef = newStoreDefinitionMap.get(storeName);
                if(oldStoreDef == null && newStoreDef != null || oldStoreDef != null
                   && newStoreDef == null || oldStoreDef != null && newStoreDef != null
                   && !oldStoreDef.equals(newStoreDef)) {
                    storesChanged.add(storeName);
                }
            }
            System.out.println("Updating metadata version for the following stores: "
                               + storesChanged);
            try {
                adminClient.metadataMgmtOps.updateMetadataversion(adminClient.getAdminClientCluster()
                                                                             .getNodeIds(),
                                                                  storesChanged);
            } catch(Exception e) {
                System.err.println("Error while updating metadata version for the specified store.");
            }
        }
    }

    /**
     * meta sync-version command
     */
    public static class SubCommandMetaSyncVersion extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta sync-version - Synchronize metadata versions across all nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta sync-version -u <url> [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and synchronizes metadata versions across all
         * nodes.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Synchronize metadata versions across all nodes");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = all nodes");

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            AdminToolUtils.assertServerNotInRebalancingState(adminClient);

            Versioned<Properties> versionedProps = mergeAllVersions(adminClient);

            printVersions(versionedProps);

            // execute command
            if(!AdminToolUtils.askConfirm(confirm,
                                          "do you want to synchronize metadata versions to all node"))
                return;

            adminClient.metadataMgmtOps.setMetadataVersion(versionedProps);
        }

        private static void printVersions(Versioned<Properties> versionedProps) {
            Version version = versionedProps.getVersion();
            Properties props = versionedProps.getValue();
            System.out.println("Version value " + version);
            for(String propName: props.stringPropertyNames()) {
                SubCommandMetaCheckVersion.printProperty(propName, props.getProperty(propName));
            }
        }

        private static Versioned<Properties> mergeAllVersions(AdminClient adminClient) {
            Properties props = new Properties();
            VectorClock version = new VectorClock();

            for(Integer nodeId: adminClient.getAdminClientCluster().getNodeIds()) {
                Versioned<Properties> versionedProp = doMetaGetVersionsForNode_ExitOnError(adminClient,
                                                                                           nodeId);
                Properties newProps = versionedProp.getValue();
                VectorClock newVersion = (VectorClock) versionedProp.getVersion();
                
                version = version.merge(newVersion);
                props = MetadataVersionStoreUtils.mergeVersions(props, newProps);
            }

            return new Versioned<Properties>(props, version);
        }
    }

    /**
     * meta check-version command
     */
    public static class SubCommandMetaCheckVersion extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            AdminParserUtils.acceptsUrl(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  meta check-version - Verify metadata versions on all the cluster nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  meta check-version -u <url>");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and verifies metadata versions on all the cluster
         * nodes
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            String url = null;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            doMetaCheckVersion(adminClient);
        }

        public static Long tryParse(String s) {
            try {
                return Long.parseLong(s);
            } catch(NumberFormatException e) {
                return -1L;
            }
        }

        private static void printProperty(String propName, String propValue) {
            System.out.print(propName + "=" + propValue);
            Long lValue = tryParse(propValue);
            if(lValue > 0) {
                Date date = new Date(lValue);
                System.out.print(" [ " + date.toString() + " ] ");
            }

            System.out.println();
        }

        private static void printProperty(String propName, String propValue, List<Integer> nodes) {
            System.out.println("**************************** Node(s): "
                               + Arrays.toString(nodes.toArray())
                               + " ****************************");
            printProperty(propName, propValue);
        }

        private static void printProperties(Properties props) {
            for(String propName: props.stringPropertyNames()) {
                String propValue = props.getProperty(propName);
                printProperty(propName, propValue);
            }
            System.out.println();
        }

        /**
         * Verifies metadata versions for all the cluster nodes
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * 
         */
        public static void doMetaCheckVersion(AdminClient adminClient) {
            Map<Properties, List<Integer>> versionsNodeMap = new HashMap<Properties, List<Integer>>();

            for(Integer nodeId: adminClient.getAdminClientCluster().getNodeIds()) {
                Versioned<Properties> versionedProp = doMetaGetVersionsForNode_ExitOnError(adminClient,
                                                                                           nodeId);
                Properties props = versionedProp.getValue();
                if(versionsNodeMap.containsKey(props) == false) {
                    versionsNodeMap.put(props, new ArrayList<Integer>());
                }
                versionsNodeMap.get(props).add(nodeId);
            }

            if(versionsNodeMap.keySet().size() <= 0) {
                System.err.println("No Versions found in the system store... something seriously wrong");
            } else if(versionsNodeMap.keySet().size() == 1) {
                System.err.println("All the nodes have the same metadata versions.");
                printProperties(versionsNodeMap.keySet().iterator().next());
            } else {
                System.err.println("Mismatching versions detected !!! . All are supposed to be written by the same client "
                                   + ""
                                   + " and hence they should exactly match but something different, let us analyze deeper ");
                Map<String, Map<String, List<Integer>>> propertyValueMap = new HashMap<String, Map<String, List<Integer>>>();
                for(Entry<Properties, List<Integer>> entry: versionsNodeMap.entrySet()) {
                    System.out.println("**************************** Node(s): "
                                       + Arrays.toString(entry.getValue().toArray())
                                       + " ****************************");
                    Properties props = entry.getKey();
                    printProperties(props);

                    for(String propName: props.stringPropertyNames()) {
                        String propValue = props.getProperty(propName);

                        if(propertyValueMap.containsKey(propName) == false) {
                            propertyValueMap.put(propName, new HashMap<String, List<Integer>>());
                        }
                        Map<String, List<Integer>> valuetoNodeMap = propertyValueMap.get(propName);
                        if(valuetoNodeMap.containsKey(propValue) == false) {
                            valuetoNodeMap.put(propValue, new ArrayList<Integer>());
                        }
                        valuetoNodeMap.get(propValue).addAll(entry.getValue());
                    }
                }
                
                System.out.println("########## Properties discrepancy report ########");
                for(Entry<String, Map<String, List<Integer>>> entry: propertyValueMap.entrySet()) {
                    Map<String, List<Integer>> valueToNodeMap = entry.getValue();
                    String propName = entry.getKey();
                    List<Integer> allNodeIds = new ArrayList<Integer>();
                    allNodeIds.addAll(adminClient.getAdminClientCluster().getNodeIds());
                    
                    List<Integer> nodesWithValues = new ArrayList<Integer>();
                    if(valueToNodeMap.size() != 1) {
                        System.out.println("Properties with multiple values");
                        for(Entry<String, List<Integer>> valueToNodeEntry: valueToNodeMap
                                                                              .entrySet()) {
                            String propValue = valueToNodeEntry.getKey();
                            nodesWithValues.addAll(valueToNodeEntry.getValue());
                            printProperty(propName, propValue, valueToNodeEntry.getValue());
                        }
                    } else  {
                        Map.Entry<String, List<Integer>> valueToNodeEntry = valueToNodeMap.entrySet()
                                                                                          .iterator()
                                                                                          .next();

                        nodesWithValues.addAll(valueToNodeEntry.getValue());
                        if(nodesWithValues.size() < allNodeIds.size()) {
                            String propValue = valueToNodeEntry.getKey();
                            printProperty(propName, propValue, valueToNodeEntry.getValue());
                        }
                    }

                    allNodeIds.removeAll(nodesWithValues);
                    if(allNodeIds.size() > 0) {
                        System.out.println("The Property " + propName + " is present in the nodes "
                                           + Arrays.toString(nodesWithValues.toArray())
                                           + " but missing from the nodes "
                                           + Arrays.toString(allNodeIds.toArray()));
                    }
                }
            }
        }
    }

    /**
     * Sets metadata.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds Node ids to set metadata
     * @param metaKey Metadata key to set
     * @param metaValue Metadata value to set
     */
    public static void doMetaSet(AdminClient adminClient,
                                 List<Integer> nodeIds,
                                 String metaKey,
                                 String metaValue) {
        adminClient.metadataMgmtOps.updateRemoteMetadata(nodeIds, metaKey, metaValue);
    }
    
    private static Versioned<Properties> doMetaGetVersionsForNode_ExitOnError(AdminClient adminClient,
                                                                   Integer nodeId) {

        try {
            return doMetaGetVersionsForNode(adminClient, nodeId);
        } catch(IOException e) {
            System.err.println("Error while retrieving Metadata versions from node : " + nodeId
                               + ". Exception = " + e.getMessage() + " \n");
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    /**
     * Gets metadata versions for a given node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to get metadata version from
     */
    private static Versioned<Properties> doMetaGetVersionsForNode(AdminClient adminClient,
                                                                           Integer nodeId)
            throws IOException {

        ByteArray keyArray = new ByteArray(SystemStoreConstants.VERSIONS_METADATA_KEY.getBytes("UTF8"));
        List<Versioned<byte[]>> valueObj = adminClient.storeOps.getNodeKey(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                           nodeId,
                                                                           keyArray);

        if(valueObj.size() != 1) {
            throw new IOException("Expected one value for the key "
                                  + SystemStoreConstants.VERSIONS_METADATA_KEY
                                  + " on node id " + nodeId + " but found " + valueObj.size());
        }

        System.out.println(" Node : " + nodeId + " Version : " + valueObj.get(0).getVersion());
        return MetadataVersionStoreUtils.parseProperties(valueObj);
    }
}

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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.io.FileUtils;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
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
            if(metaKeys.size() == 1 && metaKeys.get(0).equals(METAKEY_ALL)) {
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
                    System.err.println("Nodes with same value for " + keyName + ". Id :"
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
                    System.err.println("key " + keyName + " is missing in the Node " + nodeName);
                }
            }
            return checkResult;
        }

        /**
         * Checks if metadata is consistent across all nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param metaKeys List of metakeys to check
         * 
         */
        public static void doMetaCheck(AdminClient adminClient, List<String> metaKeys) {
            for(String key: metaKeys) {
                Map<String, Map<Object, List<String>>> storeNodeValueMap = new HashMap<String, Map<Object, List<String>>>();
                Map<Object, List<String>> metadataNodeValueMap = new HashMap<Object, List<String>>();
                Collection<Node> allNodes = adminClient.getAdminClientCluster().getNodes();
                Collection<String> allNodeNames = new ArrayList<String>();

                Boolean checkResult = true;
                for(Node node: allNodes) {
                    String nodeName = "Host '" + node.getHost() + "' : ID " + node.getId();
                    allNodeNames.add(nodeName);

                    System.out.println("processing " + nodeName);

                    Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                key);
                    if(versioned == null || versioned.getValue() == null) {
                        throw new VoldemortException("Value returned from node " + node.getId()
                                                     + " was null");
                    } else if(key.compareTo(MetadataStore.STORES_KEY) == 0) {
                        List<StoreDefinition> storeDefinitions = new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue()));
                        for(StoreDefinition storeDef: storeDefinitions) {
                            String storeName = storeDef.getName();
                            if(storeNodeValueMap.containsKey(storeName) == false) {
                                storeNodeValueMap.put(storeName,
                                                      new HashMap<Object, List<String>>());
                            }
                            Map<Object, List<String>> storeDefMap = storeNodeValueMap.get(storeName);
                            addMetadataValue(storeDefMap, storeDef, nodeName);
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
                    checkResult &= checkDiagnostics(key, metadataNodeValueMap, allNodeNames);
                }

                if(storeNodeValueMap.size() > 0) {
                    for(Map.Entry<String, Map<Object, List<String>>> storeNodeValueEntry: storeNodeValueMap.entrySet()) {
                        String storeName = storeNodeValueEntry.getKey();
                        Map<Object, List<String>> storeDefMap = storeNodeValueEntry.getValue();
                        checkResult &= checkDiagnostics(storeName, storeDefMap, allNodeNames);
                    }
                }

                System.out.println(key + " metadata check : " + (checkResult ? "PASSED" : "FAILED"));
            }
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

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "set metadata")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            if(meta.size() == 2) {
                String metaKey = meta.get(0), metaValue = meta.get(1);
                String metaFile = metaValue.replace("~", System.getProperty("user.home"));

                if(metaKey.equals(MetadataStore.CLUSTER_KEY)
                   || metaKey.equals(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML)) {
                    if(!Utils.isReadableFile(metaFile)) {
                        throw new VoldemortException("Cluster xml file path incorrect");
                    }
                    ClusterMapper mapper = new ClusterMapper();
                    Cluster newCluster = mapper.readCluster(new File(metaFile));
                    doMetaSet(adminClient, nodeIds, metaKey, mapper.writeCluster(newCluster));
                } else if(metaKey.equals(MetadataStore.STORES_KEY)) {
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

                    doMetaSet(adminClient, nodeIds, metaKey, mapper.writeStoreList(newStoreDefs));
                    if(!allNodes) {
                        System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                           + "although this set-metadata oprations only goes to node: ");
                        for(Integer nodeId: nodeIds) {
                            System.err.println(nodeId);
                        }
                    }
                    doMetaUpdateVersionsOnStores(adminClient, oldStoreDefs, newStoreDefs);
                } else if(metaKey.equals(MetadataStore.SLOP_STREAMING_ENABLED_KEY)
                          || metaKey.equals(MetadataStore.PARTITION_STREAMING_ENABLED_KEY)
                          || metaKey.equals(MetadataStore.READONLY_FETCH_ENABLED_KEY)) {
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
                adminClient.metadataMgmtOps.updateMetadataversion(storesChanged);
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
            // required options
            AdminParserUtils.acceptsNodeSingle(parser);
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
            stream.println("  meta sync-version -n <base-node-id> -u <url> [--confirm]");
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

            // declare parameters
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Synchronize metadata versions across all nodes");
            System.out.println("Base node id = " + nodeId);
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = all nodes");

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "synchronize metadata version"))
                return;

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            AdminToolUtils.assertServerNotInRebalancingState(adminClient);

            doMetaSyncVersion(adminClient, nodeId);
        }

        /**
         * Synchronizes metadata versions across all nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeId Base node id to get metadata version from
         * 
         */
        public static void doMetaSyncVersion(AdminClient adminClient, Integer nodeId) {
            try {
                Properties props = doMetaGetVersionsForNode(adminClient, nodeId);
                adminClient.metadataMgmtOps.setMetadataversion(props);
                System.out.println("Metadata versions synchronized successfully.");
            } catch(IOException e) {
                System.err.println("Error while retrieving Metadata versions from node : " + nodeId
                                   + ". Exception = " + e.getMessage() + " \n");
                e.printStackTrace();
                System.exit(-1);
            }
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

        /**
         * Verifies metadata versions for all the cluster nodes
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * 
         */
        public static void doMetaCheckVersion(AdminClient adminClient) {
            Map<Properties, Integer> versionsNodeMap = new HashMap<Properties, Integer>();

            for(Integer nodeId: adminClient.getAdminClientCluster().getNodeIds()) {
                Properties props = null;
                try {
                    props = doMetaGetVersionsForNode(adminClient, nodeId);
                } catch(IOException e) {
                    System.err.println("Error while retrieving Metadata versions from node : "
                                       + nodeId + ". Exception = " + e.getMessage() + " \n");
                    e.printStackTrace();
                    System.exit(-1);
                }
                versionsNodeMap.put(props, nodeId);
            }

            if(versionsNodeMap.keySet().size() > 1) {
                System.err.println("Mismatching versions detected !!!");
                for(Entry<Properties, Integer> entry: versionsNodeMap.entrySet()) {
                    System.out.println("**************************** Node: " + entry.getValue()
                                       + " ****************************");
                    System.out.println(entry.getKey());
                }
            } else {
                System.err.println("All the nodes have the same metadata versions."
                                   + Arrays.toString(versionsNodeMap.keySet().toArray()));
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

    /**
     * Gets metadata versions for a given node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to get metadata version from
     */
    private static Properties doMetaGetVersionsForNode(AdminClient adminClient, Integer nodeId)
            throws IOException {

        ByteArray keyArray = new ByteArray(MetadataVersionStoreUtils.VERSIONS_METADATA_KEY.getBytes("UTF8"));
        List<Versioned<byte[]>> valueObj = adminClient.storeOps.getNodeKey(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                           nodeId,
                                                                           keyArray);

        if(valueObj.size() != 1) {
            throw new IOException("Expected one value for the key "
                                  + MetadataVersionStoreUtils.VERSIONS_METADATA_KEY
                                  + " on node id " + nodeId + " but found " + valueObj.size());
        }

        Properties props = new Properties();
        props.load(new ByteArrayInputStream(valueObj.get(0).getValue()));
        return props;

    }
}

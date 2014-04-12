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

package voldemort.tools.admin;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
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
import voldemort.serialization.Serializer;
import voldemort.serialization.StringSerializer;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
        args = AdminUtils.copyArrayCutFirst(args);
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
    private static class SubCommandMetaCheck extends AbstractAdminCommand {

        private static final String OPT_HEAD_META_CHECK = "meta-check";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            parser.accepts(OPT_HEAD_META_CHECK, "metadata keys to be checked")
                  .withRequiredArg()
                  .describedAs("meta-key-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser, true);
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
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(OPT_HEAD_META_CHECK);
            requiredAll.add(AdminParserUtils.OPT_URL);

            // declare parameters
            List<String> metaKeys = null;
            String url = null;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_CHECK);
            OptionSet options = parser.parse(args);

            // load parameters
            metaKeys = (List<String>) options.valuesOf(OPT_HEAD_META_CHECK);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);

            // execute command
            if(metaKeys.size() == 1 && metaKeys.get(0).compareTo(METAKEY_ALL) == 0) {
                metaKeys = Lists.newArrayList();
                metaKeys.add(MetadataStore.CLUSTER_KEY);
                metaKeys.add(MetadataStore.STORES_KEY);
                metaKeys.add(MetadataStore.SERVER_STATE_KEY);
            }

            AdminClient adminClient = AdminUtils.getAdminClient(url);

            doMetaCheck(adminClient, metaKeys);
        }

        /**
         * Checks if metadata is consistent across all nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param metaKeys List of metakeys to check
         * 
         */
        private static void doMetaCheck(AdminClient adminClient, List<String> metaKeys) {
            Set<Object> metaValues = Sets.newHashSet();
            for(String key: metaKeys) {
                metaValues.clear();
                System.out.println("Metadata: " + key);
                for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                    System.out.println(node.getHost() + ":" + node.getId());
                    Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                key);
                    if(versioned == null || versioned.getValue() == null) {
                        throw new VoldemortException("Value returned from node " + node.getId()
                                                     + " was null");
                    } else {
                        if(key.compareTo(MetadataStore.CLUSTER_KEY) == 0
                           || key.compareTo(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML) == 0) {
                            metaValues.add(new ClusterMapper().readCluster(new StringReader(versioned.getValue())));
                        } else if(key.compareTo(MetadataStore.STORES_KEY) == 0) {
                            metaValues.add(new StoreDefinitionsMapper().readStoreList(new StringReader(versioned.getValue())));
                        } else if(key.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                            metaValues.add(VoldemortState.valueOf(versioned.getValue()));
                        } else {
                            throw new VoldemortException("Incorrect metadata key");
                        }
                    }
                }
                if(metaValues.size() == 1) {
                    System.out.println(key + " on all nodes are the same.");
                } else {
                    System.out.println("different " + key + " found!");
                }
            }
        }
    }

    /**
     * meta clear-rebalance command
     */
    private static class SubCommandMetaClearRebalance extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser, false); // either
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> optionalNode = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_URL);
            optionalNode.add(AdminParserUtils.OPT_NODE);
            optionalNode.add(AdminParserUtils.OPT_ALL_NODES);

            // declare parameters
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM))
                confirm = true;

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkOptionalOne(options, optionalNode);

            // execute command
            if(!AdminUtils.askConfirm(confirm, "remove metadata related to rebalancing"))
                return;
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            doMetaClearRebalance(adminClient, nodes);
        }

        /**
         * Removes metadata related to rebalancing.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes List of nodes to clear metadata after rebalancing
         * 
         */
        private static void doMetaClearRebalance(AdminClient adminClient, Collection<Node> nodes) {
            System.out.println("Setting " + MetadataStore.SERVER_STATE_KEY + " to "
                               + MetadataStore.VoldemortState.NORMAL_SERVER);
            doMetaSet(adminClient,
                      nodes,
                      MetadataStore.SERVER_STATE_KEY,
                      MetadataStore.VoldemortState.NORMAL_SERVER.toString());
            RebalancerState state = RebalancerState.create("[]");
            System.out.println("Cleaning up " + MetadataStore.REBALANCING_STEAL_INFO + " to "
                               + state.toJsonString());
            doMetaSet(adminClient,
                      nodes,
                      MetadataStore.REBALANCING_STEAL_INFO,
                      state.toJsonString());
            System.out.println("Cleaning up " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML
                               + " to empty string");
            doMetaSet(adminClient, nodes, MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, "");
        }
    }

    /**
     * meta get command
     */
    private static class SubCommandMetaGet extends AbstractAdminCommand {

        public static final String OPT_HEAD_META_GET = "meta-get";
        public static final String OPT_VERBOSE = "verbose";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            parser.accepts(OPT_HEAD_META_GET, "metadata keys to fetch")
                  .withRequiredArg()
                  .describedAs("meta-key-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsDir(parser, false);
            AdminParserUtils.acceptsNodeMultiple(parser, false); // either
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> optionalNode = Lists.newArrayList();
            requiredAll.add(OPT_HEAD_META_GET);
            requiredAll.add(AdminParserUtils.OPT_URL);
            optionalNode.add(AdminParserUtils.OPT_NODE);
            optionalNode.add(AdminParserUtils.OPT_ALL_NODES);

            // declare parameters
            List<String> metaKeys = null;
            String url = null;
            String dir = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean verbose = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_GET);
            OptionSet options = parser.parse(args);

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
            if(options.has(OPT_VERBOSE))
                verbose = true;

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkOptionalOne(options, optionalNode);

            // execute command
            File directory = AdminUtils.createDir(dir);
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            if(metaKeys.size() == 1 && metaKeys.get(0).compareTo(METAKEY_ALL) == 0) {
                metaKeys = Lists.newArrayList();
                for(Object key: MetadataStore.METADATA_KEYS) {
                    metaKeys.add((String) key);
                }
            }

            doMetaGet(adminClient, nodes, metaKeys, directory, verbose);
        }

        /**
         * Gets metadata.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes List of nodes to fetch metadata from
         * @param metaKeys List of metadata to fetch
         * @param directory Directory to output to
         * @param verbose Tells whether to print metadata verbosely
         * @throws IOException
         */
        @SuppressWarnings({ "unchecked", "cast", "rawtypes" })
        private static void doMetaGet(AdminClient adminClient,
                                      Collection<Node> nodes,
                                      List<String> metaKeys,
                                      File directory,
                                      Boolean verbose) throws IOException {
            Map<String, List<Node>> nodeMap = new HashMap<String, List<Node>>();
            Map<Node, Version> versionMap = new HashMap<Node, Version>();
            for(String key: metaKeys) {
                nodeMap.clear();
                versionMap.clear();
                System.out.println("Metadata: " + key);
                for(Node node: nodes) {
                    Versioned<String> versioned = null;
                    try {
                        versioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(), key);
                    } catch(Exception e) {
                        System.out.println("Error in retrieving " + e.getMessage());
                        System.out.println();
                        continue;
                    }
                    if(directory != null) {
                        FileUtils.writeStringToFile(new File(directory, key + "_" + node.getId()),
                                                    ((versioned == null) ? ""
                                                                        : versioned.getValue()));
                    } else {
                        if(verbose) {
                            System.out.println(node.getHost() + ":" + node.getId());
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
    private static class SubCommandMetaSet extends AbstractAdminCommand {

        public static final String OPT_HEAD_META_SET = "meta-set";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            parser.accepts(OPT_HEAD_META_SET, "metadata key-file pairs")
                  .withRequiredArg()
                  .describedAs("meta-key>=<meta-file")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser, false); // either
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
            stream.println("  meta set <meta-key>=<meta-file>[,<meta-key2>=<meta-file2>] -u <url>");
            stream.println("           [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  To set one metadata, please specify one of the following:");
            stream.println("    " + MetadataStore.CLUSTER_KEY);
            stream.println("    " + MetadataStore.REBALANCING_SOURCE_CLUSTER_XML);
            stream.println("    " + MetadataStore.SERVER_STATE_KEY);
            stream.println("    " + MetadataStore.STORES_KEY);
            stream.println("    " + MetadataStore.REBALANCING_STEAL_INFO);
            stream.println("  To set a pair of metadata values, valid meta keys are:");
            stream.println("    " + MetadataStore.CLUSTER_KEY);
            stream.println("    " + MetadataStore.STORES_KEY);
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> optionalNode = Lists.newArrayList();
            requiredAll.add(OPT_HEAD_META_SET);
            requiredAll.add(AdminParserUtils.OPT_URL);
            optionalNode.add(AdminParserUtils.OPT_NODE);
            optionalNode.add(AdminParserUtils.OPT_ALL_NODES);

            // declare parameters
            List<String> meta = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_META_SET);
            OptionSet options = parser.parse(args);

            // load parameters
            meta = AdminUtils.getValueList((List<String>) options.valuesOf(OPT_HEAD_META_SET), "=");
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

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkOptionalOne(options, optionalNode);

            // execute command
            if(!AdminUtils.askConfirm(confirm, "set metadata")) {
                return;
            }

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            if(meta.size() == 2) {
                String metaKey = meta.get(0), metaFile = meta.get(1);
                metaFile = metaFile.replace("~", System.getProperty("user.home"));

                if(metaKey.compareTo(MetadataStore.CLUSTER_KEY) == 0
                   || metaKey.compareTo(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML) == 0) {
                    if(!Utils.isReadableFile(metaFile)) {
                        throw new VoldemortException("Cluster xml file path incorrect");
                    }
                    ClusterMapper mapper = new ClusterMapper();
                    Cluster newCluster = mapper.readCluster(new File(metaFile));
                    doMetaSet(adminClient, nodes, metaKey, mapper.writeCluster(newCluster));
                } else if(metaKey.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                    VoldemortState newState = VoldemortState.valueOf(metaFile);
                    doMetaSet(adminClient, nodes, metaKey, newState.toString());
                } else if(metaKey.compareTo(MetadataStore.STORES_KEY) == 0) {
                    if(!Utils.isReadableFile(metaFile)) {
                        throw new VoldemortException("Stores definition xml file path incorrect");
                    }
                    StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                    List<StoreDefinition> newStoreDefs = mapper.readStoreList(new File(metaFile));
                    StoreDefinitionUtils.validateSchemasAsNeeded(newStoreDefs);

                    // original metadata
                    Integer nodeIdToGetStoreXMLFrom = nodes.iterator().next().getId();
                    Versioned<String> storesXML = adminClient.metadataMgmtOps.getRemoteMetadata(nodeIdToGetStoreXMLFrom,
                                                                                                MetadataStore.STORES_KEY);

                    List<StoreDefinition> oldStoreDefs = mapper.readStoreList(new StringReader(storesXML.getValue()));

                    doMetaSet(adminClient, nodes, metaKey, mapper.writeStoreList(newStoreDefs));
                    if(!allNodes) {
                        System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                           + "although this set-metadata oprations only goes to node: ");
                        for(Node node: nodes) {
                            System.err.println(node.getId());
                        }
                    }
                    doMetaUpdateVersionsOnStores(adminClient, oldStoreDefs, newStoreDefs);
                } else if(metaKey.compareTo(MetadataStore.REBALANCING_STEAL_INFO) == 0) {
                    if(!Utils.isReadableFile(metaFile)) {
                        throw new VoldemortException("Rebalancing steal info file path incorrect");
                    }
                    String rebalancingStealInfoJsonString = FileUtils.readFileToString(new File(metaFile));
                    RebalancerState state = RebalancerState.create(rebalancingStealInfoJsonString);
                    doMetaSet(adminClient, nodes, metaKey, state.toJsonString());
                } else {
                    throw new VoldemortException("Incorrect metadata key");
                }
            } else if(meta.size() == 4) {
                // set metadata pair cluster.xml, stores.xml
                String clusterFile, storesFile;

                if(meta.get(0).compareTo(MetadataStore.CLUSTER_KEY) == 0
                   && meta.get(2).compareTo(MetadataStore.STORES_KEY) == 0) {
                    clusterFile = meta.get(1);
                    storesFile = meta.get(3);
                } else if(meta.get(0).compareTo(MetadataStore.STORES_KEY) == 0
                          && meta.get(2).compareTo(MetadataStore.CLUSTER_KEY) == 0) {
                    storesFile = meta.get(1);
                    clusterFile = meta.get(3);
                } else {
                    throw new VoldemortException("meta set-pair keys should be <cluster.xml, stores.xml>");
                }

                clusterFile = clusterFile.replace("~", System.getProperty("user.home"));
                storesFile = storesFile.replace("~", System.getProperty("user.home"));

                ClusterMapper clusterMapper = new ClusterMapper();
                StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();

                // original metadata
                Integer nodeIdToGetStoreXMLFrom = nodes.iterator().next().getId();
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
                              nodes,
                              clusterMapper.writeCluster(cluster),
                              storeDefsMapper.writeStoreList(newStoreDefs));
                if(!allNodes) {
                    System.err.println("WARNING: Metadata version update of stores goes to all servers, "
                                       + "although this set-metadata oprations only goes to node: ");
                    for(Node node: nodes) {
                        System.err.println(node.getId());
                    }
                }
                doMetaUpdateVersionsOnStores(adminClient, oldStoreDefs, newStoreDefs);
            }
        }

        /**
         * Sets <cluster.xml,stores.xml> metadata pair atomically.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes List of nodes to set metadata
         * @param clusterValue Cluster value to set
         * @param storesValue Stores value to set
         */
        private static void doMetaSetPair(AdminClient adminClient,
                                          Collection<Node> nodes,
                                          Object clusterValue,
                                          Object storesValue) {
            List<Integer> nodeIds = Lists.newArrayList();
            VectorClock updatedClusterVersion = null;
            VectorClock updatedStoresVersion = null;
            for(Node node: nodes) {
                nodeIds.add(node.getId());
                if(updatedClusterVersion == null && updatedStoresVersion == null) {
                    updatedClusterVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                        MetadataStore.CLUSTER_KEY)
                                                                                     .getVersion();
                    updatedStoresVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                       MetadataStore.STORES_KEY)
                                                                                    .getVersion();
                } else {
                    updatedClusterVersion = updatedClusterVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                                    MetadataStore.CLUSTER_KEY)
                                                                                                                 .getVersion());
                    updatedStoresVersion = updatedStoresVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                                  MetadataStore.STORES_KEY)
                                                                                                               .getVersion());
                }

                // TODO: This will work for now but we should take a step back
                // and
                // think about a uniform clock for the metadata values.
                updatedClusterVersion = updatedClusterVersion.incremented(nodeIds.get(0),
                                                                          System.currentTimeMillis());
                updatedStoresVersion = updatedStoresVersion.incremented(nodeIds.get(0),
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
        private static void doMetaUpdateVersionsOnStores(AdminClient adminClient,
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
    private static class SubCommandMetaSyncVersion extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsUrl(parser, true);
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
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_URL);

            // declare parameters
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM))
                confirm = true;

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);

            // execute command
            if(!AdminUtils.askConfirm(confirm, "synchronize metadata version"))
                return;

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);

            doMetaSyncVersion(adminClient, node);
        }

        /**
         * Synchronizes metadata versions across all nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes Base node object to get metadata version from
         * 
         */
        private static void doMetaSyncVersion(AdminClient adminClient, Node node) {
            String valueObject = doMetaGetVersionsForNode(adminClient, node);
            Properties props = new Properties();
            try {
                props.load(new ByteArrayInputStream(valueObject.getBytes()));
                if(props.size() == 0) {
                    System.err.println("The specified node does not have any versions metadata ! Exiting ...");
                    System.exit(-1);
                }
                adminClient.metadataMgmtOps.setMetadataversion(props);
                System.out.println("Metadata versions synchronized successfully.");
            } catch(IOException e) {
                System.err.println("Error while retrieving Metadata versions from node : "
                                   + node.getId() + ". Exception = \n");
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /**
     * meta check-version command
     */
    private static class SubCommandMetaCheckVersion extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsUrl(parser, true);
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
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_URL);

            // declare parameters
            String url = null;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);

            // execute command
            AdminClient adminClient = AdminUtils.getAdminClient(url);

            doMetaCheckVersion(adminClient);
        }

        /**
         * Verifies metadata versions for all the cluster nodes
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * 
         */
        private static void doMetaCheckVersion(AdminClient adminClient) {
            Map<Properties, Integer> versionsNodeMap = new HashMap<Properties, Integer>();

            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                String valueObject = doMetaGetVersionsForNode(adminClient, node);
                Properties props = new Properties();
                try {
                    props.load(new ByteArrayInputStream(valueObject.getBytes()));
                } catch(IOException e) {
                    System.err.println("Error while parsing Metadata versions for node : "
                                       + node.getId() + ". Exception = \n");
                    e.printStackTrace();
                    System.exit(-1);
                }
                versionsNodeMap.put(props, node.getId());
            }

            if(versionsNodeMap.keySet().size() > 1) {
                System.err.println("Mismatching versions detected !!!");
                for(Entry<Properties, Integer> entry: versionsNodeMap.entrySet()) {
                    System.out.println("**************************** Node: " + entry.getValue()
                                       + " ****************************");
                    System.out.println(entry.getKey());
                }
            } else {
                System.err.println("All the nodes have the same metadata versions .");
            }
        }
    }

    /**
     * Sets metadata.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to set metadata
     * @param metaKey Metadata key to set
     * @param metaValue Metadata value to set
     */
    private static void doMetaSet(AdminClient adminClient,
                                  Collection<Node> nodes,
                                  String metaKey,
                                  Object metaValue) {
        List<Integer> nodeIds = Lists.newArrayList();
        VectorClock updatedVersion = null;
        for(Node node: nodes) {
            nodeIds.add(node.getId());
            if(updatedVersion == null) {
                updatedVersion = (VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                             metaKey)
                                                                          .getVersion();
            } else {
                updatedVersion = updatedVersion.merge((VectorClock) adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                                                  metaKey)
                                                                                               .getVersion());
            }
            // Bump up version on first node
            updatedVersion = updatedVersion.incremented(nodeIds.get(0), System.currentTimeMillis());
        }
        adminClient.metadataMgmtOps.updateRemoteMetadata(nodeIds,
                                                         metaKey,
                                                         Versioned.value(metaValue.toString(),
                                                                         updatedVersion));
    }

    /**
     * Gets metadata versions for a given node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param node Node object to get metadata version from
     */
    private static String doMetaGetVersionsForNode(AdminClient adminClient, Node node) {
        List<Integer> partitionIdList = Lists.newArrayList();

        for(Node nodeIter: adminClient.getAdminClientCluster().getNodes()) {
            partitionIdList.addAll(nodeIter.getPartitionIds());
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.bulkFetchOps.fetchEntries(node.getId(),
                                                                                                             SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                                                             partitionIdList,
                                                                                                             null,
                                                                                                             true);

        Serializer<String> serializer = new StringSerializer("UTF8");
        String keyObject = null;
        String valueObject = null;

        while(entriesIterator.hasNext()) {
            try {
                Pair<ByteArray, Versioned<byte[]>> kvPair = entriesIterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                byte[] valueBytes = kvPair.getSecond().getValue();
                keyObject = serializer.toObject(keyBytes);
                if(!keyObject.equals(MetadataVersionStoreUtils.VERSIONS_METADATA_KEY)) {
                    continue;
                }
                valueObject = serializer.toObject(valueBytes);
            } catch(Exception e) {
                System.err.println("Error while retrieving Metadata versions from node : "
                                   + node.getId() + ". Exception = \n");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        return valueObject;
    }

}

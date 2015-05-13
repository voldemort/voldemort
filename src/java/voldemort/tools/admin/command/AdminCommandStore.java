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
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreOperationFailureException;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

/**
 * Implements all functionality of admin store operations.
 * 
 */
public class AdminCommandStore extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("add")) {
            SubCommandStoreAdd.executeCommand(args);
        } else if(subCmd.equals("update")) {
            SubCommandStoreUpdate.executeCommand(args);
        } else if(subCmd.equals("delete")) {
            SubCommandStoreDelete.executeCommand(args);
        } else if(subCmd.equals("rollback-ro")) {
            SubCommandStoreRollbackReadOnly.executeCommand(args);
        } else if(subCmd.equals("truncate-partition")) {
            SubCommandStoreTruncatePartition.executeCommand(args);
        } else if(subCmd.equals("truncate-store")) {
            SubCommandStoreTruncateStore.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Store Commands");
        stream.println("-----------------------------------");
        stream.println("add                  Add stores from a \'stores.xml\' file.");
        stream.println("update               Update store definitions from a \'stores.xml\' file.");
        stream.println("delete               Delete stores.");
        stream.println("rollback-ro          Rollback read-only store to a given version.");
        stream.println("truncate-partition   Remove contents of partitions on a node.");
        stream.println("truncate-store       Remove contents of stores.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help store <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("add")) {
            SubCommandStoreAdd.printHelp(stream);
        } else if(subCmd.equals("update")) {
            SubCommandStoreUpdate.printHelp(stream);
        } else if(subCmd.equals("delete")) {
            SubCommandStoreDelete.printHelp(stream);
        } else if(subCmd.equals("rollback-ro")) {
            SubCommandStoreRollbackReadOnly.printHelp(stream);
        } else if(subCmd.equals("truncate-partition")) {
            SubCommandStoreTruncatePartition.printHelp(stream);
        } else if(subCmd.equals("truncate-store")) {
            SubCommandStoreTruncateStore.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * store add command
     */
    public static class SubCommandStoreAdd extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsFile(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
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
            stream.println("  store add - Add stores from a \'stores.xml\' file");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store add -f <stores.xml-file-path> -u <url> [-n <node-id-list> | --all-nodes]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and adds store on given nodes from a given
         * stores.xml file.
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
            String storesFile = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_FILE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storesFile = (String) options.valueOf(AdminParserUtils.OPT_FILE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            doStoreAdd(adminClient, nodeIds, storesFile);
        }

        /**
         * Adds store on given nodes from a given stores.xml file.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to add stores on
         * @param storesFile File path of stores.xml to be added
         * @throws IOException
         * 
         */
        public static void doStoreAdd(AdminClient adminClient,
                                      List<Integer> nodeIds,
                                      String storesFile) throws IOException {
            List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(new File(storesFile));
            for(StoreDefinition storeDef: storeDefinitionList) {
                System.out.println("Adding " + storeDef.getName());
                adminClient.storeMgmtOps.addStore(storeDef, nodeIds);
            }
        }
    }

    /**
     * store update command
     */
    public static class SubCommandStoreUpdate extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsFile(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
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
            stream.println("  store update - Update store definitions from a \'stores.xml\' file");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store update -f <stores.xml-file-path> -u <url> [-n <node-id-list> | --all-nodes]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and adds store on given nodes from a given
         * stores.xml file.
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
            String storesFile = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_FILE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storesFile = (String) options.valueOf(AdminParserUtils.OPT_FILE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            doStoreUpdate(adminClient, nodeIds, storesFile);
        }

        /**
         * Updates store on given nodes from a given stores.xml file.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to update stores on
         * @param storesFile File path of stores.xml to be updated
         * @throws IOException
         * 
         */
        public static void doStoreUpdate(AdminClient adminClient,
                                         List<Integer> nodeIds,
                                         String storesFile) throws IOException {
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesFile));
            adminClient.metadataMgmtOps.updateRemoteStoreDefList(storeDefs, nodeIds);
        }
    }

    /**
     * store delete command
     */
    public static class SubCommandStoreDelete extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  store delete - Delete stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store delete -s <store-name-list> -u <url> [-n <node-id-list> | --all-nodes]");
            stream.println("               [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and deletes given list of stores on given nodes.
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
            List<String> storeNames = null;
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
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Delete stores");
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "delete store")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            doStoreDelete(adminClient, nodeIds, storeNames);
        }

        /**
         * Deletes given list of stores on given nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to add stores on
         * @param storeNames List of stores to be deleted
         * 
         */
        public static void doStoreDelete(AdminClient adminClient,
                                         List<Integer> nodeIds,
                                         List<String> storeNames) {
            for(String storeName: storeNames) {
                for(Integer nodeId: nodeIds) {
                    System.out.println("Deleting " + storeName + " on node " + nodeId);
                    try {
                        adminClient.storeMgmtOps.deleteStore(storeName, nodeId);
                    } catch(StoreOperationFailureException e) {
                        System.out.println("Store deletion failed on node " + nodeId);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * store rollback-ro command
     */
    public static class SubCommandStoreRollbackReadOnly extends AbstractAdminCommand {

        public static final String OPT_VERSION = "version";

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
            AdminParserUtils.acceptsStoreSingle(parser);
            AdminParserUtils.acceptsUrl(parser);
            parser.accepts(OPT_VERSION, "rollback read-only store to version")
                  .withRequiredArg()
                  .describedAs("store-version")
                  .ofType(Long.class);
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
            stream.println("  store rollback-ro - Rollback read-only store to a given version");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store rollback-ro -s <store-name> -u <url> --version <store-version>");
            stream.println("                    [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and rolls back a read-only store to a given
         * version.
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
            String storeName = null;
            String url = null;
            Long pushVersion = null;
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
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options, OPT_VERSION);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storeName = (String) options.valueOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            pushVersion = (Long) options.valueOf(OPT_VERSION);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Rollback read-only stores");
            System.out.println("Push Version = " + pushVersion);
            System.out.println("Store:");
            System.out.println("  " + storeName);
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = allnodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "rollback read-only store")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            adminClient.readonlyOps.rollbackStore(nodeIds, storeName, pushVersion);
        }
    }

    /**
     * store truncate-partition command
     */
    public static class SubCommandStoreTruncatePartition extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsPartition(parser);
            AdminParserUtils.acceptsNodeSingle(parser);
            AdminParserUtils.acceptsStoreMultiple(parser); // either
                                                           // --store or
                                                           // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
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
            stream.println("  store truncate-partition - Remove contents of partitions on a node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store truncate-partition -p <partition-id-list> -n <node-id>");
            stream.println("                           (-s <store-name-list> | --all-stores)");
            stream.println("                           -u <url> [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and removes contents of partitions on a single
         * node.
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
            List<Integer> partIds = null;
            Integer nodeId = null;
            List<String> storeNames = null;
            Boolean allStores = false;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_PARTITION);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_STORE,
                                           AdminParserUtils.OPT_ALL_STORES);

            // load parameters
            partIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_PARTITION);
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            if(options.has(AdminParserUtils.OPT_STORE)) {
                storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
                allStores = false;
            } else {
                allStores = true;
            }
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Remove contents of partitions");
            System.out.println("Partition:");
            for(Integer partId: partIds) {
                System.out.println("  " + partId);
            }
            System.out.println("Store:");
            if(allStores) {
                System.out.println("  all stores");
            } else {
                System.out.println("  " + Joiner.on(", ").join(storeNames));
            }
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "truncate partition")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allStores) {
                storeNames = AdminToolUtils.getAllUserStoreNamesOnNode(adminClient, nodeId);
            } else {
                AdminToolUtils.validateUserStoreNamesOnNode(adminClient, nodeId, storeNames);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeId);

            doStoreTruncatePartition(adminClient, nodeId, storeNames, partIds);
        }

        /**
         * Removes contents of partitions on a single node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeId Node id to remove partitions from
         * @param storeNames List of stores to remove partitions from
         * @param partIds List of partitions to be removed
         * 
         */
        public static void doStoreTruncatePartition(AdminClient adminClient,
                                                    Integer nodeId,
                                                    List<String> storeNames,
                                                    List<Integer> partIds) {
            for(String storeName: storeNames) {
                System.out.println("Truncating partition " + Joiner.on(", ").join(partIds) + " of "
                                   + storeName);
                adminClient.storeMntOps.deletePartitions(nodeId, storeName, partIds, null);
            }
        }
    }

    /**
     * store truncate-store command
     */
    public static class SubCommandStoreTruncateStore extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsStoreMultiple(parser);
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
            stream.println("  store truncate-store - Remove contents of stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  store truncate-store -s <store-name-list> -u <url>");
            stream.println("                       [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and removes contents of stores.
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
            List<String> storeNames = null;
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
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Remove contents of stores");
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "truncate store")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeIds);

            doStoreTruncateStore(adminClient, nodeIds, storeNames);
        }

        /**
         * Removes contents of stores.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to remove stores from
         * @param storeNames List of stores to be removed
         * 
         */
        public static void doStoreTruncateStore(AdminClient adminClient,
                                                List<Integer> nodeIds,
                                                List<String> storeNames) {
            for(String storeName: storeNames) {
                System.out.println("Truncating store " + storeName);
                adminClient.storeMntOps.truncate(nodeIds, storeName);
            }
        }
    }
}

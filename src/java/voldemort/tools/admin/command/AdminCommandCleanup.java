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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Implements all cleanup commands.
 */
public class AdminCommandCleanup extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("orphaned-data")) {
            SubCommandCleanupOrphanedData.executeCommand(args);
        } else if(subCmd.equals("vector-clocks")) {
            SubCommandCleanupVectorClocks.executeCommand(args);
        } else if(subCmd.equals("slops")) {
            SubCommandCleanupSlops.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Cleanup Commands");
        stream.println("-------------------------------------");
        stream.println("orphaned-data   Remove orhpaned data on a single node after rebalancing is done.");
        stream.println("vector-clocks   Prune data resulting from versioned puts during rebalancing.");
        stream.println("slops           Purge slops.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help cleanup <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("orphaned-data")) {
            SubCommandCleanupOrphanedData.printHelp(stream);
        } else if(subCmd.equals("vector-clocks")) {
            SubCommandCleanupVectorClocks.printHelp(stream);
        } else if(subCmd.equals("slops")) {
            SubCommandCleanupSlops.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * cleanup orphaned-data command
     */
    private static class SubCommandCleanupOrphanedData extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsNodeMultiple(parser); // either --node or
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
            stream.println("  cleanup orphaned-data - Remove orhpaned data on a single node after");
            stream.println("                          rebalancing is done");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  cleanup orphaned-data -u <url> [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and removes orphaned data on a single node after
         * rebalancing is done. Previously known as "--repair-job".
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
            System.out.println("Remove orphaned data after rebalancing");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminUtils.askConfirm(confirm, "cleanup orphaned data"))
                return;

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            doCleanupOrphanedData(adminClient, nodes);
        }

        /**
         * Removes orphaned data on a single node after rebalancing is done.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes Nodes to remove orphaned data on
         */
        private static void doCleanupOrphanedData(AdminClient adminClient, Collection<Node> nodes) {
            for(Node node: nodes) {
                adminClient.storeMntOps.repairJob(node.getId());
            }
        }
    }

    /**
     * cleanup vector-clocks command
     */
    private static class SubCommandCleanupVectorClocks extends AbstractAdminCommand {

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
            stream.println("  cleanup vector-clocks - Prune data resulting from versioned puts during");
            stream.println("                          rebalancing");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  cleanup vector-clocks -s <store-name-list> -u <url>");
            stream.println("                        [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and prunes data resulting from versioned puts
         * during rebalancing Previously known as "--prune-job".
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
            System.out.println("Prune data resulting from versioned puts");
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
            if(!AdminUtils.askConfirm(confirm, "cleanup vector clocks"))
                return;

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            doCleanupVectorClocks(adminClient, nodes, storeNames);
        }

        /**
         * Prunes data resulting from versioned puts during rebalancing
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes Nodes to prune vector clocks
         * @param storeNames List of stores to prune vector clocks
         */
        private static void doCleanupVectorClocks(AdminClient adminClient,
                                                  Collection<Node> nodes,
                                                  List<String> storeNames) {
            for(Node node: nodes) {
                adminClient.storeMntOps.pruneJob(node.getId(), storeNames);
            }
        }
    }

    /**
     * cleanup slops command
     */
    private static class SubCommandCleanupSlops extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsZone(parser);
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
            stream.println("  cleanup slops - Purge slops");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  cleanup slops -s <store-name-list> -u <url> -z <zone-id>");
            stream.println("                [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and purges slops; the command is previously known
         * as "--purge-slops".
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
            Integer zoneId = null;
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
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_ZONE);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            zoneId = (Integer) options.valueOf(AdminParserUtils.OPT_ZONE);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Purge slops");
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  zone = " + zoneId);
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(",").join(nodeIds));
            }

            // execute command
            if(!AdminUtils.askConfirm(confirm, "cleanup slops"))
                return;

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            if(allNodes) {
                Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
                nodeIds = Lists.newArrayList();
                for(Node node: nodes)
                    nodeIds.add(node.getId());
            }

            adminClient.storeMntOps.slopPurgeJob(nodeIds, zoneId, storeNames);
        }
    }
}
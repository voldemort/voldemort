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

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Lists;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;

/**
 * Implements all functionality of admin cleanup operations.
 * 
 */
public class AdminCommandCleanup {

    /**
     * Main entry of group command 'cleanup', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     * 
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
        String subCmd = new String();
        if (args.length >= 2) subCmd = args[1];
        
        if (subCmd.equals("orphaned-data")) executeCleanupOrphanedData(args, printHelp);
        else if (subCmd.equals("vector-clocks")) executeCleanupVectorClocks(args, printHelp);
        else if (subCmd.equals("slops")) executeCleanupSlops(args, printHelp);
        else executeCleanupHelp();
    }

    /**
     * Removes orphaned data on a single node after rebalancing is done.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes Nodes to remove orphaned data on
     */
    private static void doCleanupOrphanedData(AdminClient adminClient, Collection<Node> nodes) {
        for (Node node: nodes) {
            adminClient.storeMntOps.repairJob(node.getId());
        }
    }

    /**
     * Prunes data resulting from versioned puts during rebalancing
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes Nodes to prune vector clocks
     * @param storeNames List of stores to prune vector clocks
     */
    private static void doCleanupVectorClocks(AdminClient adminClient, Collection<Node> nodes, List<String> storeNames) {
        for (Node node: nodes) {
            adminClient.storeMntOps.pruneJob(node.getId(), storeNames);
        }
    }

    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeCleanupHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Cleanup Commands");
        System.out.println("-------------------------------------");
        System.out.println("orphaned-data   Remove orhpaned data on a single node after rebalancing is done.");
        System.out.println("vector-clocks   Prune data resulting from versioned puts during rebalancing.");
        System.out.println("slops           Purge slops.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help cleanup <command-name>\'.");
        System.out.println();
    }
    
    /**
     * Parses command-line and removes orphaned data on a single node after rebalancing is done.
     * Previously known as "--repair-job".
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeCleanupOrphanedData(String[] args, Boolean printHelp) throws IOException {
        
        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  cleanup orphaned-data - Remove orhpaned data on a single node after");
            System.out.println("                          rebalancing is done");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  cleanup orphaned-data -u <url> [-n <node-id-list> | --all-nodes] [--confirm]");
            System.out.println();
            parser.printHelp();
            System.out.println();
            return;
        }
        
        // parse command-line input
        try {
            parser.parse(args, 2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        // load parameters
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "cleanup orphaned data")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doCleanupOrphanedData(adminClient, nodes);
    }

    /**
     * Parses command-line and prunes data resulting from versioned puts during rebalancing
     * Previously known as "--prune-job".
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeCleanupVectorClocks(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> storeNames = null;
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  cleanup vector-clocks - Prune data resulting from versioned puts during");
            System.out.println("                          rebalancing");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  cleanup vector-clocks -s <store-name-list> -u <url>");
            System.out.println("                        [-n <node-id-list> | --all-nodes] [--confirm]");
            System.out.println();
            parser.printHelp();
            System.out.println();
            return;
        }
        
        // parse command-line input
        try {
            parser.parse(args, 2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        // load parameters
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "cleanup vector clocks")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doCleanupVectorClocks(adminClient, nodes, storeNames);
    }

    /**
     * Parses command-line and cleans up slops
     * Previously known as "--purge-slops".
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeCleanupSlops(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> storeNames = null;
        String url = null;
        Integer zoneId = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addRequired(AdminOptionParser.OPT_ZONE);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);

        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  cleanup slops - Purge slops");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  cleanup slops -s <store-name-list> -u <url> -z <zone-id>");
            System.out.println("                [-n <node-id-list> | --all-nodes] [--confirm]");
            System.out.println();
            parser.printHelp();
            System.out.println();
            return;
        }
        
        // parse command-line input
        try {
            parser.parse(args, 2);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        // load parameters
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        zoneId = (Integer) parser.getValue(AdminOptionParser.OPT_ZONE);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "cleanup slops")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        if (allNodes) {
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
            nodeIds = Lists.newArrayList();
            for (Node node: nodes) nodeIds.add(node.getId());
        }
        
        adminClient.storeMntOps.slopPurgeJob(nodeIds, zoneId, storeNames);
    }
}

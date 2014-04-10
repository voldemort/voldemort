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

import java.io.*;
import java.util.*;

import com.google.common.base.Joiner;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Implements all functionality of admin store operations.
 * 
 */
public class AdminCommandStore {

    /**
     * Main entry of group command 'store', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     * 
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
        String subCmd = new String();
        if (args.length >= 2) subCmd = args[1];
        
        if (subCmd.compareTo("add") == 0) executeStoreAdd(args, printHelp);
        else if (subCmd.compareTo("delete") == 0) executeStoreDelete(args, printHelp);
        else if (subCmd.compareTo("rollback-ro") == 0) executeStoreRollbackReadOnly(args, printHelp);
        else if (subCmd.compareTo("truncate-partition") == 0) executeStoreTruncatePartition(args, printHelp);
        else if (subCmd.compareTo("truncate-store") == 0) executeStoreTruncateStore(args, printHelp);
        else executeStoreHelp();
    }

    /**
     * Adds store on given nodes from a given stores.xml file.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to add stores on
     * @param storesFile File path of stores.xml to be added
     * @throws IOException
     * 
     */
    private static void doStoreAdd(AdminClient adminClient, Collection<Node> nodes, String storesFile)
            throws IOException {
        List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(new File(storesFile));
        for (StoreDefinition storeDef: storeDefinitionList) {
            System.out.println("Adding " + storeDef.getName());
            for (Node node: nodes) {
                System.out.println("to node " + node.getId());
                adminClient.storeMgmtOps.addStore(storeDef, node.getId());
            }
        }
    }

    /**
     * Deletes given list of stores on given nodes.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to add stores on
     * @param storeNames List of stores to be deleted
     * 
     */
    private static void doStoreDelete(AdminClient adminClient, Collection<Node> nodes, List<String> storeNames) {
        for (String storeName: storeNames) {
            System.out.println("Deleting " + storeName);
            for (Node node: nodes) {
                System.out.println("from node " + node.getId());
                adminClient.storeMgmtOps.deleteStore(storeName, node.getId());
            }
        }
    }

    /**
     * Rolls back a given store to a given version.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to add stores on
     * @param storeName Name of store to be rolled back
     * @param pushVersion Version to be rolled back to
     * 
     */
    private static void doStoreRollbackReadOnly(AdminClient adminClient,
            Collection<Node> nodes, String storeName, long pushVersion) {
        for (Node node: nodes) {
            System.out.println("Rollback store " + storeName + " on node " + node.getId());
            adminClient.readonlyOps.rollbackStore(node.getId(), storeName, pushVersion);
        }
    }

    /**
     * Removes contents of partitions on a single node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param node Node to remove partitions from
     * @param storeNames List of stores to remove partitions from
     * @param partIds List of partitions to be removed
     * 
     */
    private static void doStoreTruncatePartition(AdminClient adminClient, Node node, List<String> storeNames,
                                          List<Integer> partIds) {
        for(String storeName: storeNames) {
            System.out.println("Truncating partition " + Joiner.on(", ").join(partIds)
                               + " of " + storeName);
            adminClient.storeMntOps.deletePartitions(node.getId(), storeName, partIds, null);
        }
    }

    /**
     * Removes contents of stores.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to remove stores from
     * @param storeNames List of stores to be removed
     * 
     */
    private static void doStoreTruncateStore(AdminClient adminClient, Collection<Node> nodes, List<String> storeNames) {
        for (String storeName: storeNames) {
            System.out.println("Truncating store " + storeName);
            for(Node node: nodes) {
                System.out.println("on node " + node.getId());
                adminClient.storeMntOps.truncate(node.getId(), storeName);
            }
        }
    }

    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeStoreHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Store Commands");
        System.out.println("-----------------------------------");
        System.out.println("add                  Add stores from a \'stores.xml\' file.");
        System.out.println("delete               Delete stores.");
        System.out.println("rollback-ro          Rollback read-only store to a given version.");
        System.out.println("truncate-partition   Remove contents of partitions on a node.");
        System.out.println("truncate-store       Remove contents of stores.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help store <command-name>\'.");
        System.out.println();
    }

    /**
     * Parses command-line and adds store on given nodes from a given stores.xml file.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStoreAdd(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String storesFile = null;
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_FILE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  store add - Add stores from a \'stores.xml\' file");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  store add -f <stores.xml-file-path> -u <url> [-n <node-id-list> | --all-nodes]");
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
        storesFile = (String) parser.getValue(AdminOptionParser.OPT_FILE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doStoreAdd(adminClient, nodes, storesFile);
    }

    /**
     * Parses command-line and deletes given list of stores on given nodes.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStoreDelete(String[] args, Boolean printHelp) throws IOException {

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
            System.out.println("  store delete - Delete stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  store delete -s <store-name-list> -u <url> [-n <node-id-list> | --all-nodes]");
            System.out.println("               [--confirm]");
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
        
        if (!AdminUtils.askConfirm(confirm, "delete store")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doStoreDelete(adminClient, nodes, storeNames);
    }

    /**
     * Parses command-line and rolls back a read-only store to a given version.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStoreRollbackReadOnly(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String storeName = null;
        String url = null;
        Long pushVersion = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_STORE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addRequired(AdminOptionParser.OPT_VERSION);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  store rollback-ro - Rollback read-only store to a given version");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  store rollback-ro -s <store-name> -u <url> --version <store-version>");
            System.out.println("                    [-n <node-id-list> | --all-nodes] [--confirm]");
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
        storeName = (String) parser.getValue(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        pushVersion = (Long) parser.getValue(AdminOptionParser.OPT_VERSION);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "rollback read-only store")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doStoreRollbackReadOnly(adminClient, nodes, storeName, pushVersion);
    }

    /**
     * Parses command-line and removes contents of partitions on a single node.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStoreTruncatePartition(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<Integer> partIds= null;
        Integer nodeId = null;
        List<String> storeNames = null;
        Boolean allStores = false;
        String url = null;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_PARTITION);
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE, AdminOptionParser.OPT_ALL_STORES);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  store truncate-partition - Remove contents of partitions on a node");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  store truncate-partition -p <partition-id-list> -n <node-id>");
            System.out.println("                           (-s <store-name-list> | --all-stores)");
            System.out.println("                           -u <url> [--confirm]");
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
        partIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_PARTITION);
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        if (parser.hasOption(AdminOptionParser.OPT_STORE)) {
            storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
            allStores = false;
        } else {
            allStores = true;
        }
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "truncate partition")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
        storeNames = AdminUtils.getUserStoresOnNode(adminClient, node, storeNames, allStores);
        
        doStoreTruncatePartition(adminClient, node, storeNames, partIds);
    }

    /**
     * Parses command-line and removes contents of stores.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStoreTruncateStore(String[] args, Boolean printHelp) throws IOException {

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
        parser.addRequired(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  store truncate-store - Remove contents of stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  store truncate-store -s <store-name-list> -u <url>");
            System.out.println("                       [-n <node-id-list> | --all-nodes] [--confirm]");
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
        
        if (!AdminUtils.askConfirm(confirm, "truncate store")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doStoreTruncateStore(adminClient, nodes, storeNames);
    }

}

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
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.quota.QuotaUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

/**
 * Implements all functionality of admin quota operations.
 * 
 */
public class AdminCommandQuota {

    /**
     * Main entry of group command 'quota', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
        String subCmd = new String();
        if (args.length >= 2) subCmd = args[1];
        
        if (subCmd.compareTo("get") == 0) executeQuotaGet(args, printHelp);
        else if (subCmd.compareTo("set") == 0) executeQuotaSet(args, printHelp);
        else if (subCmd.compareTo("reserve-memory") == 0) executeQuotaReserveMemory(args, printHelp);
        else if (subCmd.compareTo("unset") == 0) executeQuotaUnset(args, printHelp);
        else executeQuotaHelp();
    }

    /**
     * Gets quota for given quota types on given stores.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param storeNames List of stores to query quota
     * @param quotaType List of quota types to fetch
     */
    private static void doQuotaGet(AdminClient adminClient, List<String> storeNames, List<String> quotaTypes) {
        for (String storeName: storeNames) {
            if (!adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                System.out.println("Store " + storeName + " not in cluster.");
            } else {
                System.out.println("Store " + storeName);
                for (String quotaType: quotaTypes) {
                    Versioned<String> quotaVal = adminClient.quotaMgmtOps.getQuota(storeName, quotaType);
                    if (quotaVal == null) {
                        System.out.println("No quota set for " + quotaType);
                    } else {
                        System.out.println("Quota value  for " + quotaType + " : " + quotaVal.getValue());
                    }
                }
            }
        }
    }

    /**
     * Reserves memory for given stores on given nodes.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes List of nodes to reserve memory on
     * @param storeNames List of stores to reserve memory on
     * @param memoryMBSize Size of memory to be reserved
     * 
     */
    private static void doQuotaReserveMemory(AdminClient adminClient,
        Collection<Node> nodes, List<String> storeNames, long memoryMBSize) {
        for (Node node: nodes) {
            adminClient.quotaMgmtOps.reserveMemory(node.getId(), storeNames, memoryMBSize);
        }
    }
    
    /**
     * Sets quota for given quota types on given stores.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param storeNames The list of target stores to set quota
     * @param quotaMap Pairs of quota type-value to set
     * 
     */
    private static void doQuotaSet(AdminClient adminClient, List<String> storeNames, Map<String, String> quotaMap) {
        for (String storeName: storeNames) {
            if (adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                Iterator<Entry<String, String>> iter = quotaMap.entrySet().iterator();
                while (iter.hasNext()) {
                    @SuppressWarnings("rawtypes")
                    Map.Entry entry = (Map.Entry) iter.next();
                    adminClient.quotaMgmtOps.setQuota(storeName,
                                                      (String) entry.getKey(),
                                                      (String) entry.getValue());
                }
            } else {
                System.err.println("Store " + storeName + " not in cluster.");
            }
        }
    }

    /**
     * Unsets quota for given quota types on given stores.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param storeNames The list of target stores to unset quota
     * @param quotaTypes Quota types to unset
     * 
     */
    private static void doQuotaUnset(AdminClient adminClient, List<String> storeNames, List<String> quotaTypes) {
        for (String storeName: storeNames) {
            if (adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                for (String quotaType: quotaTypes) {
                    adminClient.quotaMgmtOps.unsetQuota(storeName, quotaType);
                }
            } else {
                System.err.println("Store " + storeName + " not in cluster.");
            }
        }
    }
    
    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeQuotaHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Quota Commands");
        System.out.println("-----------------------------------");
        System.out.println("get              Get quota values of stores.");
        System.out.println("reserve-memory   Reserve memory for stores.");
        System.out.println("set              Set quota values for stores.");
        System.out.println("unset            Clear quota settings for stores.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help quota <command-name>\'.");
        System.out.println();
    }
    
    /**
     * Parses command-line and gets quota.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeQuotaGet(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> quotaTypes = null;
        List<String> storeNames = null;
        String url = null;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEAD_QUOTA_GET);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  quota get - Get quota values of stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  quota get (<quota-type-list> | all) -s <store-name-list> -u <url>");
            System.out.println();
            System.out.println("COMMENTS");
            System.out.println("  Valid quota types are:");
            for (String quotaType: QuotaUtils.validQuotaTypes()) {
                System.out.println("    " + quotaType);
            }
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
        quotaTypes = (List<String>) parser.getValueList(AdminOptionParser.OPT_HEAD_QUOTA_GET);
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        
        quotaTypes = AdminUtils.getQuotaTypes(quotaTypes);

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        
        doQuotaGet(adminClient, storeNames, quotaTypes);
    }

    /**
     * Parses command-line and reserves memory for given stores on given nodes.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeQuotaReserveMemory(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        long memoryMBSize = 0;
        List<String> storeNames = null;
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEAD_QUOTA_RESERVE_MEMORY);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  quota reserve-memory - Reserve memory for stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  quota reserve-memory <memory-size> -s <store-name-list> -u <url>");
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
        memoryMBSize = (Integer) parser.getValue(AdminOptionParser.OPT_HEAD_QUOTA_RESERVE_MEMORY);
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;

        if (!AdminUtils.askConfirm(confirm, "reserve memory")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doQuotaReserveMemory(adminClient, nodes, storeNames, memoryMBSize);
    }

    /**
     * Parses command-line and sets quota.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeQuotaSet(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> quota = null;
        List<String> storeNames = null;
        String url = null;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEAD_QUOTA_SET);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  quota set - Set quota values for stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  quota set (<quota-type1>=<quota-value1>,...) -s <store-name-list> -u <url>");
            System.out.println("            [--confirm]");
            System.out.println();
            System.out.println("COMMENTS");
            System.out.println("  Valid quota types are:");
            for (String quotaType: QuotaUtils.validQuotaTypes()) {
                System.out.println("    " + quotaType);
            }
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
        quota = parser.getValuePairList(AdminOptionParser.OPT_HEAD_QUOTA_SET, "=");
        if (quota.size() % 2 != 0) throw new VoldemortException("Invalid quota type-value pair.");
        Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
        for (Integer i = 0;i < quota.size();i += 2) {
            if (!validQuotaTypes.contains(quota.get(i))) {
                Utils.croak("Invalid quota type: " + quota.get(i));
            }
        }
        
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;

        if (!AdminUtils.askConfirm(confirm, "set quota")) return;
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Map<String, String> quotaMap = AdminUtils.convertListToMap(quota);
        
        doQuotaSet(adminClient, storeNames, quotaMap);
    }

    /**
     * Parses command-line and unsets quota.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeQuotaUnset(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> quotaTypes = null;
        List<String> storeNames = null;
        String url = null;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEAD_QUOTA_UNSET);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  quota unset - Clear quota settings for stores");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  quota unset (<quota-type-list> | all) -s <store-name-list> -u <url>");
            System.out.println("              [--confirm]");
            System.out.println();
            System.out.println("COMMENTS");
            System.out.println("  Valid quota types are:");
            for (String quotaType: QuotaUtils.validQuotaTypes()) {
                System.out.println("    " + quotaType);
            }
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
        quotaTypes = (List<String>) parser.getValueList(AdminOptionParser.OPT_HEAD_QUOTA_UNSET);
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;

        if (!AdminUtils.askConfirm(confirm, "unset quota")) return;
        
        quotaTypes = AdminUtils.getQuotaTypes(quotaTypes);
        
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        
        doQuotaUnset(adminClient, storeNames, quotaTypes);
    }

}

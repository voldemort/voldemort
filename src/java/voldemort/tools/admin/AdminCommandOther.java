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

import voldemort.client.protocol.admin.AdminClient;

/**
 * Implements all functionality of admin other operations.
 * 
 */
public class AdminCommandOther {

    /**
     * Main entry of commands that are not grouped, directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
        String subCmd = new String();
        if (args.length >= 1) subCmd = args[0];
        
        if (subCmd.compareTo("native-backup") == 0) executeNativeBackup(args, printHelp);
        else if (subCmd.compareTo("restore-from-replica") == 0) executeRestoreFromReplica(args, printHelp);
        else executeHelp();
    }
    
    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Commands");
        System.out.println("-----------------------------");
        System.out.println("async-job          Show or stop async jobs.");
        System.out.println("cleanup            Cleanup orphaned data, vector clocks or slops.");
        System.out.println("debug              Check keys or routing plan.");
        System.out.println("meta               Get, set, clear metadata; check or sync metadata version.");
        System.out.println("quota              Get, set, unset quota or reserve memory on stores.");
        System.out.println("store              Add, delete, rollback, truncate stores, or truncate");
        System.out.println("                   partitions.");
        System.out.println("stream             Fetch keys or entries, or mirror data across nodes, or update");
        System.out.println("                   entries.");
        System.out.println("help               Show help menu or information for each command.");
        System.out.println("native-backup      Backup bdb data on one single node natively.");
        System.out.println("restore-from-peer  Restore data from peer replication.");
        System.out.println();
        System.out.println("To get more information on each command, please try \'help <command-name>\'.");
        System.out.println();
    }
    
    /**
     * Parses command-line and backup bdb data natively.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    private static void executeNativeBackup(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String dir = null;
        Integer nodeId = null;
        String storeName = null;
        String url = null;
        Integer timeout = 30;
        Boolean confirm = false;
        Boolean incremental = false;
        Boolean verify = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_DIR);
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_STORE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addRequired(AdminOptionParser.OPT_TIMEOUT);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        parser.addOptional(AdminOptionParser.OPT_INCREMENTAL);
        parser.addOptional(AdminOptionParser.OPT_VERIFY);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  native-backup - Backup bdb data on one single node natively");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  native-backup -d <backup-dir> -n <node-id> -s <store-name> -u <url>");
            System.out.println("                --timeout <time-minute> [--confirm] [--incremental] [--verify]");
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
        dir = (String) parser.getValue(AdminOptionParser.OPT_DIR);
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        storeName = (String) parser.getValue(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        timeout = (Integer) parser.getValue(AdminOptionParser.OPT_TIMEOUT);
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        if (parser.hasOption(AdminOptionParser.OPT_INCREMENTAL)) incremental = true;
        if (parser.hasOption(AdminOptionParser.OPT_VERIFY)) verify = true;
        
        if (!AdminUtils.askConfirm(confirm, "backup bdb data natively")) return;

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        
        adminClient.storeMntOps.nativeBackup(nodeId, storeName, dir, timeout, verify, incremental);
    }

    /**
     * Parses command-line and restores from peer replication
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    private static void executeRestoreFromReplica(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        Integer nodeId = null;
        String url = null;
        Integer zoneId = null;
        Integer parallel = 5;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addRequired(AdminOptionParser.OPT_ZONE);
        parser.addOptional(AdminOptionParser.OPT_PARALLEL);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);

        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  restore-from-replica - Restore data from peer replication");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  restore-from-replica -n <node-id> -u <url> -z <zone-id> [--parallel <num>]");
            System.out.println("                       [--confirm]");
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
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        zoneId = (Integer) parser.getValue(AdminOptionParser.OPT_ZONE);
        if (parser.hasOption(AdminOptionParser.OPT_PARALLEL)) {
            parallel = (Integer) parser.getValue(AdminOptionParser.OPT_PARALLEL);
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "restore node from replica")) return;

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        
        System.out.println("Starting restore");
        adminClient.restoreOps.restoreDataFromReplications(nodeId, parallel, zoneId);
        System.out.println("Finished restore");
    }

}

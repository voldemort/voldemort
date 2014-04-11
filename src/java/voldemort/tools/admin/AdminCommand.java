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
import java.util.List;

import com.google.common.collect.Lists;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;

/**
 * Implements all admin commands.
 * 
 */
public class AdminCommand extends AbstractAdminCommand{

    /**
     * Parses command-line and directs to command groups or sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if (subCmd.equals("async-job")) AdminCommandAsyncJob.executeCommand(args);
        else if (subCmd.equals("cleanup")) AdminCommandCleanup.execute(args, false);
        else if (subCmd.equals("debug")) AdminCommandDebug.execute(args, false);
        else if (subCmd.equals("meta")) AdminCommandMeta.execute(args, false);
        else if (subCmd.equals("quota")) AdminCommandQuota.execute(args, false);
        else if (subCmd.equals("store")) AdminCommandStore.execute(args, false);
        else if (subCmd.equals("stream")) AdminCommandStream.execute(args, false);
        else if (subCmd.equals("native-backup")) SubCommandNativeBackup.executeCommand(args);
        else if (subCmd.equals("restore-from-replica")) SubCommandRestoreFromReplica.executeCommand(args);
        else if (subCmd.equals("help") || subCmd.equals("--help") || subCmd.equals("-h")) executeHelp(System.out, args);
        else printHelp(System.out);
    }
    
    /**
     * Prints command-line help menu for all command groups.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Commands");
        stream.println("-----------------------------");
        stream.println("async-job              Show or stop async jobs.");
        stream.println("cleanup                Cleanup orphaned data, vector clocks or slops.");
        stream.println("debug                  Check keys or routing plan.");
        stream.println("meta                   Get, set, clear metadata; check or sync metadata version.");
        stream.println("quota                  Get, set, unset quota or reserve memory on stores.");
        stream.println("store                  Add, delete, rollback, truncate stores, or truncate");
        stream.println("                       partitions.");
        stream.println("stream                 Fetch keys or entries, or mirror data across nodes, or");
        stream.println("                       update entries.");
        stream.println("help                   Show help menu or information for each command.");
        stream.println("native-backup          Backup bdb data on one single node natively.");
        stream.println("restore-from-replica   Restore data from peer replication.");
        stream.println();
        stream.println("To get more information on each command, please try \'help <command-name>\'.");
        stream.println();
    }   

    /**
     * Parses command-line input and prints help menu.
     * @throws Exception 
     */
    public static void executeHelp(PrintStream stream, String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if (subCmd.equals("async-job")) AdminCommandAsyncJob.executeHelp(stream, args);
        else if (subCmd.equals("cleanup")) AdminCommandCleanup.execute(args, true);
        else if (subCmd.equals("debug")) AdminCommandDebug.execute(args, true);
        else if (subCmd.equals("meta")) AdminCommandMeta.execute(args, true);
        else if (subCmd.equals("quota")) AdminCommandQuota.execute(args, true);
        else if (subCmd.equals("store")) AdminCommandStore.execute(args, true);
        else if (subCmd.equals("stream")) AdminCommandStream.execute(args, true);
        else if (subCmd.equals("native-backup")) SubCommandNativeBackup.printHelp(stream);
        else if (subCmd.equals("restore-from-replica")) SubCommandRestoreFromReplica.printHelp(stream);
        else printHelp(stream);
    }

    /**
     * native-backup command
     */
    private static class SubCommandNativeBackup extends AbstractAdminCommand {

        private static final String OPT_INCREMENTAL = "incremental";
        private static final String OPT_TIMEOUT = "timeout";
        private static final String OPT_VERIFY = "verify";
        
    	/**
    	 * Initializes parser
    	 * 
    	 * @return OptionParser object with all available options
    	 */
    	protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            AdminParserUtils.acceptsDir(parser, true);
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsStoreSingle(parser, true);
            AdminParserUtils.acceptsUrl(parser, true);
            parser.accepts(OPT_TIMEOUT, "native-backup timeout in minute, defaults to 30")
                  .withRequiredArg()
                  .describedAs("time-minute")
                  .ofType(Integer.class);
            AdminParserUtils.acceptsConfirm(parser);
            parser.accepts(OPT_INCREMENTAL, "incremental native-backup for point-in-time recovery");
            parser.accepts(OPT_VERIFY, "native-backup verify checksum");
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
            stream.println("  native-backup - Backup bdb data on one single node natively");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  native-backup -d <backup-dir> -n <node-id> -s <store-name> -u <url>");
            stream.println("                [--timeout <time-minute>] [--confirm] [--incremental] [--verify]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
    	}
    	
        /**
         * Parses command-line and backup bdb data natively.
         * 
         * @param args Command-line input
         * @throws IOException 
         * 
         */
        public static void executeCommand(String[] args) throws IOException {
        	
            OptionParser parser = getParser();
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_DIR);
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_STORE);
            requiredAll.add(AdminParserUtils.OPT_URL);
            
            // declare parameters
            String dir = null;
            Integer nodeId = null;
            String storeName = null;
            String url = null;
            Integer timeout = 30;
            Boolean confirm = false;
            Boolean incremental = false;
            Boolean verify = false;
            
            // parse command-line input
            OptionSet options = parser.parse(args);
            
            // load parameters
            dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            storeName = (String) options.valueOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if (options.has(OPT_TIMEOUT)) {
            	timeout = (Integer) options.valueOf(OPT_TIMEOUT);
            }
            if (options.has(AdminParserUtils.OPT_CONFIRM)) {
            	confirm = true;
            }
            if (options.has(OPT_INCREMENTAL)) {
            	incremental = true;
            }
            if (options.has(OPT_VERIFY)) {
            	verify = true;
            }
            
            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            
            // execute command
            if (!AdminUtils.askConfirm(confirm, "backup bdb data natively")) {
            	return;
            }
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            
            adminClient.storeMntOps.nativeBackup(nodeId, storeName, dir, timeout, verify, incremental);
        }
    }

    /**
     * restore-from-replica command
     */
    private static class SubCommandRestoreFromReplica extends AbstractAdminCommand {

        private static final String OPT_PARALLEL = "parallel";
        
    	/**
    	 * Initializes parser
    	 * 
    	 * @return OptionParser object with all available options
    	 */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsUrl(parser, true);
            AdminParserUtils.acceptsZone(parser, true);
            parser.accepts(OPT_PARALLEL, "parallism parameter for restore-from-replica, defaults to 5")
                  .withRequiredArg()
                  .describedAs("num")
                  .ofType(Integer.class);
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
            System.out.println();
            System.out.println("NAME");
            System.out.println("  restore-from-replica - Restore data from peer replication");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  restore-from-replica -n <node-id> -u <url> -z <zone-id> [--parallel <num>]");
            System.out.println("                       [--confirm]");
            System.out.println();
            getParser().printHelpOn(stream);
            System.out.println();
    	}
    	
	    /**
	     * Parses command-line and restores from peer replication
	     * 
	     * @param args Command-line input
	     * @throws IOException
	     */
	    public static void executeCommand(String[] args) throws IOException {
	
	        OptionParser parser = getParser();
	        List<String> requiredAll = Lists.newArrayList();
	        requiredAll.add(AdminParserUtils.OPT_NODE);
	        requiredAll.add(AdminParserUtils.OPT_URL);
	        requiredAll.add(AdminParserUtils.OPT_ZONE);
	        
	        // declare parameters
	        Integer nodeId = null;
	        String url = null;
	        Integer zoneId = null;
	        Integer parallel = 5;
	        Boolean confirm = false;
	        
	        // parse command-line input
	        OptionSet options = parser.parse(args);
	        
	        // load parameters
	        nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
	        url = (String) options.valueOf(AdminParserUtils.OPT_URL);
	        zoneId = (Integer) options.valueOf(AdminParserUtils.OPT_ZONE);
	        if (options.has(OPT_PARALLEL)) {
	            parallel = (Integer) options.valueOf(OPT_PARALLEL);
	        }
	        if (options.has(AdminParserUtils.OPT_CONFIRM)) {
	        	confirm = true;
	        }
	        
	        // check correctness
	        AdminParserUtils.checkRequiredAll(options, requiredAll);

	        // execute command
	        if (!AdminUtils.askConfirm(confirm, "restore node from replica")) {
	        	return;
	        }
	        AdminClient adminClient = AdminUtils.getAdminClient(url);
	        
	        System.out.println("Starting restore");
	        adminClient.restoreOps.restoreDataFromReplications(nodeId, parallel, zoneId);
	        System.out.println("Finished restore");
	    }
    }
}

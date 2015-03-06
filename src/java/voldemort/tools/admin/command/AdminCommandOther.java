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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Zone;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;

/**
 * Implements all non-grouped admin commands.
 * 
 */
public class AdminCommandOther extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to command groups or non-grouped
     * sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("native-backup")) {
            SubCommandNativeBackup.executeCommand(args);
        } else if(subCmd.equals("restore-from-replica")) {
            SubCommandRestoreFromReplica.executeCommand(args);
        } else {
            AdminCommand.printHelp(System.out);
        }
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("native-backup")) {
            SubCommandNativeBackup.printHelp(stream);
        } else if(subCmd.equals("restore-from-replica")) {
            SubCommandRestoreFromReplica.printHelp(stream);
        } else {
            AdminCommand.printHelp(stream);
        }
    }

    /**
     * native-backup command
     */
    public static class SubCommandNativeBackup extends AbstractAdminCommand {

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
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            AdminParserUtils.acceptsDir(parser);
            AdminParserUtils.acceptsNodeSingle(parser);
            AdminParserUtils.acceptsStoreSingle(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
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
         */
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

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
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_DIR);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            storeName = (String) options.valueOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(OPT_TIMEOUT)) {
                timeout = (Integer) options.valueOf(OPT_TIMEOUT);
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }
            if(options.has(OPT_INCREMENTAL)) {
                incremental = true;
            }
            if(options.has(OPT_VERIFY)) {
                verify = true;
            }

            // print summary
            System.out.println("Backup bdb data natively");
            System.out.println("Store:");
            System.out.println("  " + storeName);
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);
            System.out.println("Settings:");
            System.out.println("  backup directory is \'" + dir + "\'");
            System.out.println("  timeout is " + timeout + " minutes");
            if(options.has(OPT_INCREMENTAL)) {
                System.out.println("  incremental backup");
            } else {
                System.out.println("  do not backup incrementally");
            }
            if(options.has(OPT_VERIFY)) {
                System.out.println("  verify backup checksum");
            } else {
                System.out.println("  do not verify backup checksum");
            }

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "backup bdb data natively")) {
                return;
            }
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeId);

            System.out.println("Please wait while performing native-backup...");
            adminClient.storeMntOps.nativeBackup(nodeId,
                                                 storeName,
                                                 dir,
                                                 timeout,
                                                 verify,
                                                 incremental);
        }
    }

    /**
     * restore-from-replica command
     */
    public static class SubCommandRestoreFromReplica extends AbstractAdminCommand {

        private static final String OPT_PARALLEL = "parallel";

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
            AdminParserUtils.acceptsZone(parser);
            // optional options
            parser.accepts(OPT_PARALLEL,
                           "parallism parameter for restore-from-replica, defaults to 5")
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
            stream.println();
            stream.println("NAME");
            stream.println("  restore-from-replica - Restore data from peer replication");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  restore-from-replica -n <node-id> -u <url> [-z <zone-id>] [--parallel <num>]");
            stream.println("                       [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and restores from peer replication
         * 
         * @param args Command-line input
         * @throws IOException
         */
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            Integer nodeId = null;
            String url = null;
            Integer zoneId = Zone.UNSET_ZONE_ID;
            Integer parallel = 5;
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
            if(options.has(OPT_PARALLEL)) {
                parallel = (Integer) options.valueOf(OPT_PARALLEL);
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }
            if(options.has(AdminParserUtils.OPT_ZONE)) {
                zoneId = (Integer) options.valueOf(AdminParserUtils.OPT_ZONE);
            }

            // print summary
            System.out.println("Restore from peer replica");
            System.out.println("Location:");
            System.out.println("  zone = " + zoneId);
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);
            System.out.println("Settings:");
            System.out.println("  parallelism is " + parallel);

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "restore node from replica")) {
                return;
            }
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);
            AdminToolUtils.assertServerNotInRebalancingState(adminClient, nodeId);

            System.out.println("Starting restore");
            adminClient.restoreOps.restoreDataFromReplications(nodeId, parallel, zoneId);
            System.out.println("Finished restore");
        }
    }
}

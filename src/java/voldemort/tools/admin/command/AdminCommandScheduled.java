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
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;

import com.google.common.base.Joiner;

/**
 * Implements all admin scheduled-job commands.
 */
public class AdminCommandScheduled extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("list")) {
            SubCommandScheduledList.executeCommand(args);
        } else if(subCmd.equals("stop")) {
            SubCommandScheduledStop.executeCommand(args);
        } else if(subCmd.equals("enable")) {
            SubCommandScheduledEnable.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Scheduled-Job Commands");
        stream.println("-------------------------------------------");
        stream.println("list     List scheduled jobs on nodes.");
        stream.println("stop     Stop scheduled jobs on one node.");
        stream.println("enable   Enable scheduled jobs on one node.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help scheduled <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws IOException
     */
    public static void executeHelp(String[] args, PrintStream stream) throws IOException {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("list")) {
            SubCommandScheduledList.printHelp(stream);
        } else if(subCmd.equals("stop")) {
            SubCommandScheduledStop.printHelp(stream);
        } else if(subCmd.equals("enable")) {
            SubCommandScheduledEnable.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * scheduled-job list command
     */
    public static class SubCommandScheduledList extends AbstractAdminCommand {

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
            stream.println("  scheduled list - Get scheduled job list from nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  scheduled list -u <url> [-n <node-id-list> | --all-nodes]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and gets job list from nodes.
         * 
         * @param args Command-line input
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

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            doListScheduledJobs(adminClient, nodeIds);
        }

        /**
         * Gets list of jobs across multiple nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to get jobs from
         */
        public static void doListScheduledJobs(AdminClient adminClient, List<Integer> nodeIds) {
            // Print the job information
            for(Integer nodeId: nodeIds) {
                System.out.println("Retrieving scheduled jobs from node " + nodeId);
                List<String> jobIds = adminClient.rpcOps.getScheduledJobsList(nodeId);
                System.out.println("Scheduled job ids on node " + nodeId + " : " + jobIds);
                for(String jobId: jobIds) {
                    System.out.println("  Scheduled job id " + jobId + " is "
                                       + (adminClient.rpcOps.getScheduledJobStatus(nodeId, jobId) ? "enabled"
                                                                                                 : "disabled"));
                }
                System.out.println();
            }
        }
    }

    /**
     * scheduled-job stop command
     */
    public static class SubCommandScheduledStop extends AbstractAdminCommand {

        private static final String OPT_HEAD_SCHEDULED_STOP = "scheduled-stop";

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
            parser.accepts(OPT_HEAD_SCHEDULED_STOP, "list of job ids to be stopped")
                  .withOptionalArg()
                  .describedAs("job-id-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
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
            stream.println("  scheduled stop - Stop scheduled jobs on one node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  scheduled stop <job-id-list> -n <node-id> -u <url> [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and stops jobs on one node.
         * 
         * @param args Command-line input
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> jobIds = null;
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_SCHEDULED_STOP);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_SCHEDULED_STOP);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            jobIds = (List<String>) options.valuesOf(OPT_HEAD_SCHEDULED_STOP);
            if(jobIds.size() < 1) {
                throw new VoldemortException("Please specify scheduled jobs to stop.");
            }
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Stop scheduled jobs");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);
            System.out.println("Jobs to stop:");
            System.out.println("  " + Joiner.on(", ").join(jobIds));

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "stop scheduled job")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            doStopScheduledJobs(adminClient, nodeId, jobIds);
        }

        /**
         * Stop jobs on a single nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to stop jobs
         * @param jobIds List of jobs to be stopped
         */
        public static void doStopScheduledJobs(AdminClient adminClient,
                                          Integer nodeId,
                                               List<String> jobIds) {
            for(String jobId: jobIds) {
                System.out.println("Stopping job id " + jobId);
                adminClient.rpcOps.stopScheduledJob(nodeId, jobId);
                System.out.println("Stopped job id " + jobId);
            }
        }
    }

    /**
     * scheduled-job enable command
     */
    public static class SubCommandScheduledEnable extends AbstractAdminCommand {

        private static final String OPT_HEAD_SCHEDULED_ENABLE = "scheduled-enable";

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
            parser.accepts(OPT_HEAD_SCHEDULED_ENABLE, "list of job ids to be enabled")
                  .withOptionalArg()
                  .describedAs("job-id-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
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
            stream.println("  scheduled enable - Enable scheduled jobs on one node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  scheduled enable <job-id-list> -n <node-id> -u <url> [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and stops jobs on one node.
         * 
         * @param args Command-line input
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> jobIds = null;
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminToolUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_SCHEDULED_ENABLE);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_SCHEDULED_ENABLE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            jobIds = (List<String>) options.valuesOf(OPT_HEAD_SCHEDULED_ENABLE);
            if(jobIds.size() < 1) {
                throw new VoldemortException("Please specify scheduled jobs to enables.");
            }
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Enable scheduled jobs");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);
            System.out.println("Jobs to enable:");
            System.out.println("  " + Joiner.on(", ").join(jobIds));

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "enable scheduled job")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            doEnableScheduledJobs(adminClient, nodeId, jobIds);
        }

        /**
         * Enable jobs on a single nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to stop jobs
         * @param jobIds List of jobs to be stopped
         */
        public static void doEnableScheduledJobs(AdminClient adminClient,
                                          Integer nodeId,
                                                 List<String> jobIds) {
            for(String jobId: jobIds) {
                System.out.println("Enabling scheduled id " + jobId);
                adminClient.rpcOps.enableScheduledJob(nodeId, jobId);
                System.out.println("Enabled scheduled id " + jobId);
            }
        }
    }

}

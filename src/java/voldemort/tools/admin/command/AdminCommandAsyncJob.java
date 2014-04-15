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
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminUtils;

import com.google.common.base.Joiner;

/**
 * Implements all admin async-job commands.
 */
public class AdminCommandAsyncJob extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("list")) {
            SubCommandAsyncJobList.executeCommand(args);
        } else if(subCmd.equals("stop")) {
            SubCommandAsyncJobStop.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Async-Job Commands");
        stream.println("---------------------------------------");
        stream.println("list   Get async job list from nodes.");
        stream.println("stop   Stop async jobs on one node.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help async-job <command-name>\'.");
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
            SubCommandAsyncJobList.printHelp(stream);
        } else if(subCmd.equals("stop")) {
            SubCommandAsyncJobStop.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * async-job list command
     */
    private static class SubCommandAsyncJobList extends AbstractAdminCommand {

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
            stream.println("  async-job list - Get async job list from nodes");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  async-job list -u <url> [-n <node-id-list> | --all-nodes]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and gets async job list from nodes.
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
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            doAsyncJobList(adminClient, nodes);
        }

        /**
         * Gets list of async jobs across multiple nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes Nodes to get async jobs from
         */
        private static void doAsyncJobList(AdminClient adminClient, Collection<Node> nodes) {
            // Print the job information
            for(Node node: nodes) {
                System.out.println("Retrieving async jobs from node " + node.getId());
                List<Integer> asyncIds = adminClient.rpcOps.getAsyncRequestList(node.getId());
                System.out.println("Async Job Ids on node " + node.getId() + " : " + asyncIds);
                for(int asyncId: asyncIds) {
                    System.out.println("Async Job Id "
                                       + asyncId
                                       + " ] "
                                       + adminClient.rpcOps.getAsyncRequestStatus(node.getId(),
                                                                                  asyncId));
                    System.out.println();
                }
            }
        }
    }

    /**
     * async-job stop command
     */
    private static class SubCommandAsyncJobStop extends AbstractAdminCommand {

        private static final String OPT_HEAD_ASYNC_JOB_STOP = "async-job-stop";

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
            parser.accepts(OPT_HEAD_ASYNC_JOB_STOP, "list of job ids to be stopped")
                  .withOptionalArg()
                  .describedAs("job-id-list")
                  .withValuesSeparatedBy(',')
                  .ofType(Integer.class);
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
            stream.println("  async-job stop - Stop async jobs on one node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  async-job stop <job-id-list> -n <node-id> -u <url> [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and stops async jobs on one node.
         * 
         * @param args Command-line input
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<Integer> jobIds = null;
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_ASYNC_JOB_STOP);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_ASYNC_JOB_STOP);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            jobIds = (List<Integer>) options.valuesOf(OPT_HEAD_ASYNC_JOB_STOP);
            if(jobIds.size() < 1) {
                throw new VoldemortException("Please specify async jobs to stop.");
            }
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Stop async jobs");
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);
            System.out.println("Jobs to stop:");
            System.out.println("  " + Joiner.on(", ").join(jobIds));

            // execute command
            if(!AdminUtils.askConfirm(confirm, "stop async job")) {
                return;
            }
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);

            doAsyncJobStop(adminClient, node, jobIds);
        }

        /**
         * Stop async jobs on a single nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to stop async jobs
         * @param jobIds List of async jobs to be stopped
         */
        private static void doAsyncJobStop(AdminClient adminClient, Node node, List<Integer> jobIds) {
            for(Integer jobId: jobIds) {
                System.out.println("Stopping async id " + jobId);
                adminClient.rpcOps.stopAsyncRequest(node.getId(), jobId);
                System.out.println("Stopped async id " + jobId);
            }
        }
    }
}

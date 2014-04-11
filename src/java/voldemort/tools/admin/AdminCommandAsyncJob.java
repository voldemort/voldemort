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
import java.io.PrintStream;
import java.util.*;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.google.common.collect.Lists;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;

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
        if (subCmd.equals("list")) SubCommandAsyncJobList.executeCommand(args);
        else if (subCmd.equals("stop")) SubCommandAsyncJobStop.executeCommand(args);
        else if (subCmd.equals("help") || subCmd.equals("--help") || subCmd.equals("-h")) executeHelp(System.out, args);
        else printHelp(System.out);
    }

    /**
     * Prints command-line help menu for all async-job commands.
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
     * @throws Exception 
     */
    public static void executeHelp(PrintStream stream, String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if (subCmd.equals("list")) SubCommandAsyncJobList.printHelp(stream);
        else if (subCmd.equals("stop")) SubCommandAsyncJobStop.printHelp(stream);
        else printHelp(stream);
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
            AdminParserUtils.acceptsUrl(parser, true);
            AdminParserUtils.acceptsNodeMultiple(parser, false);
            AdminParserUtils.acceptsAllNodes(parser);
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> optionalNode = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_URL);
            optionalNode.add(AdminParserUtils.OPT_NODE);
            optionalNode.add(AdminParserUtils.OPT_ALL_NODES);

            // declare parameters
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;

            // parse command-line input
            OptionSet options = parser.parse(args);
            
            // load parameters
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if (options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            
            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkOptionalOne(options, optionalNode);

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
                    System.out.println("Async Job Id " + asyncId + " ] "
                                       + adminClient.rpcOps.getAsyncRequestStatus(node.getId(), asyncId));
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
            parser.accepts(OPT_HEAD_ASYNC_JOB_STOP, "list of job ids to be stopped")
                  .withRequiredArg()
                  .describedAs("job-id-list")
                  .withValuesSeparatedBy(',')
                  .ofType(Integer.class);
            AdminParserUtils.acceptsNodeSingle(parser, false);
            AdminParserUtils.acceptsUrl(parser, true);
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
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(OPT_HEAD_ASYNC_JOB_STOP);
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_URL);

            // declare parameters
            List<Integer> jobIds = null;
            Integer nodeId = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, OPT_HEAD_ASYNC_JOB_STOP);
            OptionSet options = parser.parse(args);
            
            // load parameters
            jobIds = (List<Integer>) options.valuesOf(OPT_HEAD_ASYNC_JOB_STOP);
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if (options.has(AdminParserUtils.OPT_CONFIRM)) {
            	confirm = true;
            }
            
            // checks correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            
            // execute command
            if (!AdminUtils.askConfirm(confirm, "stop async job")) {
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

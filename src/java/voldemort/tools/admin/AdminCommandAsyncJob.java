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

import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;

/**
 * Implements all functionality of admin async-job operations.
 * 
 */
public class AdminCommandAsyncJob {

    /**
     * Main entry of group command 'async-job', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     * 
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
        String subCmd = new String();
        if (args.length >= 2) subCmd = args[1];
        
        if (subCmd.equals("list")) executeAsyncJobList(args, printHelp);
        else if (subCmd.equals("stop")) executeAsyncJobStop(args, printHelp);
        else executeAsyncJobHelp();
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

    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeAsyncJobHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Async-Job Commands");
        System.out.println("---------------------------------------");
        System.out.println("list   Get async job list from nodes.");
        System.out.println("stop   Stop async jobs on one node.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help async-job <command-name>\'.");
        System.out.println();
    }

    /**
     * Parses command-line and gets async job list from nodes.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeAsyncJobList(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();

        // declare parameters
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  async-job list - Get async job list from nodes");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  async-job list -u <url> [-n <node-id-list> | --all-nodes]");
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

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);
        
        doAsyncJobList(adminClient, nodes);
    }

    /**
     * Parses command-line and stops async jobs on one node.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeAsyncJobStop(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<Integer> jobIds = null;
        Integer nodeId = null;
        String url = null;
        Boolean confirm = true;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEAD_ASYNC_JOB_STOP);
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  async-job stop - Stop async jobs on one node");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  async-job stop <job-id-list> -n <node-id> -u <url> [--confirm]");
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
        jobIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_HEAD_ASYNC_JOB_STOP);
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "stop async job")) return;

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
        
        doAsyncJobStop(adminClient, node, jobIds);
    }

}

/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ReadOnlyReplicationHelperCLI {

    private static Logger logger = Logger.getLogger(ReadOnlyReplicationHelperCLI.class);

    public final static int DEFAULT_NODE_ID = 0;
    public final static String SPLIT_LITERAL = "_";
    private final static String OPT_HELP = "help";
    private final static String OPT_URL = "url";
    private final static String OPT_NODE = "node";
    private final static String OPT_OUTPUT = "output";
    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts(OPT_HELP, "Print usage information");
        parser.accepts(OPT_URL, "Voldemort cluster bootstrap url")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts(OPT_NODE, "Destination node id on which files will be copied")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts(OPT_OUTPUT,
                       "Output file that contains lines of: store_name,src_node_id,src_file_name,dest_file_name")
              .withRequiredArg()
              .describedAs("output-file-path")
              .ofType(String.class);
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("\n");
        help.append("NAME\n");
        help.append("  ReadOnlyReplicationHelper - Get read-only data files to replicate for a node \n");
        help.append("\n");
        help.append("SYNOPSIS\n");
        help.append("   --" + OPT_URL + " <bootstrap-url> --" + OPT_NODE + " <node-id> [--"
                    + OPT_OUTPUT + " <optput-file-path>]\n");
        help.append("\n");
        System.out.print(help.toString());
        try {
            parser.printHelpOn(System.out);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage + "\n");
    }

    private static OptionSet getValidOptions(String[] args) {
        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch(OptionException oe) {
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
        }

        if(options.has(OPT_HELP)) {
            printUsage();
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, OPT_URL, OPT_NODE);
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }
        return options;
    }

    /**
     * This method take a list of fileName of the type partitionId_Replica_Chunk
     * and returns file names that match the regular expression
     * masterPartitionId_
     */
    private static List<String> parseAndCompare(List<String> fileNames, int masterPartitionId) {
        List<String> sourceFileNames = new ArrayList<String>();
        for(String fileName: fileNames) {
            String[] partitionIdReplicaChunk = fileName.split(SPLIT_LITERAL);
            if(Integer.parseInt(partitionIdReplicaChunk[0]) == masterPartitionId) {
                sourceFileNames.add(fileName);
            }
        }
        return sourceFileNames;
    }

    /**
     * Analyze read-only storage file replication info
     * 
     * @param cluster
     * @param nodeId
     * @return List of read-only replicaiton info in format of:
     *         store_name,src_node_id,src_file_name,dest_file_name
     */
    public static List<String> getReadOnlyReplicationInfo(AdminClient adminClient, Integer nodeId) {
        List<String> infoList = Lists.newArrayList();
        List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                     .getValue();
        Cluster cluster = adminClient.metadataMgmtOps.getRemoteCluster(nodeId).getValue();

        for(StoreDefinition storeDef: storeDefs) {
            String storeName = storeDef.getName();
            // Skip a store without replication, or has wrong ro storage format.
            String storageFormat = null;
            if(storeDef.getReplicationFactor() <= 1
               || !storeDef.getType().equals(ReadOnlyStorageConfiguration.TYPE_NAME)
               || !(storageFormat = adminClient.readonlyOps.getROStorageFormat(nodeId, storeName)).equals(ReadOnlyStorageFormat.READONLY_V2.getCode())) {
                logger.error("Unqualified store: " + storeName + ", replication factor = "
                             + storeDef.getReplicationFactor() + ", type = " + storeDef.getType()
                             + ", ro format = " + storageFormat);
                continue;
            }

            logger.info("Processing store " + storeName);

            RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                          cluster);

            // Go over the entire partitions and find if the destination node
            // belong in the replication partition list
            for(int masterPartitionId = 0; masterPartitionId < cluster.getNumberOfPartitions(); ++masterPartitionId) {
                List<Integer> naryPartitionIds = strategy.getReplicatingPartitionList(masterPartitionId);
                int nary = 0;
                Boolean hasPartition = false;

                for(int naryPartitionId: naryPartitionIds) {
                    Node naryNode = cluster.getNodeForPartitionId(naryPartitionId);
                    if(naryNode.getId() == nodeId) {
                        // Test if destination node has the partition of
                        // masterPartitionId, and record what kind of replica it
                        // is and break out of the loop
                        hasPartition = true;
                        naryPartitionIds.remove(nary);
                        break;
                    }
                    nary++;
                }

                if(!hasPartition) {
                    logger.trace("Node " + nodeId + " doesn't have partition " + masterPartitionId);
                    continue;
                }

                // Now find out which node hosts one of these replicas
                Node sourceNode = cluster.getNodeForPartitionId(naryPartitionIds.get(0));

                // Now get all the file names from this node.
                List<String> fileNames = adminClient.readonlyOps.getROStorageFileList(sourceNode.getId(),
                                                                                      storeDef.getName());

                List<String> sourceFileNames = parseAndCompare(fileNames, masterPartitionId);

                // Compare if there are files that relate to the
                // masterPartitionId
                if(sourceFileNames.size() > 0) {

                    for(String sourceFileName: sourceFileNames) {

                        String[] partitionIdReplicaChunk = sourceFileName.split(SPLIT_LITERAL);
                        // At the destination node the replicaId will be
                        // different, so only change it to nary
                        String partitionId = partitionIdReplicaChunk[0];
                        String replicaId = String.valueOf(nary);
                        String chunkId = partitionIdReplicaChunk[2];
                        // Now concat the parts together to create the file name
                        // on the destination node
                        String destFileName = partitionId.concat(SPLIT_LITERAL)
                                                         .concat(replicaId)
                                                         .concat(SPLIT_LITERAL)
                                                         .concat(chunkId);

                        infoList.add(storeName + "," + sourceNode.getId() + "," + sourceFileName
                                     + "," + destFileName);
                    }
                } else {
                    logger.warn("Cannot find file for partition " + masterPartitionId
                                + " on source node " + sourceNode.getId());
                }
            }
        }
        return infoList;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        // Required args
        String url = (String) options.valueOf(OPT_URL);
        Integer nodeId = (Integer) options.valueOf(OPT_NODE);
        PrintStream outputStream;
        if(options.has(OPT_OUTPUT)) {
            String output = (String) options.valueOf(OPT_OUTPUT);
            outputStream = new PrintStream(output);
        } else {
            outputStream = System.out;
        }

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());

        outputStream.println("store_name,source_node_id,source_file_name,dest_file_name");

        List<String> infoList = getReadOnlyReplicationInfo(adminClient, nodeId);
        for(String info: infoList) {
            outputStream.println(info);
        }

        if(outputStream != System.out) {
            outputStream.close();
        }
    }
}

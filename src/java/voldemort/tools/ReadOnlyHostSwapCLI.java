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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;

import com.google.common.base.Joiner;

public class ReadOnlyHostSwapCLI {

    public final static int DEFAULT_NODE_ID = 0;
    public final static String SPLIT_LITERAL = "_";
    private static OptionParser parser;

    private static void setupParser() {
        parser = new OptionParser();
        parser.accepts("help", "Print usage information");
        parser.accepts("cluster", "Path to cluster xml")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("nodeId", "Destination node id on which files will be copied")
              .withRequiredArg()
              .describedAs("nodeId")
              .ofType(Integer.class);
    }

    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ReadOnlyHostSwap\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --cluster <clusterXML>\n");
        help.append("    --nodeId <nodeId>\n");
        try {
            parser.printHelpOn(System.out);
        } catch(IOException e) {
            e.printStackTrace();
        }
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    private static OptionSet getValidOptions(String[] args) {
        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch(OptionException oe) {
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
        }

        if(options.has("help")) {
            printUsage();
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "cluster", "nodeId");
        if(missing.size() > 0) {
            printUsageAndDie("Missing required arguments: " + Joiner.on(", ").join(missing));
        }
        return options;
    }

    // This method take a list of fileName of the type partitionId_Replica_Chunk
    // and returns file names
    // that match the regular expression masterPartitionId_*
    public static List<String> parseAndCompare(List<String> fileNames, int masterPartitionId) {
        List<String> sourceFileNames = new ArrayList<String>();
        for(String fileName: fileNames) {
            String[] partitionIdReplicaChunk = fileName.split(SPLIT_LITERAL);
            if(Integer.parseInt(partitionIdReplicaChunk[0]) == masterPartitionId) {
                sourceFileNames.add(fileName);
            }
        }
        return sourceFileNames;
    }

    public static void main(String[] args) throws Exception {
        setupParser();
        OptionSet options = getValidOptions(args);

        // Required args
        String clusterXML = (String) options.valueOf("cluster");
        Integer nodeId = CmdUtils.valueOf(options, "nodeId", DEFAULT_NODE_ID);

        Cluster clusterDef = new ClusterMapper().readCluster(new File(clusterXML));
        AdminClient adminClient = new AdminClient(clusterDef,
                                                  new AdminClientConfig(),
                                                  new ClientConfig());

        for(StoreDefinition storeDef: adminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                 .getValue()) {

            // Skip a store with 1 or smaller replication factor.
            if(storeDef.getReplicationFactor() <= 1) {
                continue;
            }

            RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                          clusterDef);

            // Go over the entire partitions and find if the destination node
            // belong in the replication partition list
            for(int masterPartitionId = 0; masterPartitionId < clusterDef.getNumberOfPartitions(); ++masterPartitionId) {
                List<Integer> naryPartitionIds = strategy.getReplicatingPartitionList(masterPartitionId);
                int nary = 0;
                for(int naryPartitionId: naryPartitionIds) {
                    Node naryNode = clusterDef.getNodeForPartitionId(naryPartitionId);
                    if(naryNode.getId() == nodeId) {
                        // If the destination node is a replica, record what
                        // kind of replica it is and break out of the loop
                        naryPartitionIds.remove(nary);
                        break;
                    }
                    nary++;
                }
                // Now find out which node hosts one of these replicas

                Node replicatingNode = clusterDef.getNodeForPartitionId(naryPartitionIds.get(0));
                // Now get all the file names from this node.
                List<String> fileNames = adminClient.readonlyOps.getROStorageFileList(replicatingNode.getId(),
                                                                                      storeDef.getName());

                List<String> sourceFileNames = parseAndCompare(fileNames, masterPartitionId);

                // Compare if there are files that relate to the
                // masterPartitionId
                if(sourceFileNames.size() > 0) {

                    for(String sourceFileName: sourceFileNames) {

                        System.out.println("Source Node  " + replicatingNode
                                           + "File name on source Node " + sourceFileName);

                        String[] partitionIdReplicaChunk = sourceFileName.split(SPLIT_LITERAL);
                        // At the destination node the nary will be different,
                        // so change that
                        partitionIdReplicaChunk[1] = String.valueOf(nary);
                        // Now concat the parts together to create the file name
                        // on the destination node
                        String fileNameOnDestinationNode = partitionIdReplicaChunk[0].concat(SPLIT_LITERAL)
                                                                                     .concat(partitionIdReplicaChunk[1])
                                                                                     .concat(SPLIT_LITERAL)
                                                                                     .concat(partitionIdReplicaChunk[2]);

                        System.out.println("File name on destination node "
                                           + fileNameOnDestinationNode);
                    }
                }
            }
        }
    }
}

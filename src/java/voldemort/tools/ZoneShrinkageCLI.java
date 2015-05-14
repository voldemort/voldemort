/**
 * Copyright 2014 LinkedIn, Inc
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

import static voldemort.VoldemortAdminTool.executeSetMetadataPair;
import static voldemort.store.metadata.MetadataStore.CLUSTER_KEY;
import static voldemort.store.metadata.MetadataStore.STORES_KEY;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * This tool change the cluster topology by dropping one zone
 */
public class ZoneShrinkageCLI {

    public static Logger logger = Logger.getLogger(ZoneShrinkageCLI.class);
    protected AdminClient adminClient;
    protected final Integer droppingZoneId;
    protected final String bootstrapUrl;

    public static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("u", "url"), "Bootstrap URL of target cluster")
              .withRequiredArg()
              .ofType(String.class)
              .describedAs("bootstrap-url");
        parser.acceptsAll(Arrays.asList("i", "drop-zoneid"), "ID of the zone to be dropped")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("zone-id");
        parser.acceptsAll(Arrays.asList("real-run"),
                          "If and only if this option is specified, the program will actually execute the shrinkage(Real Run). Otherwise, it will not actually execute the shrinkage");
        parser.acceptsAll(Arrays.asList("h", "help"), "Show help message");

        return parser;
    }

    public static void validateOptions(OptionSet options) throws IOException {
        Integer exitStatus = null;
        if(options.has("help")) {
            exitStatus = 0;
            System.out.println("This tool changes the targeted cluster topology by shrinking one zone. The replication of data in other zones will not change and the targeted zone will disappear from current routing strategy");
        } else if(!options.has("url")) {
            System.err.println("Option \"url\" is required");
            exitStatus = 1;
        } else if(!options.has("drop-zoneid")) {
            System.err.println("Option \"drop-zoneid\" is required");
            exitStatus = 1;
        }
        if(exitStatus != null) {
            if(exitStatus == 0)
                getParser().printHelpOn(System.out);
            else
                getParser().printHelpOn(System.err);
            System.exit(exitStatus);
        }
    }

    public static void main(String[] argv) throws Exception {
        OptionParser parser = getParser();
        OptionSet options = parser.parse(argv);
        validateOptions(options);

        ZoneShrinkageCLI cli = new ZoneShrinkageCLI((String) options.valueOf("url"),
                                                    (Integer) options.valueOf("drop-zoneid"));
        try {
            cli.executeShrink(options.has("real-run"));
        } catch(Exception e) {
            e.printStackTrace();
            logAbort();
        }
    }

    public ZoneShrinkageCLI(String url, Integer droppingZoneId) {
        AdminClientConfig acc = new AdminClientConfig();
        ClientConfig cc = new ClientConfig();
        adminClient = new AdminClient(url, acc, cc);
        this.droppingZoneId = droppingZoneId;
        this.bootstrapUrl = url;
    }

    protected static boolean verifyMetadataConsistency(AdminClient adminClient,
                                                       Collection<Node> nodes,
                                                       String clusterXml,
                                                       String storesXml) {

        boolean success = true;
        logger.info("Checking metadata consistency on all servers");
        for(Node node: nodes) {
            int nodeId = node.getId();

            boolean nodeGood = true;
            Versioned<String> currentClusterXmlVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                         CLUSTER_KEY);
            Versioned<String> currentStoresXmlVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                        STORES_KEY);

            if(currentClusterXmlVersioned == null) {
                logger.error("Cluster XML does not exist on node " + nodeId);
                nodeGood = false;
            } else {
                if(clusterXml.equals(currentClusterXmlVersioned.getValue())) {
                    logger.info("Node " + nodeId + " cluster.xml is GOOD (cluster xml)");
                } else {
                    logger.info("Node " + nodeId + " cluster.xml is BAD (cluster xml)");
                    nodeGood = false;
                }
            }
            if(currentStoresXmlVersioned == null) {
                logger.error("Stores XML does not exist on node " + nodeId);
                nodeGood = false;
            } else {
                if(storesXml.equals(currentStoresXmlVersioned.getValue())) {
                    logger.info("Node " + nodeId + " stores.xml is GOOD (stores xml)");
                } else {
                    logger.info("Node " + nodeId + " stores.xml is BAD (stores xml)");
                    nodeGood = false;
                }
            }
            success = success && nodeGood;
        }
        logger.info("Finished checking metadata consistency on all servers");
        return success;
    }

    protected static boolean verifyRebalanceState(AdminClient adminClient, Collection<Node> nodes) {
        boolean success = true;
        logger.info("Checking server states on all nodes");
        for(Node node: nodes) {
            Integer nodeId = node.getId();

            Versioned<String> stateVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                             MetadataStore.SERVER_STATE_KEY);
            if(stateVersioned == null) {
                logger.error("Node " + nodeId + " State object is null");
                success = false;
            } else {
                if(!stateVersioned.getValue()
                                  .equals(MetadataStore.VoldemortState.NORMAL_SERVER.toString())) {
                    logger.error("Node " + nodeId + " Server state is not normal");
                    success = false;
                } else {
                    logger.info("Node " + nodeId + " is GOOD (rebalance state)");
                }
            }
        }
        logger.info("Finished checking server states on all nodes");
        return success;
    }

    protected static String shrinkClusterXml(String clusterXml, int droppingZoneId) {
        Cluster initialCluster = new ClusterMapper().readCluster(new StringReader(clusterXml));
        Cluster intermediateCluster = RebalanceUtils.vacateZone(initialCluster, droppingZoneId);
        Cluster finalCluster = RebalanceUtils.dropZone(intermediateCluster, droppingZoneId);

        String newClusterXml = new ClusterMapper().writeCluster(finalCluster);
        return newClusterXml;
    }

    protected static String shrinkStoresXml(String storesXml, int droppingZoneId) {
        List<StoreDefinition> initialStoreDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml));
        List<StoreDefinition> finalStoreDefs = RebalanceUtils.dropZone(initialStoreDefs,
                                                                       droppingZoneId);

        String newStoresXml = new StoreDefinitionsMapper().writeStoreList(finalStoreDefs);
        return newStoresXml;
    }

    public void executeShrink(boolean realRun) {
        Cluster initialCluster = adminClient.getAdminClientCluster();
        Collection<Node> initialClusterNodes = initialCluster.getNodes();
        Integer initialClusterFirstNodeId = initialCluster.getNodes().iterator().next().getId();

        boolean shouldContinue;

        // Get Metadata from one server
        logger.info("Start fetching metadata for server " + initialClusterFirstNodeId);
        String initialClusterXml = adminClient.metadataMgmtOps.getRemoteMetadata(initialClusterFirstNodeId,
                                                                                 CLUSTER_KEY)
                                                              .getValue();
        String initialStoresXml = adminClient.metadataMgmtOps.getRemoteMetadata(initialClusterFirstNodeId,
                                                                                STORES_KEY)
                                                             .getValue();
        logger.info("End fetching metadata for server " + initialClusterFirstNodeId);

        logger.info("Original cluster.xml: \n" + initialClusterXml + "\n");
        logger.info("Original stores.xml: \n" + initialStoresXml + "\n");

        // Query the servers to see if all have the same XML
        shouldContinue = verifyMetadataConsistency(adminClient,
                                                   initialClusterNodes,
                                                   initialClusterXml,
                                                   initialStoresXml);
        if(!shouldContinue) {
            logAbort();
            return;
        }

        // Calculate and print out the new metadata
        String newStoresXml = shrinkStoresXml(initialStoresXml, droppingZoneId);
        String newClusterXml = shrinkClusterXml(initialClusterXml, droppingZoneId);

        logger.info("New cluster.xml: \n" + newClusterXml + "\n");
        logger.info("New stores.xml: \n" + newStoresXml + "\n");

        // Verifying Server rebalancing states
        shouldContinue = verifyRebalanceState(adminClient, initialClusterNodes);
        if(!shouldContinue) {
            logAbort();
            return;
        }

        // Run shrinkage
        if(realRun) {
            logger.info("Updating metadata(cluster.xml, stores.xml) on all nodes");
            executeSetMetadataPair(-1,
                                   adminClient,
                                   MetadataStore.CLUSTER_KEY,
                                   newClusterXml,
                                   MetadataStore.STORES_KEY,
                                   newStoresXml);
        } else {
            logger.info("(dry-run)Skipping updating metadata");
        }

        // Check metadata consistency
        if(realRun) {
            shouldContinue = verifyMetadataConsistency(adminClient,
                                                       initialClusterNodes,
                                                       newClusterXml,
                                                       newStoresXml);
            if(!shouldContinue) {
                logAbort();
                return;
            }
        } else {
            logger.info("(dry-run)Skipping verifying new metadata");
        }

        // Verifying Server rebalancing states
        shouldContinue = verifyRebalanceState(adminClient, initialClusterNodes);
        if(!shouldContinue) {
            logAbort();
        } else {
            logger.info("Shrinkage on " + bootstrapUrl + " is completed");
            logger.info("=========================================================");
            logger.info("||     DDDDDDD        OOO       NN    NN    EEEEEEEE   ||");
            logger.info("||     DD    DD     OO   OO     NNN   NN    EE         ||");
            logger.info("||     DD     DD   OO     OO    NNNN  NN    EE         ||");
            logger.info("||     DD     DD   OO     OO    NN NN NN    EEEEEEEE   ||");
            logger.info("||     DD     DD   OO     OO    NN  NNNN    EE         ||");
            logger.info("||     DD    DD     OO   OO     NN   NNN    EE         ||");
            logger.info("||     DDDDDDD        OOO       NN    NN    EEEEEEEE   ||");
            logger.info("=========================================================");
        }
    }

    protected static void logAbort() {
        logger.info("===========================================================");
        logger.info("||     FFFFFFFF       AA          IIIIII      LL         ||");
        logger.info("||     FF            AAAA           II        LL         ||");
        logger.info("||     FF           AA  AA          II        LL         ||");
        logger.info("||     FFFFFFFF    AA    AA         II        LL         ||");
        logger.info("||     FF         AAAAAAAAAA        II        LL         ||");
        logger.info("||     FF         AA      AA        II        LL         ||");
        logger.info("||     FF         AA      AA      IIIIII      LLLLLLLL   ||");
        logger.info("===========================================================");
    }
}

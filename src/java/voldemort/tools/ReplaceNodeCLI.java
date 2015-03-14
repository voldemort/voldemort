package voldemort.tools;

import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class ReplaceNodeCLI {

    private String url;
    private String newUrl;
    private int nodeId;
    private boolean skipRestore;
    private int parallelism;
    private int newNodeId;

    private AdminClient adminClient;
    private AdminClient newAdminClient;

    private Cluster cluster;
    private Cluster newCluster;

    private List<StoreDefinition> storeDefinitions;

    private String clusterXml;
    private String storesXml;

    private static final Logger logger = Logger.getLogger(ZoneClipperCLI.class);

    public ReplaceNodeCLI(String url,
                          int nodeId,
                          String newUrl,
                          boolean skipRestore,
                          int parallelism) {
        this.url = url;
        this.nodeId = nodeId;
        this.newUrl = newUrl;
        this.skipRestore = skipRestore;
        this.parallelism = parallelism;

        init();
    }

    private void init() {
        this.adminClient = new AdminClient(this.url, new AdminClientConfig(), new ClientConfig());
        this.newAdminClient = new AdminClient(this.newUrl,
                                              new AdminClientConfig(),
                                              new ClientConfig());

        this.cluster = adminClient.getAdminClientCluster();
        this.newCluster = newAdminClient.getAdminClientCluster();

        if(newCluster.getNumberOfNodes() > 1) {
            newNodeId = nodeId;
        } else {
            newNodeId = newCluster.getNodeIds().iterator().next().intValue();
        }

        this.clusterXml = getClusterXML();

        // Update your cluster XML based on the consensus
        this.cluster = new ClusterMapper().readCluster(new StringReader(clusterXml));
        this.storesXml = getStoresXML();
        this.storeDefinitions = new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml),
                                                                           false);
    }

    public void execute() {
        this.verifyPreConditions();

        this.makeServersOffline();

        this.modifyTopology();

        if(skipRestore == false) {
            this.restoreFromReplica();
        }

        this.enableSlopStreaming();

        this.updateClusterVersion();

        this.verifyPostConditions();
    }

    private Map<Integer, String> getMetadataXML(String key) {
        Map<Integer, String> metadataXMLsInNodes = new HashMap<Integer, String>();
        for(Integer i: cluster.getNodeIds()) {
            try {
                Versioned<String> xmlVersionedValue = adminClient.metadataMgmtOps.getRemoteMetadata(i.intValue(),
                                                                                                    key);
                String xml = xmlVersionedValue.getValue();
                metadataXMLsInNodes.put(i.intValue(), xml);
            } catch(UnreachableStoreException e) {
                if(i.intValue() == nodeId) {
                    logger.info("Ignoring unreachable store error on the node being replaced "
                                + nodeId, e);
                } else {
                    throw e;
                }
            }
        }

        if(metadataXMLsInNodes.size() < 1) {
            throw new VoldemortApplicationException("No XML found or only one node in cluster for Key "
                                                    + key);
        }

        return metadataXMLsInNodes;
    }

    private String getClusterXML() {
        ClusterMapper clusterMapper = new ClusterMapper();
        Cluster existingCluster = null;
        String clusterXML = null;

        Map<Integer, String> clusterXMLInNodes = getMetadataXML(MetadataStore.CLUSTER_KEY);

        for(Map.Entry<Integer, String> clusterNodeId: clusterXMLInNodes.entrySet()) {
            String xml = clusterNodeId.getValue();
            Cluster cluster = clusterMapper.readCluster(new StringReader(xml), false);
            if(existingCluster == null) {
                existingCluster = cluster;
                clusterXML = xml;
            } else if(existingCluster.equals(cluster) == false) {
                throw new VoldemortApplicationException("Cluster XMLs are different across nodes, fix that before the node swap...aborting "
                            + clusterNodeId.getKey());
            }
        }
        return clusterXML;
    }

    private String getStoresXML() {
        StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

        Map<Integer, String> storeXMLInNodes = getMetadataXML(MetadataStore.STORES_KEY);
        List<StoreDefinition> storeList = null;
        String storeXML = null;

        for(Map.Entry<Integer, String> storeNodeId: storeXMLInNodes.entrySet()) {
            String xml = storeNodeId.getValue();
            List<StoreDefinition> storeDefinitions = storeMapper.readStoreList(new StringReader(xml),
                                                                               false);
            if(storeList == null) {
                storeList = storeDefinitions;
                storeXML = xml;
            } else if(storeList.equals(storeDefinitions) == false) {
                throw new VoldemortApplicationException("Store XMLs are different across nodes, fix that before the node swap...aborting "
                            + storeNodeId.getKey());
            }
        }
        return storeXML;
    }

    /**
     * Return args parser
     *
     * @return program parser
     * */
    private static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url for the cluster in which a node is to be replaced")
              .ofType(String.class);
        parser.accepts("node", "[REQUIRED] node id.")
              .withRequiredArg()
              .describedAs("node id which needs to be replaced by the new node")
              .ofType(Integer.class);
        parser.accepts("newurl", "[REQUIRED] new bootstrap-url")
              .withRequiredArg()
              .describedAs("bootstrap-url for the new cluster, which will replace the node id")
              .ofType(String.class);
        parser.accepts("skip-restore", "do not restore data from existing machine");
        parser.accepts("parallelism", "parallel data restores.")
              .withRequiredArg()
              .describedAs("number of data restores to happen in parallel")
              .ofType(Integer.class)
              .defaultsTo(3);
        return parser;
    }

    /**
     * Print Usage to STDOUT
     */
    private static void printUsage() {
        StringBuilder help = new StringBuilder();
        help.append("ReplaceNodeCLI Tool\n");
        help.append("  Replace a node in the cluster with a new node.\n");
        help.append("Options:\n");
        help.append("  Required:\n");
        help.append("    --url <bootstrap-url>\n");
        help.append("    --node <nodeIdToBeReplaced>\n");
        help.append("    --newurl <newVoldemortClusterWithSingleNode>\n");
        help.append("  Optional:\n");
        help.append("    --skip-restore \n");
        help.append("    --parallelism <numberOfRestoresInParallel> \n");
        help.append("    --help\n");
        help.append("  Notes:\n");
        help.append("    use this command to replace a node in the voldemort cluster\n");
        help.append("    There are two recommended ways to set up the new node that replaces old node.\n");
        help.append("    1) Make the new node as only node in voldemort cluster without any data.\n");
        help.append("       The data will be restored from other nodes in the cluster during replacement.\n");
        help.append("    2) Move the hard disk from the failed node to the new node or restore the data, \n");
        help.append("       config( cluster.xml, stores) manually. Fix the host, port in the \n");
        help.append("       cluster.xml to reflect the new node instead of old node. \n");
        help.append("       use --skip-restore option to disable data restore in this case. \n");
        help.append("       Second way is the only way to restore a node in read only cluster. \n");
        help.append("    After the node is replaced, the new node will be left in offline state and slop enabled. \n");
        help.append("    Wait for slops to drain to an acceptable level. Make the new node online via admin shell \n");
        help.append("    to complete the operation. \n");
        System.out.print(help.toString());
    }

    private static void printUsageAndDie(String errMessage) {
        printUsage();
        Utils.croak("\n" + errMessage);
    }

    private void verifyAdminPort(Collection<Node> nodes, String url) {
        boolean isAdminPortUsed = false;
        for(Node node: nodes) {
            int adminPort = node.getAdminPort();

            URI bootstrapUrl = null;
            try {
                bootstrapUrl = new URI(url);
            } catch(Exception e) {
                logger.error("error parsing url " + url, e);
                throw new VoldemortApplicationException("error parsing url " + url + "  "
                                                        + e.getMessage());
            }
            int urlPort = bootstrapUrl.getPort();
            if(urlPort == adminPort) {
                isAdminPortUsed = true;
                break;
            }
        }

        if(isAdminPortUsed == false) {
            throw new VoldemortApplicationException("The bootstrap URL should point to the admin port as the client port will be made offline ... aborting "
                        + " url " + url);
        }
    }

    private void verifyPostConditions() {
        try {
            getClusterXML();
            getStoresXML();
        } catch(VoldemortApplicationException e) {
            throw new VoldemortApplicationException(" Verify Post conditions failed after the node is replaced "
                                                            + e.getMessage(),
                                                    e);
        }
    }

    private void verifyPreConditions() {
        // Verify node id exists in the current cluster
        if(cluster.hasNodeWithId(nodeId) == false) {
            throw new VoldemortApplicationException(" Node "
                                                    + nodeId
                        + " could not be found in the existing cluster. Please check the node id ");
        }

        verifyAdminPort(cluster.getNodes(), url);
        verifyAdminPort(Arrays.asList(newCluster.getNodeById(newNodeId)), newUrl);

        List<StoreDefinition> newStoreDefinitions = this.newAdminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                                       .getValue();

        Set<StoreDefinition> existingStores = new HashSet<StoreDefinition>();
        existingStores.addAll(storeDefinitions);

        Set<StoreDefinition> newStores = new HashSet<StoreDefinition>();
        newStores.addAll(newStoreDefinitions);

        if(skipRestore) {
            // If no restore verify that the cluster has same stores as the
            // existing ones.

            if(existingStores.equals(newStores) == false) {
                throw new VoldemortApplicationException("Command called with skip data restore, but store definitions do not match... aborting ");
            }

            // If the machine has other hardware failure, but hard drive is
            // intact one way to restore is take the hard drive attach it to a
            // new machine and bring that machine up. In that case when the
            // voldemort is started on the new machine it might have the same
            // number of nodes as the old cluster.
            if(newCluster.getNumberOfNodes() > 1) {
                // If hard drive restore node count should match, if not
                // something fishy, abort
                if(cluster.getNumberOfNodes() != newCluster.getNumberOfNodes()) {
                    throw new VoldemortApplicationException("number of nodes in new "
                                + newCluster.getNumberOfNodes()
                                + " and old cluster "
                                + cluster.getNumberOfNodes()
                                + " not the same. New cluster can have one or equal number of nodes ... aborting");
                }

                for(Node oldNode: cluster.getNodes()) {

                    Node newNode = newCluster.getNodeById(oldNode.getId());

                    if(oldNode.getId() == nodeId) {
                        // new cluster xml needs to be edited with new host
                        // names. if they match the cluster.xml is not updated
                        // with new host names, report error and abort
                        if(oldNode.isEqualState(newNode)) {
                            // TODO: this might report issue if replaceNode
                            // command is restarted.
                            throw new VoldemortApplicationException(" node in the new cluster has the same metadata as the node being replaced. \n"
                                        + "fix the new cluster to have the correct host and ports... aborting");
                        }
                    } else {
                        if(oldNode.isEqualState(newNode) == false) {
                            throw new VoldemortApplicationException(" node in the old and new cluster should be the same except for the one being replaced. "
                                        + "Node id : " + oldNode.getId() + "... aborting");
                        }
                    }

                    if(oldNode.getPartitionIds().equals(newNode.getPartitionIds()) == false) {
                        throw new VoldemortApplicationException("old node and new node has different partition ids, is this a correct replacement for node Id"
                                    + oldNode.getId() + " ... aborting");
                    }
                }
            }
        } else {
            // Verify that the new cluster has only one node so that you can
            // replace the failing node with this node. If it has multiple
            // nodes, it could involve complications so it is better if the
            // cluster is edited to contain only the single node to be replaced.
            if(newCluster.getNumberOfNodes() != 1) {
                // TODO: this might report issue if replaceNode command is
                // restarted.
                throw new VoldemortApplicationException("needs data restore and new cluster has more than one nodes... aborting");
            }

            List<StoreDefinition> readOnlyStoreDefs = StoreDefinitionUtils.filterStores(storeDefinitions,
                                                                                        true);

            if(readOnlyStoreDefs.size() > 0) {
                throw new VoldemortApplicationException("data restore not supported for clusters with read only stores. Read only store name "
                            + readOnlyStoreDefs.get(0));
            }

            if(existingStores.equals(newStores) == false) {
                if(newStores.size() != 0) {
                    throw new VoldemortApplicationException(" Stores xml in the new cluster should either match the old cluster or should be empty ");
                }
            }

        }
    }

    private void setNodeOffline(AdminClient adminClient, int nodeId) {
        adminClient.metadataMgmtOps.setRemoteOfflineState(nodeId, true);

        adminClient.metadataMgmtOps.fetchAndUpdateRemoteMetadata(nodeId,
                                                                 MetadataStore.SLOP_STREAMING_ENABLED_KEY,
                                                                 Boolean.toString(false));
    }

    private void makeServersOffline() {
        // Put the old node in offline state, if it is up and running
        try {
            setNodeOffline(adminClient, nodeId);
        } catch(UnreachableStoreException e) {
            logger.info("Ignoring the error while updating the old node.", e);
        }

        setNodeOffline(newAdminClient, newNodeId);
    }

    private String updateClusterXML() {
        Node nodeToAdd = newCluster.getNodeById(newNodeId);

        List<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        List<Zone> zones = new ArrayList<Zone>(cluster.getZones());

        Node nodeToRemove = cluster.getNodeById(nodeId);

        Node newNode = new Node(nodeId,
                                nodeToAdd.getHost(),
                                nodeToAdd.getHttpPort(),
                                nodeToAdd.getSocketPort(),
                                nodeToAdd.getAdminPort(),
                                nodeToRemove.getZoneId(),
                                nodeToRemove.getPartitionIds(),
                                nodeToAdd.getRestPort());

        boolean isInserted = false;

        for(int i = 0; i < nodes.size(); i++) {
            if(nodes.get(i).getId() == nodeId) {
                nodes.remove(i);
                nodes.add(i, newNode);
                isInserted = true;
                break;
            }
        }

        if(isInserted == false) {
            logger.error("Unable to insert the new node, something odd happened");
            throw new VoldemortApplicationException("Unable to insert the new node, something odd happened");
        }

        Cluster updatedCluster = new Cluster(cluster.getName(), nodes, zones);
        return new ClusterMapper().writeCluster(updatedCluster);
    }

    private void modifyTopology() {
        List<StoreDefinition> newStoreDefinitions = this.newAdminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                                       .getValue();
        

        String updatedClusterXML = updateClusterXML();
        String storesXML = new StoreDefinitionsMapper().writeStoreList(storeDefinitions);

        List<Integer> newNodeIdAsList = new ArrayList<Integer>();
        newNodeIdAsList.add(newNodeId);

        newAdminClient.metadataMgmtOps.updateRemoteMetadata(newNodeIdAsList,
                                                            MetadataStore.CLUSTER_KEY,
                                                            updatedClusterXML);

        if(newStoreDefinitions.size() == 0) {
            for(StoreDefinition def: storeDefinitions) {
                newAdminClient.storeMgmtOps.addStore(def, newNodeIdAsList);
            }
        }

        newAdminClient.metadataMgmtOps.updateRemoteMetadata(newNodeIdAsList,
                                                            MetadataStore.STORES_KEY,
                                                            storesXML);

        newAdminClient.metadataMgmtOps.updateRemoteMetadata(newNodeIdAsList,
                                                            MetadataStore.NODE_ID_KEY,
                                                            Integer.toString(nodeId));

        List<Integer> oldNodeIds = new ArrayList<Integer>(cluster.getNodeIds());
        oldNodeIds.remove(nodeId);
        adminClient.metadataMgmtOps.updateRemoteMetadata(oldNodeIds,
                                                         MetadataStore.CLUSTER_KEY,
                                                         updatedClusterXML);

        init();
    }

    private void restoreFromReplica() {
        adminClient.restoreOps.restoreDataFromReplications(this.nodeId, this.parallelism);
    }

    private void enableSlopStreaming() {
        List<Integer> nodeBeingReplaced = new ArrayList<Integer>();
        nodeBeingReplaced.add(nodeId);

        newAdminClient.metadataMgmtOps.updateRemoteMetadata(nodeBeingReplaced,
                                                            MetadataStore.SLOP_STREAMING_ENABLED_KEY,
                                                            Boolean.toString(true));
    }

    private void updateClusterVersion() {
        newAdminClient.metadataMgmtOps.updateMetadataversion(SystemStoreConstants.CLUSTER_VERSION_KEY);
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = null;
        OptionSet options = null;
        try {
            parser = getParser();
            options = parser.parse(args);
        } catch(OptionException oe) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Exception when parsing arguments : " + oe.getMessage());
            return;
        }

        /* validate options */
        if(options.hasArgument("help")) {
            parser.printHelpOn(System.out);
            printUsage();
            return;
        }
        if(!options.hasArgument("url") || !options.hasArgument("node")
           || !options.hasArgument("newurl")) {
            parser.printHelpOn(System.out);
            printUsageAndDie("Missing a required argument.");
            return;
        }

        String url = (String) options.valueOf("url");
        String newUrl = (String) options.valueOf("newurl");
        int nodeId = ((Integer) options.valueOf("node")).intValue();

        boolean skipRestore = options.has("skip-restore");
        int parallelism = ((Integer) options.valueOf("parallelism")).intValue();

        if(parallelism <= 0) {
            Utils.croak(" parallelism " + parallelism + " should be a positive integer ");
        }

        ReplaceNodeCLI nodeReplacer = new ReplaceNodeCLI(url,
                                                         nodeId,
                                                         newUrl,
                                                         skipRestore,
                                                         parallelism);

        try {
            nodeReplacer.execute();
        } catch(VoldemortApplicationException e) {
            logger.error("Error during node replace", e);
            Utils.croak(e.getMessage());
        }
    }
}

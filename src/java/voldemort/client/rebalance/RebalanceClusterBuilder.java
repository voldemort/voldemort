package voldemort.client.rebalance;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.ClusterViewer;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * Insert a node into the cluster in a way that preserves most of the existing cluster's characteristics
 * and moves as little data around as possible.
 * 
 * Most of the logic resides in {@link voldemort.client.rebalance.RebalanceClusterTool}, see that class for
 * more documentation.
 * 
 */
public class RebalanceClusterBuilder {
    private final Cluster cluster;
    private final List<StoreDefinition> stores;
    private final int maxRemappedReplicas;
    private final int minNumPartitions;
    private final int desiredNumPartitions;

    private RebalanceClusterBuilder(Cluster cluster,
                                    List<StoreDefinition> stores,
                                    int maxRemappedReplicas,
                                    int minNumPartitions,
                                    int desiredNumPartitions) {
        this.cluster = cluster;
        this.stores = stores;
        this.maxRemappedReplicas = maxRemappedReplicas;
        this.minNumPartitions = minNumPartitions;
        this.desiredNumPartitions = desiredNumPartitions;
    }

    /**
     * Create an instance of <tt>RebalanceClusterBuilder</tt>. Parses the cluster.xml and stores.xml file to
     * find the store with the highest replication-factor.
     *
     * @param clusterXmlFile Path to the original cluster.xml
     * @param storesXmlFile Path to the stores.xml
     * @param maxRemappedReplicas Maximum number of partition to replica mappings that can change
     * @param minNumPartitions Minimum number of partitions that should be moved to new nodes
     * @param desiredNumPartitions Desired number of partitions to move to the new node
     * @return Constructed instance of <tt>RebalanceClusterBuilder</tt>
     * @throws IOException If unable to read from cluster.xml or stores.xml
     */
    public static RebalanceClusterBuilder create(String clusterXmlFile,
                                                 String storesXmlFile,
                                                 int maxRemappedReplicas,
                                                 int minNumPartitions,
                                                 int desiredNumPartitions) throws IOException {
        Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterXmlFile)));
        List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storesXmlFile)));

        if (desiredNumPartitions < minNumPartitions)
            desiredNumPartitions = cluster.getNumberOfPartitions() / (cluster.getNumberOfNodes() + 1);

        // TODO: replace this with a more reasonable default
        if (maxRemappedReplicas < 0)
            maxRemappedReplicas = cluster.getNumberOfPartitions() / 2;

        return new RebalanceClusterBuilder(cluster,
                                           stores,
                                           maxRemappedReplicas,
                                           minNumPartitions,
                                           desiredNumPartitions);
    }

    /**
     * Create a targetCluster.xml as a string. If a file is specified, output to that file; otherwise print to
     * screen. Fail and exit if we're unable to add the new node at all.
     *
     * @param targetClusterXmlFile File to write to (will be written to STDOUT if null)
     * @param host Host name of the new node
     * @param httpPort Http port for the new node
     * @param socketPort Socket port for the new node
     * @param adminPort Admin port for the new node
     * @throws IOException If we're unable to write the cluster XML to file
     */
    public void build(String targetClusterXmlFile,
                      String host,
                      int httpPort,
                      int socketPort,
                      int adminPort) throws IOException {
        // First find the store with the highest N
        StoreDefinition store = stores.get(0);
        for (int i=1; i < stores.size(); i++) {
            StoreDefinition curr = stores.get(i);
            
            if (store.getReplicationFactor() > curr.getReplicationFactor())
                store = curr;
        }

        RebalanceClusterTool clusterTool = new RebalanceClusterTool(cluster, store);
        ClusterViewer clusterViewer = new ClusterViewer(cluster, store);

        System.out.println("Original layout: ");
        clusterViewer.viewMasterToReplica();

        Node template = new Node(cluster.getNumberOfNodes(),
                                 host,
                                 httpPort,
                                 socketPort,
                                 adminPort,
                                 ImmutableList.<Integer>of());

        System.out.println("Inserting " + template + "\n");
        System.out.println("Configuration " + Joiner.on(" ")
                       .withKeyValueSeparator("=")
                       .join(ImmutableMap.<String, Integer>builder()
                                      .put("maxRemappedReplicas", maxRemappedReplicas)
                                      .put("minNumPartitions", minNumPartitions)
                                      .put("desiredNumPartitions", desiredNumPartitions)
                                      .build()));
        
        Cluster targetCluster = clusterTool.insertNode(template,
                                                       minNumPartitions,
                                                       desiredNumPartitions,
                                                       maxRemappedReplicas);
        if (targetCluster == null)
            Utils.croak("Unable to insert " + template + " into " + cluster);

        System.out.println("Created target cluster layout: ");
        clusterViewer.viewMasterToReplica(targetCluster);
        clusterViewer.compareToCluster(targetCluster);

        String clusterXmlString = new ClusterMapper().writeCluster(targetCluster);
        if (targetClusterXmlFile == null) {
            System.err.println("Warning: target-cluster not specified, printing to STDOUT instead\n");
            System.out.println(clusterXmlString);
        } else {
            BufferedWriter out = new BufferedWriter(new FileWriter(targetClusterXmlFile));
            try {
                out.write(clusterXmlString);
            } finally {
                out.close();
            }
            System.out.println("Wrote target cluster.xml to " + targetClusterXmlFile);
        }
    }

    /**
     * Main method to run to create a targetCluster.xml. Example usage:
     * <code>
     * ./bin/voldemort-rebalance-configure.sh \
     * --cluster ./test/common/voldemort/config/rebalance-tool-big-cluster.xml \
     * --stores ./test/common/voldemort/config/rebalance-tool-stores.xml \
     * --host node6 --http-port 8081 --socket-port 6666 --target-cluster ./targetCluster.xml
     * </code>
     * @param args See USAGE for details
     * @throws IOException If unable to read input files or write to output file
     */
    public static void main(String [] args) throws IOException {
        OptionParser parser = new OptionParser();

        parser.accepts("help", "print usage information");
        parser.accepts("stores", "[REQUIRED] path to the stores xml config file")
                       .withRequiredArg()
                       .describedAs("stores.xml");
        parser.accepts("cluster", "[REQUIRED] path to the ORIGINAL cluster xml config file")
                       .withRequiredArg()
                       .describedAs("cluster.xml");
        parser.accepts("target-cluster", "path to the TARGET cluster xml config file")
                       .withRequiredArg()
                       .describedAs("targetCluster.xml");

        parser.accepts("host", "[REQUIRED] new node's host name")
                       .withRequiredArg()
                       .describedAs("host-name");

        parser.accepts("http-port", "[REQUIRED] new node's http port")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("http-port");
        parser.accepts("socket-port", "[REQUIRED] new node's socket port")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("socket-port");
        parser.accepts("admin-port", "new node's admin port")
                       .withRequiredArg()
                       .ofType(Integer.class)
                       .describedAs("admin-port");

        parser.accepts("max-remaps", "Maximum number of replication mappings that may change")
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("desired-partitions", "Desired number of partitions per node")
                       .withRequiredArg()
                       .ofType(Integer.class);
        parser.accepts("min-partitions", "Minimum number of partitions per node")
                       .withRequiredArg()
                       .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "stores",
                                               "cluster",
                                               "host",
                                               "http-port",
                                               "socket-port");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String storesXmlFile = (String) options.valueOf("stores");
        String clusterXmlFile = (String) options.valueOf("cluster");
        String targetClusterXmlFile = (String) options.valueOf("target-cluster");

        int maxRemappedReplicas = CmdUtils.valueOf(options, "max-remaps", -1);
        int minNumPartitions = CmdUtils.valueOf(options, "min-partitions", 1);
        int desiredNumPartitions = CmdUtils.valueOf(options, "desired-partitions", -1);

        String host = (String) options.valueOf("host");
        int httpPort = (Integer) options.valueOf("http-port");
        int socketPort = (Integer) options.valueOf("socket-port");
        int adminPort = CmdUtils.valueOf(options, "admin-port", socketPort + 1);

        RebalanceClusterBuilder rebalanceClusterBuilder = create(clusterXmlFile,
                                                                 storesXmlFile,
                                                                 maxRemappedReplicas,
                                                                 minNumPartitions,
                                                                 desiredNumPartitions);
        rebalanceClusterBuilder.build(targetClusterXmlFile,
                                      host,
                                      httpPort,
                                      socketPort,
                                      adminPort);
    }
}

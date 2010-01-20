package voldemort;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.rebalance.RebalanceClusterTool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Inspect a cluster configuration to determine whether it can be rebalanced to a target cluster geometry.
 *
 * @author afeinberg
 */
public class ClusterViewer {

    private final Cluster originalCluster;
    private final StoreDefinition storeDefinition;


    /**
     * Constructs a <code>ClusterViewer</code> initialized with an original cluster a store definition.
     *
     * @param originalCluster The original cluster
     * @param storeDefinition A store definition, which specifies the replication-factor (needed to calculate routes). 
     */
    public ClusterViewer(Cluster originalCluster,
                         StoreDefinition storeDefinition) {
        this.originalCluster = originalCluster;
        this.storeDefinition = storeDefinition;

    }

    /**
     * Prints out the layout display mapping between replicas and their partitions for the cluster with which
     * the <code>ClusterViewer</code> has been constructed.
     */
    public void viewMasterToReplica() {
        viewMasterToReplica(originalCluster);
    }

    /**
     * Prints out the a layout displaying mapping between replicas and their partitions for a given cluster.
     *
     * @param cluster Cluster to examine
     */
    public void viewMasterToReplica(Cluster cluster) {
        RebalanceClusterTool clusterTool = new RebalanceClusterTool(cluster, storeDefinition);
        System.out.println(cluster);
        Multimap<Integer,Integer> masterToReplicas = clusterTool.getMasterToReplicas();
        System.out.println("\nReplication: ");
        for (int partition: masterToReplicas.keySet()) {
            Set<Integer> replicas = Sets.union(ImmutableSet.of(partition),
                                               ImmutableSet.copyOf(masterToReplicas.get(partition)));
            System.out.println("\t" + partition + " -> " + Joiner.on(", ").join(replicas));
        }
        System.out.println();
    }

    /**
     * Compares cluster with which the <code>ClusterViewer</code> has been constructed to another cluster,
     * determining the feasibility of rebalancing to that cluster's layout.
     *
     * @param target The target cluster geometry.
     */
    public void compareToCluster(Cluster target) {
        RebalanceClusterTool clusterTool = new RebalanceClusterTool(originalCluster, storeDefinition);
        Multimap<Node,Integer> multipleCopies = clusterTool.getMultipleCopies(target);
        if (multipleCopies.size() > 0) {
            for (Node n: multipleCopies.keySet()) {
                System.out.println(n + " has multiple copies of data: " + Joiner.on(", ").join(multipleCopies.get(n)));
            }
        } else
            System.out.println("No multiple copies found.");

        System.out.println();
        
        Multimap<Integer, Pair<Integer,Integer>> remappedReplicas = clusterTool.getRemappedReplicas(target);
        if (remappedReplicas.size() > 0) {
            for (int partition: remappedReplicas.keySet()) {
                System.out.println("Mapping for partition " + partition + " has changed: ");
                for (Pair<Integer,Integer> pair: remappedReplicas.get(partition)) {
                    System.out.println("\tUsed to have " + partition + " -> " + pair.getFirst() + "; now have: "
                                       + partition + " -> " + pair.getSecond());
                }
            }
        } else
            System.out.println("No replica mappings have changed.");
    }

    public static void main(String [] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("stores", "[REQUIRED] path to stores xml config file")
                       .withRequiredArg()
                       .describedAs("stores.xml");
        parser.accepts("cluster", "[REQUIRED] path to cluster xml config file")
                       .withRequiredArg()
                       .describedAs("cluster.xml");
        parser.accepts("other-cluster", "Cluster to compare with")
                       .withRequiredArg()
                       .describedAs("targetCluster.xml");
        parser.accepts("store-name", "[REQUIRED] store name")
                       .withRequiredArg()
                       .describedAs("store name");
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "store-name",
                                               "cluster",
                                               "stores");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String clusterFile = (String) options.valueOf("cluster");
        String storesFile = (String) options.valueOf("stores");
        String storeName = (String) options.valueOf("store-name");

        try {
            Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));
            List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storesFile)));
            StoreDefinition storeDef = null;

            for (StoreDefinition def: stores) {
                if (def.getName().equals(storeName))
                    storeDef = def;
            }

            if (storeDef == null)
                Utils.croak("No store found with name \"" + storeName + "\"");

            ClusterViewer clusterViewer = new ClusterViewer(cluster, storeDef);

            System.out.println("Original cluster: ");
            clusterViewer.viewMasterToReplica();

            if (options.has("other-cluster")) {
                String otherClusterFile = (String) options.valueOf("other-cluster");
                Cluster otherCluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(otherClusterFile)));

                System.out.println("New cluster: ");
                clusterViewer.viewMasterToReplica(otherCluster);
                clusterViewer.compareToCluster(otherCluster);
            }

        } catch (FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
    }
    
}

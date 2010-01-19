package voldemort;

import com.google.common.base.Joiner;
import com.google.common.collect.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
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
 @author afeinberg
 */
public class ClusterViewer {
    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final RoutingStrategy routingStrategy;
    private final Multimap<Integer,Integer> partitionMap;
    private final Multimap<Integer,Integer> replicaMap;

    public ClusterViewer (Cluster cluster,
                          StoreDefinition storeDefinition,
                          RoutingStrategy routingStrategy) {
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.routingStrategy = routingStrategy;

        Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>> geometry = createGeometry();
        partitionMap = geometry.getFirst();
        replicaMap = geometry.getSecond();
    }

    protected Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>> createGeometry () {
        return createGeometry(this.cluster, this.routingStrategy);
    }

    protected Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>> createGeometry (Cluster cluster, RoutingStrategy routingStrategy) {
        Multimap<Integer, Integer> nodePartitions = LinkedHashMultimap.create();
        Multimap<Integer, Integer> replicas = TreeMultimap.create();

        for  (Node node: cluster.getNodes()) {
            List<Integer> partitionIds = node.getPartitionIds();
            nodePartitions.replaceValues(node.getId(), partitionIds);

            for (int partitionId: partitionIds) {
                if (!replicas.containsKey(partitionId))
                    replicas.replaceValues(partitionId, routingStrategy.getReplicatingPartitionList(partitionId));
            }
        }

        return new Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>>(nodePartitions, replicas);
    }

    public void view () {
        view(this.partitionMap, this.replicaMap);
    }

    public void view (Cluster cluster, RoutingStrategy routingStrategy) {
        Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>> geometry = createGeometry(cluster, routingStrategy);
        view(geometry.getFirst(), geometry.getSecond());
    }

    public void view (Multimap<Integer,Integer> partitionMap, Multimap<Integer,Integer> replicaMap) {
        Multimap<Integer,Integer> invertedPartitionMap = LinkedHashMultimap.create();
        Multimaps.invertFrom(partitionMap, invertedPartitionMap);

        System.out.println("====================================");
        System.out.println("store: " + storeDefinition.getName());
        System.out.println("====================================");

        for (int partitionId: replicaMap.keySet()) {
            int masterNode = invertedPartitionMap.get(partitionId).iterator().next();
            System.out.println("partition " + partitionId + " mastered by node " + masterNode) ;
            System.out.println("\t replicas: " + Joiner.on(", ").join(replicaMap.get(partitionId)));
        }

        System.out.println();
    }

    public void compareMapping(Cluster otherCluster, RoutingStrategy otherRoutingStrategy) {
        Pair<Multimap<Integer,Integer>,Multimap<Integer,Integer>> otherGeometry = createGeometry(otherCluster, otherRoutingStrategy);
        Multimap<Integer,Integer> otherPartitions = otherGeometry.getFirst();
        Multimap<Integer,Integer> otherReplicas = otherGeometry.getSecond();

        // First check for broken replicas (i.e. partition x *used* to be a replica of partition y, but is not
        // a replica of it any longer).

        Set<Map.Entry<Integer,Integer>> replicaPairs = ImmutableSet.<Map.Entry<Integer,Integer>>builder()
                .addAll(replicaMap.entries())
                .build();
        Set<Map.Entry<Integer,Integer>> otherReplicaPairs = ImmutableSet.<Map.Entry<Integer,Integer>>builder().
                addAll(otherReplicas.entries())
                .build();

        for (Map.Entry<Integer,Integer> pair: replicaPairs) {
            if (!otherReplicaPairs.contains(pair))
                System.out.println("Other cluster lacks replication " + pair.getKey() + " -> " + pair.getValue());
        }

        // Now check for double replication
        for (int nodeId: otherPartitions.keySet()) {
            Set<Integer> partitionsAtNode = ImmutableSet.<Integer>builder().addAll(otherPartitions.get(nodeId)).build();
            for (int partitionId: partitionsAtNode) {
                for (int replicaId: replicaMap.get(partitionId)) {
                    if (replicaId != partitionId && partitionsAtNode.contains(replicaId))
                        System.err.println("Double replication on node " + nodeId + ": " + replicaId +
                                           " is replica of " + partitionId);
                }
            }
        }
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

            RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster);

            ClusterViewer clusterViewer = new ClusterViewer(cluster,
                              storeDef,
                              routingStrategy);
            System.out.println("Original cluster: ");
            clusterViewer.view();

            if (options.has("other-cluster")) {
                String otherClusterFile = (String) options.valueOf("other-cluster");
                Cluster otherCluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(otherClusterFile)));
                RoutingStrategy otherRoutingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                          otherCluster);

                System.out.println("New cluster: ");
                clusterViewer.view(otherCluster, otherRoutingStrategy);
                clusterViewer.compareMapping(otherCluster, otherRoutingStrategy);
            }

        } catch (FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
    }
    
}

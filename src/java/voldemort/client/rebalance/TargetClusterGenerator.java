package voldemort.client.rebalance;

import com.google.common.collect.*;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;

import java.util.Map;
import java.util.Set;


/**
 * Generate a target cluster for rebalancing given an existing cluster and a new node.
 *
 * @author afeinberg
 */
public class TargetClusterGenerator {
    private final StoreDefinition storeDefinition;
    private final Multimap<Integer,Integer> masterToReplicas;

    public TargetClusterGenerator(Cluster cluster, StoreDefinition storeDefinition) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster);
        ImmutableMultimap.Builder<Integer,Integer> builder = ImmutableSetMultimap.builder();

        for (int i = 0; i < cluster.getNumberOfPartitions(); i++) {
            builder.putAll(i, routingStrategy.getReplicatingPartitionList(i));
        }

        this.masterToReplicas = builder.build();
        this.storeDefinition = storeDefinition;
    }

    /**
     * When a cluster geometry is re-arranged, it is possible that one more node will end up with
     * a set of partitions such that one of the partitions in that set had been (in the original cluster
     * configuration) a replica of another partition in that set. Thus, we end up losing copies of data.
     * <p>
     * For example, suppose N=2 and original cluster configuration is:
     * <ul>
     * <li>Node A: 0, 3, 6</li>
     * <li>Node B: 1, 4, 7</li>
     * <li>Node C: 2, 5, 8</li>
     * </ul>
     * According to {@link voldemort.routing.ConsistentRoutingStrategy}, the replica mapping is this:
     * <ul>
     * <li>Partition 0 is replicated to 1</li>
     * <li>Partition 1 is replicated to 2</li>
     * <li>Partition 2 is replicated to 3</li>
     * <li>Partition 3 is replicated to 4</li>
     * <li>Partition 4 is replicated to 5</li>
     * <li>Partition 5 is replicated to 6</li>
     * <li>Partition 6 is replicated to 7</li>
     * <li>Partition 7 is replicated to 8</li>
     * <li>Partition 8 is replicated to 0</li>
     * </ul>
     * Now suppose we add a new machine D and rebalance the cluster as:
     * <ul>
     * <li>Node A: 0, 3, 6</li>
     * <li>Node B: 1, 4</li>
     * <li>Node C: 2, 5</li>
     * <li>Node D: 7, 8</li>
     * As you can see, node D holds partitions 7 and 8 which in the original cluster configuration
     * we replicas of each other. Now the only populated replica of partition 7 resides on the <b>same node</b> as
     * partition 7 itself.
     *
     * @param newCluster Suggested cluster geometry
     * @return True if there would multiple copies of the same data on a node, false otherwise.
     */
    public boolean hasMultipleCopies(Cluster newCluster) {
        for (Node n: newCluster.getNodes()) {
            Set<Integer> partitionSet = ImmutableSet.<Integer>builder()
                           .addAll(n.getPartitionIds())
                           .build();

            for (int partition: partitionSet) {
                for (int replica: masterToReplicas.get(partition)) {
                    if (replica != partition && partitionSet.contains(replica))
                        return true;
                }
            }
        }

        return false;
    }

    /***
     * When a new is node is inserted into a cluster and existing partitions are placed on that node, the partition
     * replication scheme may be remapped. This counts how many partitions->replica mappings from the original cluster
     * had been removed in the new cluster.
     *
     * @param newCluster Suggested cluster geometry
     * @return Count of replicas that have been lost, for all partitions in the cluster
     */
    public int getRemappedReplicaCount(Cluster newCluster) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, newCluster);
        ImmutableMultimap.Builder<Integer,Integer> builder = ImmutableSetMultimap.builder();

        for (int i = 0; i < newCluster.getNumberOfPartitions(); i++) {
            builder.putAll(i, routingStrategy.getReplicatingPartitionList(i));
        }

        Multimap<Integer,Integer> newMasterToReplicas = builder.build();

        int missingReplicas = 0;
        for (Map.Entry<Integer,Integer> entry: masterToReplicas.entries()) {
            int master = entry.getKey();
            int replica = entry.getValue();
            if (!newMasterToReplicas.get(master).contains(replica))
                missingReplicas++;
        }

        return missingReplicas;
    }
}

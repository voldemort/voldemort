package voldemort.client.rebalance;

import com.google.common.collect.*;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;

import java.util.*;

/**
 * Tools to manipulate cluster geometries and verify them for correctness, reliability and efficiency.
 *
 * @author afeinberg
 */
public class RebalanceClusterTool {

    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final ListMultimap<Integer,Integer> masterToReplicas;

    /**
     * Constructs a <tt>RebalanceClusterTool</tt> for a given cluster and store definition.
     *
     * @param cluster Original cluster
     * @param storeDefinition Store definition to extract information such as replication-factor from. Typically
     * this should be the store with the highest replication count. 
     */
    public RebalanceClusterTool(Cluster cluster, StoreDefinition storeDefinition) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, cluster);

        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.masterToReplicas = createMasterToReplicas(cluster, routingStrategy);
    }

    /**
     * Get a mapping of master partition to replicas of that partition. If a store's replication-factor is N,
     * a key is mastered by partition n<sub>0</sub>, then partitions n<sub>i</sub> (for 0 < i < N) are replicas of
     * partition  n<sub>_</sub>0 <b>iff</b> any requests for this key to this store are also routed to those partitions
     * (in addition to partition n<sub>0</sub>).
     *
     * @return Multimap with key being partition id, values being replicas of the partition
     */
    public Multimap<Integer,Integer> getMasterToReplicas() {
        return masterToReplicas;
    }

    private ListMultimap<Integer,Integer> createMasterToReplicas(Cluster cluster, RoutingStrategy routingStrategy) {
        ListMultimap<Integer,Integer> lmm = ArrayListMultimap.create();
        for (int i = 0; i < cluster.getNumberOfPartitions(); i++) {
            for (int replica: routingStrategy.getReplicatingPartitionList(i)) {
                if (replica != i)
                    lmm.put(i, replica);
            }
        }

        return lmm;
    }

    /**
     * Attempt inserting a node into a cluster the <code>RebalanceClusterTool</tt> was constructed with
     * while following these constraints:
     * <ul>
     * <li>
     * No node should receive multiple copies of same data
     * (checked by {@link voldemort.client.rebalance.RebalanceClusterTool#getMultipleCopies(voldemort.cluster.Cluster)}.
     * </li>
     * <li>Partitions should be as evenly distributed among nodes as possible.</li>
     * <li>Number of replicas re-mapped should be as low as possible
     * (checked by {@link voldemort.client.rebalance.RebalanceClusterTool#getRemappedReplicas(voldemort.cluster.Cluster)}).
     * <li>Minimize the number of copies that have to be performed.</li>
     * </li>
     * </ul>
     *
     * @param template A template node: with correct hostname, port, id information but with arbitrary partition ids
     * @return If successful, a new cluster containing the template node; otherwise null.
     */
    public Cluster insertNode(Node template,
                              int minNumberOfPartitions,
                              int desiredNumberOfPartitions) {
        // TODO: complete implementation
        return null;
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
     * Now node D holds partitions 7 and 8 which in the original cluster configuration
     * we replicas of each other. This means the only populated replica of partition 7 resides on the
     * <b>same node</b> as partition 7 itself, reducing node-level redundancy for keys mastered by that
     * partition. 
     *
     * @param newCluster Suggested cluster geometry
     * @return <p> Multimap with key being the node with multiple copies, values being the copies (including the
     * master partition). For described example it would be <code>{Node_D: [7,8]}</code>. </p>
     */
    public Multimap<Node,Integer> getMultipleCopies(Cluster newCluster) {
        Multimap<Node,Integer> copies = LinkedHashMultimap.create();
        for (Node n: newCluster.getNodes()) {
            List<Integer> partitions = n.getPartitionIds();

            for (int partition: partitions) {
                for (int replica: masterToReplicas.get(partition)) {
                    if (partitions.contains(replica)) {
                        if (!copies.get(n).contains(partition))
                            copies.put(n, partition);
                        copies.put(n, replica);
                    }
                }
            }
        }

        return copies;
    }

    /**
     * When {@link voldemort.routing.ConsistentRoutingStrategy} is used, replication mapping of partitions
     * (i.e., if a key k is mastered by partition p, in addition to p, which partitions can have requests for
     * k routed to them?) is determined by the replication-factor N and the nodes in the cluster, such that
     * each partition is replicated to N distinct nodes.
     *
     * @param newCluster Suggested cluster geometry
     * @return <p> Multimap with key being a master replica, values being pairs of (original replica, new replica).
     * For example target layout described in
     * {@link RebalanceClusterTool#getMultipleCopies(voldemort.cluster.Cluster)}
     * the return value would be <code>{7: [(7,8), (7,0)]}</code>. </p>
     */
    public Multimap<Integer, Pair<Integer,Integer>> getRemappedReplicas(Cluster newCluster) {
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition, newCluster);
        ListMultimap<Integer,Integer> newMasterToReplicas = createMasterToReplicas(newCluster, routingStrategy);

        Multimap<Integer, Pair<Integer,Integer>> remappedReplicas = ArrayListMultimap.create();
        for (int partition: masterToReplicas.keySet()) {
            List<Integer> oldReplicas = masterToReplicas.get(partition);
            List<Integer> newReplicas = newMasterToReplicas.get(partition);

            if (oldReplicas.size() != newReplicas.size())
                throw new IllegalStateException("replica count differs for partition " + partition);

            for (int i=0; i < oldReplicas.size(); i++) {
                int oldReplica = oldReplicas.get(i);
                if (!newReplicas.contains(oldReplica)) {
                    Pair<Integer,Integer> pair = new Pair<Integer,Integer>(oldReplica, newReplicas.get(i));
                    remappedReplicas.put(partition, pair);
                }
            }
        }

        return remappedReplicas;
    }

    /**
     * If we were to rebalance to the specified geometry, would there be multiple copies of the same partition
     * residing on the same node? See
     * {@link RebalanceClusterTool#getMultipleCopies(voldemort.cluster.Cluster)}
     * for more detailed documentation.
     *
     * @param newCluster Suggested cluster geometry.
     * @return True if there are multiple copies of data on the same node, false otherwise
     */
    public boolean hasMultipleCopies(Cluster newCluster) {
        return getMultipleCopies(newCluster).size() > 0;
    }

    /**
     * If we were to rebalance to the specified geometry, determine how many existing replication mappings would
     * change. See
     * {@link RebalanceClusterTool#getRemappedReplicas(voldemort.cluster.Cluster)}
     * for more detailed documentation.
     *
     * @param newCluster Suggested cluster geometry.
     * @return Count of changed partition to replica mappings.
     */
    public int getRemappedReplicaCount(Cluster newCluster) {
        return getRemappedReplicas(newCluster).entries().size();
    }
}

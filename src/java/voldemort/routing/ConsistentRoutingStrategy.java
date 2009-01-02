package voldemort.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import voldemort.cluster.Node;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;

/**
 * A Routing strategy that routes each request to the first N nodes where N is a
 * user defined replication factor.
 * 
 * The mapping is computed by creating partitions of a fixed size, and
 * maintaining a mapping from partition tag to Node. These nodes are mapped onto
 * a ring.
 * 
 * A preference list of nodes to route to is created by taking the partition
 * into which the key hashes, and then taking the next N nodes on the ring.
 * 
 * @author jay
 * 
 */
public class ConsistentRoutingStrategy implements RoutingStrategy {

    private final int numResults;
    private final Node[] partitionToNode;
    private final HashFunction hash;

    public ConsistentRoutingStrategy(Collection<Node> nodes, int numReplicas) {
        this(new FnvHashFunction(), nodes, numReplicas);
    }

    public ConsistentRoutingStrategy(HashFunction hash, Collection<Node> nodes, int numReplicas) {
        this.numResults = numReplicas;
        this.hash = hash;
        SortedMap<Integer, Node> m = new TreeMap<Integer, Node>();
        for (Node n : nodes) {
            for (Integer partition : n.getPartitionIds()) {
                if (m.containsKey(partition))
                    throw new IllegalArgumentException("Duplicate partition id " + partition
                                                       + " in cluster configuration.");
                m.put(partition, n);
            }
        }

        this.partitionToNode = new Node[m.size()];
        for (int i = 0; i < m.size(); i++) {
            if (!m.containsKey(i))
                throw new IllegalArgumentException("Missing tag " + i);
            this.partitionToNode[i] = m.get(i);
        }
    }

    public List<Node> routeRequest(byte[] key) {
        List<Node> preferenceList = new ArrayList<Node>(numResults);
        int index = Math.abs(hash.hash(key)) % this.partitionToNode.length;
        for (int i = 0; i < partitionToNode.length; i++) {
            // add this one if we haven't already
            if (!preferenceList.contains(partitionToNode[index]))
                preferenceList.add(partitionToNode[index]);

            // if we have enough, go home
            if (preferenceList.size() >= numResults)
                return preferenceList;
            // move to next clockwise slot on the ring
            index = (index + 1) % partitionToNode.length;
        }

        // we don't have enough, but that may be okay
        return preferenceList;
    }

    public Set<Node> getNodes() {
        Set<Node> s = new HashSet<Node>();
        for (Node n : this.partitionToNode)
            s.add(n);
        return s;
    }

    Node getNodeByPartition(int partition) {
        return partitionToNode[partition];
    }

    Set<Integer> getPartitionsByNode(Node n) {
        Set<Integer> tags = new HashSet<Integer>();
        for (int i = 0; i < partitionToNode.length; i++)
            if (partitionToNode[i].equals(n))
                tags.add(i);
        return tags;
    }

}

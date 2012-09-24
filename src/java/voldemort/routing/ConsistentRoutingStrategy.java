/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.utils.ByteUtils;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;

import com.google.common.collect.Sets;

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
 * 
 */
public class ConsistentRoutingStrategy implements RoutingStrategy {

    // the replication factor.
    private final int numReplicas;
    private final Node[] partitionToNode;
    private final HashFunction hash;

    private static final Logger logger = Logger.getLogger(ConsistentRoutingStrategy.class);

    public ConsistentRoutingStrategy(Collection<Node> nodes, int numReplicas) {
        this(new FnvHashFunction(), nodes, numReplicas);
    }

    public int getNumReplicas() {
        return this.numReplicas;
    }

    public Node[] getPartitionToNode() {
        return partitionToNode;
    }

    public ConsistentRoutingStrategy(HashFunction hash, Collection<Node> nodes, int numReplicas) {
        this.numReplicas = numReplicas;
        this.hash = hash;
        // sanity check that we dont assign the same partition to multiple nodes
        SortedMap<Integer, Node> m = new TreeMap<Integer, Node>();
        for(Node n: nodes) {
            for(Integer partition: n.getPartitionIds()) {
                if(m.containsKey(partition))
                    throw new IllegalArgumentException("Duplicate partition id " + partition
                                                       + " in cluster configuration " + nodes);
                m.put(partition, n);
            }
        }

        this.partitionToNode = new Node[m.size()];
        for(int i = 0; i < m.size(); i++) {
            if(!m.containsKey(i))
                throw new IllegalArgumentException("Invalid configuration, missing partition " + i);
            this.partitionToNode[i] = m.get(i);
        }
    }

    /**
     * A modified version of abs that always returns a non-negative value.
     * Math.abs returns Integer.MIN_VALUE if a == Integer.MIN_VALUE and this
     * method returns Integer.MAX_VALUE in that case.
     */
    private static int abs(int a) {
        if(a >= 0)
            return a;
        else if(a != Integer.MIN_VALUE)
            return -a;
        return Integer.MAX_VALUE;
    }

    public List<Node> routeRequest(byte[] key) {
        List<Integer> partitionList = getPartitionList(key);

        if(partitionList.size() == 0)
            return new ArrayList<Node>(0);
        // pull out the nodes corresponding to the target partitions
        List<Node> preferenceList = new ArrayList<Node>(partitionList.size());
        for(int partition: partitionList) {
            preferenceList.add(partitionToNode[partition]);
        }
        if(logger.isDebugEnabled()) {
            StringBuilder nodeList = new StringBuilder();
            for(int partition: partitionList) {
                nodeList.append(partitionToNode[partition].getId() + ",");
            }
            logger.debug("Key " + ByteUtils.toHexString(key) + " mapped to Nodes [" + nodeList
                         + "] Partitions [" + partitionList + "]");
        }
        return preferenceList;
    }

    public List<Integer> getReplicatingPartitionList(int index) {
        List<Node> preferenceList = new ArrayList<Node>(numReplicas);
        List<Integer> replicationPartitionsList = new ArrayList<Integer>(numReplicas);

        if(partitionToNode.length == 0) {
            return new ArrayList<Integer>(0);
        }
        // go over clockwise to find the next 'numReplicas' unique nodes
        // to replicate to
        for(int i = 0; i < partitionToNode.length; i++) {
            // add this one if we haven't already
            if(!preferenceList.contains(partitionToNode[index])) {
                preferenceList.add(partitionToNode[index]);
                replicationPartitionsList.add(index);
            }

            // if we have enough, go home
            if(preferenceList.size() >= numReplicas)
                return replicationPartitionsList;
            // move to next clockwise slot on the ring
            index = (index + 1) % partitionToNode.length;
        }

        // we don't have enough, but that may be okay
        return replicationPartitionsList;
    }

    /**
     * Obtain the master partition for a given key
     * 
     * @param key
     * @return
     */
    public Integer getMasterPartition(byte[] key) {
        return abs(hash.hash(key)) % (Math.max(1, this.partitionToNode.length));
    }

    public Set<Node> getNodes() {
        Set<Node> s = Sets.newHashSetWithExpectedSize(partitionToNode.length);
        for(Node n: this.partitionToNode)
            s.add(n);
        return s;
    }

    Node getNodeByPartition(int partition) {
        return partitionToNode[partition];
    }

    Set<Integer> getPartitionsByNode(Node n) {
        Set<Integer> tags = new HashSet<Integer>();
        for(int i = 0; i < partitionToNode.length; i++)
            if(partitionToNode[i].equals(n))
                tags.add(i);
        return tags;
    }

    public List<Integer> getPartitionList(byte[] key) {
        // hash the key and perform a modulo on the total number of partitions,
        // to get the master partition
        int index = getMasterPartition(key);
        if(logger.isDebugEnabled()) {
            logger.debug("Key " + ByteUtils.toHexString(key) + " primary partition " + index);
        }
        // Now based on the preference list, pick the replicating partitions and
        // return
        return getReplicatingPartitionList(index);
    }

    public String getType() {
        return RoutingStrategyType.CONSISTENT_STRATEGY;
    }
}

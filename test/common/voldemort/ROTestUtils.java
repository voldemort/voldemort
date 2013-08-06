/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/*
 * This class contains utils methods related to RO (read-only) stores. Historically, 
 * a structure named "replica_type" existed that was unfortunately hard coded into the file names 
 * on disk for the RO stores. Later, when almost all of the code base was refactored to remove 
 * replica_type this last piece of code that still had the notion of replica_type tied to it
 * could not be removed. 
 * 
 * The following two methods are called only from test classes and are exclusively related to
 * the RO store.
 * 
 */

public class ROTestUtils {
    
    private static Logger logger = Logger.getLogger(ROTestUtils.class);
    
    
    /**
     * Given a list of tuples of [replica_type, partition], flattens it and
     * generates a map of replica_type to partition mapping
     * 
     * @param partitionTuples Set of <replica_type, partition> tuples
     * @return Map of replica_type to set of partitions
     */

    public static HashMap<Integer, List<Integer>> flattenPartitionTuples(Set<Pair<Integer, Integer>> partitionTuples) {
        HashMap<Integer, List<Integer>> flattenedTuples = Maps.newHashMap();
        for(Pair<Integer, Integer> pair: partitionTuples) {
            if(flattenedTuples.containsKey(pair.getFirst())) {
                flattenedTuples.get(pair.getFirst()).add(pair.getSecond());
            } else {
                List<Integer> newPartitions = Lists.newArrayList();
                newPartitions.add(pair.getSecond());
                flattenedTuples.put(pair.getFirst(), newPartitions);
            }
        }
        return flattenedTuples;
    }
    
    /**
     * For a particular cluster creates a mapping of node id to their
     * corresponding list of [ replicaType, partition ] tuple
     * 
     * @param cluster The cluster metadata
     * @param storeDef The store definition
     * @param includePrimary Include the primary partition?
     * @return Map of node id to set of [ replicaType, partition ] tuple
     */

    public static Map<Integer, Set<Pair<Integer, Integer>>> getNodeIdToAllPartitions(final Cluster cluster,
                                                                                     final StoreDefinition storeDef,
                                                                                     boolean includePrimary) {
        final RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                   cluster);

        final Map<Integer, Set<Pair<Integer, Integer>>> nodeIdToReplicas = new HashMap<Integer, Set<Pair<Integer, Integer>>>();
        final Map<Integer, Integer> partitionToNodeIdMap = cluster.getPartitionIdToNodeIdMap();

        // Map initialization.
        for(Node node: cluster.getNodes()) {
            nodeIdToReplicas.put(node.getId(), new HashSet<Pair<Integer, Integer>>());
        }

        // Track how many zones actually have partitions (and so replica types)
        // in them.
        int zonesWithPartitions = 0;
        for(Integer zoneId: cluster.getZoneIds()) {
            if(cluster.getNumberOfPartitionsInZone(zoneId) > 0) {
                zonesWithPartitions++;
            }
        }

        // Loops through all nodes
        for(Node node: cluster.getNodes()) {

            // Gets the partitions that this node was configured with.
            for(Integer primary: node.getPartitionIds()) {

                // Gets the list of replicating partitions.
                List<Integer> replicaPartitionList = routingStrategy.getReplicatingPartitionList(primary);

                if((replicaPartitionList.size() % zonesWithPartitions != 0)
                   || ((replicaPartitionList.size() / zonesWithPartitions) != (storeDef.getReplicationFactor() / cluster.getNumberOfZones()))) {

                    // For zone expansion & shrinking, this warning is expected
                    // in some cases. For other use cases (shuffling, cluster
                    // expansion), this warning indicates that something
                    // is wrong between the clusters and store defs.
                    logger.warn("Number of replicas returned (" + replicaPartitionList.size()
                                + ") does not make sense given the replication factor ("
                                + storeDef.getReplicationFactor() + ") and that there are "
                                + cluster.getNumberOfZones() + " zones of which "
                                + zonesWithPartitions + " have partitions (and of which "
                                + (cluster.getNumberOfZones() - zonesWithPartitions)
                                + " are empty).");
                }

                int replicaType = 0;
                if(!includePrimary) {
                    replicaPartitionList.remove(primary);
                    replicaType = 1;
                }

                // Get the node that this replicating partition belongs to.
                for(Integer replicaPartition: replicaPartitionList) {
                    Integer replicaNodeId = partitionToNodeIdMap.get(replicaPartition);

                    // The replicating node will have a copy of primary.
                    nodeIdToReplicas.get(replicaNodeId).add(Pair.create(replicaType, primary));

                    replicaType++;
                }
            }
        }
        return nodeIdToReplicas;
    }

}

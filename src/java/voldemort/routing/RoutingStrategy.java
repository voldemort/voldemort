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

import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;

/**
 * A routing strategy maps puts and gets to an ordered "preference list" of
 * servers. The preference list is the order under which operations will be
 * completed in the absence of failures.
 * 
 * 
 */
public interface RoutingStrategy {

    /**
     * Get the type of RoutingStrategyType
     * 
     * @return RoutingStrategyType
     */
    public String getType();

    /**
     * Get the node preference list for the given key. The preference list is a
     * list of nodes to perform an operation on.
     * 
     * @param key The key the operation is operating on
     * @return The preference list for the given key
     */
    public List<Node> routeRequest(byte[] key);

    /**
     * Get the partition list for the given key.
     * 
     * @param key The key the operation is operating on
     * @return The partition list for the given key
     */
    public List<Integer> getPartitionList(byte[] key);

    /**
     * Obtain the master partition for a given key
     * 
     * @param key The key being operated on
     * @return The partition that owns the key
     */
    public Integer getMasterPartition(byte[] key);

    /**
     * Get the replication partitions list for the given partition.
     * 
     * @param partitionId
     * @return The List of partitionId where this partition is replicated.
     */
    public List<Integer> getReplicatingPartitionList(int partitionId);

    /**
     * Get the collection of nodes that are candidates for routing.
     * 
     * @return The collection of nodes
     */
    public Set<Node> getNodes();

    /**
     * Return the number of replicas
     * 
     * @return The number of replicas
     */
    public int getNumReplicas();

}

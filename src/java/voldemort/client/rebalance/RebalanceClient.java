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

package voldemort.client.rebalance;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.cluster.Cluster;

/**
 * The cluster rebalancing Interface.
 * 
 */
@Threadsafe
public interface RebalanceClient {

    /**
     * Steal partitions and update stealerNodeId <br>
     * partitions are stolen from cluster to balance load on all nodes equally
     * while respecting the replication strategy.
     * 
     * @param stealerNodeID : The nodeId where partitions will be copied
     * @param storeName : store to be rebalanced
     * @param currentCluster: Current Cluster configuration
     */
    public void stealPartitions(final int stealerNodeID,
                                final String storeName,
                                final Cluster currentCluster);

    /**
     * update stealerNodeId by fetching and updating given list of partitions
     * <br>
     * 
     * @param stealerNodeID : The nodeId where partitions will be copied
     * @param storeName : store to be rebalanced
     * @param currentCluster: Current Cluster configuration
     * @param targetCluster: target Cluster configuration
     */
    public void stealPartitions(final int stealerNodeID,
                                final String storeName,
                                final Cluster currentCluster,
                                final Cluster targetCluster);

    /**
     * donate partitions from donorNodeId <br>
     * partitions are dopnated from donorNodeId and updated to different nodes
     * in cluster to balance load on all nodes equally while respecting the
     * replication strategy.
     * 
     * @param donorNodeId : The nodeId from where partitions will be copied.
     * @param storeName : store to be rebalanced
     * @param currentCluster: Current Cluster configuration
     */
    public void donatePartitions(final int donorNodeId,
                                 final String storeName,
                                 final Cluster currentCluster);

    /**
     * donate all partitions listed in partitionList from donorNodeId <br>
     * 
     * @param donorNodeId : The nodeId from where partitions will be copied.
     * @param storeName : store to be rebalanced
     * @param currentCluster: Current Cluster configuration
     * @param targetCluster: target Cluster configuration
     */
    public void donatePartitions(final int donorNodeId,
                                 final String storeName,
                                 final Cluster currentCluster,
                                 final Cluster targetCluster);
}

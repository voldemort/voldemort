/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

/**
 * RebalanceUtils provide basic functionality for rebalancing. Some of these
 * functions are not utils function but are forced move here to allow more
 * granular unit testing.
 * 
 * 
 */
public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    public final static List<String> rebalancingStoreEngineBlackList = Arrays.asList(MysqlStorageConfiguration.TYPE_NAME);

    public static boolean containsNode(Cluster cluster, int nodeId) {
        try {
            cluster.getNodeById(nodeId);
            return true;
        } catch(VoldemortException e) {
            return false;
        }
    }

    /**
     * Returns a list of unique store definitions given a list of store
     * definitions, where unique is defined as having a different
     * "replication-factor" and "routing strategy"
     * 
     * @param storeDefs List of store definitions
     * @return Returns list of unique store definitions
     */
    public static List<StoreDefinition> getUniqueStoreDefinitions(List<StoreDefinition> storeDefs) {

        List<StoreDefinition> uniqueStoreDefs = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefs) {
            if(uniqueStoreDefs.isEmpty()) {
                uniqueStoreDefs.add(storeDef);
            } else {
                boolean unique = true;
                for(StoreDefinition uniqueStoreDef: uniqueStoreDefs) {
                    if(uniqueStoreDef.getReplicationFactor() == storeDef.getReplicationFactor()
                       && uniqueStoreDef.getRoutingStrategyType()
                                        .compareTo(storeDef.getRoutingStrategyType()) == 0) {
                        unique = false;
                        // Further check for the zone routing case
                        if(uniqueStoreDef.getRoutingStrategyType()
                                         .compareTo(RoutingStrategyType.ZONE_STRATEGY) == 0) {
                            boolean zonesSame = true;
                            for(int zoneId: uniqueStoreDef.getZoneReplicationFactor().keySet()) {
                                if(storeDef.getZoneReplicationFactor().get(zoneId) == null
                                   || storeDef.getZoneReplicationFactor().get(zoneId) != uniqueStoreDef.getZoneReplicationFactor()
                                                                                                       .get(zoneId)) {
                                    zonesSame = false;
                                    break;
                                }
                            }
                            if(!zonesSame) {
                                unique = true;
                            }
                        }
                    }
                }
                if(unique) {
                    uniqueStoreDefs.add(storeDef);
                }
            }
        }

        return uniqueStoreDefs;
    }

    /**
     * Checks if two lists of store definitions has the same number of stores
     * and same stores. This is not same as
     * {@link voldemort.store.StoreDefinition#equals(Object)} because we expect
     * some parameters to change ( eg : rep-factor, routing strategy ) during
     * grandfathering
     * 
     * @param storeDefList1
     * @param storeDefList2
     * @return Boolean indicating if the store definitions are same or not
     */
    public static boolean hasSameStores(List<StoreDefinition> storeDefList1,
                                        List<StoreDefinition> storeDefList2) {
        if(storeDefList1.size() != storeDefList2.size())
            return false;

        List<String> storeNames1 = RebalanceUtils.getStoreNames(storeDefList1);
        List<String> storeNames2 = RebalanceUtils.getStoreNames(storeDefList2);
        for(String storeName: storeNames1) {
            if(!storeNames2.contains(storeName)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Update the cluster with desired changes as marked in rebalanceNodeInfo
     * rebalanceNodeInfo.getFirst() is the stealerNode (destinationNode) <br>
     * rebalanceNodeInfo.getSecond() is the rebalance steal info contatining
     * donorId, partitionList<br>
     * Creates a new cluster Object with above partition list changes.<br>
     * Propagates the new cluster on all nodes
     * 
     * @param adminClient
     * @param rebalanceNodeInfo
     * @return
     */
    public static Cluster createUpdatedCluster(Cluster cluster,
                                               Node stealerNode,
                                               Node donorNode,
                                               List<Integer> partitionList) {
        List<Integer> stealerPartitionList = new ArrayList<Integer>(stealerNode.getPartitionIds());
        List<Integer> donorPartitionList = new ArrayList<Integer>(donorNode.getPartitionIds());

        for(int p: cluster.getNodeById(stealerNode.getId()).getPartitionIds()) {
            if(!stealerPartitionList.contains(p))
                stealerPartitionList.add(p);
        }

        for(int p: partitionList) {
            removePartition(donorPartitionList, p);
            if(!stealerPartitionList.contains(p))
                stealerPartitionList.add(p);
        }

        // sort both list
        Collections.sort(stealerPartitionList);
        Collections.sort(donorPartitionList);

        logger.debug("stealerNode: " + stealerNode);
        logger.debug("donorNode: " + donorNode);
        logger.debug("stealerPartitionList: " + stealerPartitionList);
        logger.debug("donorPartitionList: " + donorPartitionList);

        // update both nodes
        stealerNode = updateNode(stealerNode, stealerPartitionList);
        donorNode = updateNode(donorNode, donorPartitionList);

        Cluster updatedCluster = updateCluster(cluster, Arrays.asList(stealerNode, donorNode));
        logger.debug("currentCluster: " + cluster + " updatedCluster:" + updatedCluster);
        return updatedCluster;
    }

    private static void removePartition(List<Integer> donorPartitionList, int partition) {
        for(int i = 0; i < donorPartitionList.size(); i++) {
            if(partition == donorPartitionList.get(i)) {
                donorPartitionList.remove(i);
            }
        }
    }

    public static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
        List<Node> newNodeList = new ArrayList<Node>(updatedNodeList);
        for(Node currentNode: currentCluster.getNodes()) {
            if(!updatedNodeList.contains(currentNode))
                newNodeList.add(currentNode);
        }

        Collections.sort(newNodeList);
        return new Cluster(currentCluster.getName(), newNodeList);
    }

    public static Node updateNode(Node node, List<Integer> partitionsList) {
        return new Node(node.getId(),
                        node.getHost(),
                        node.getHttpPort(),
                        node.getSocketPort(),
                        node.getAdminPort(),
                        partitionsList);
    }

    public static Map<Integer, Integer> getCurrentPartitionMapping(Cluster currentCluster) {
        Map<Integer, Integer> partitionToNode = new HashMap<Integer, Integer>();

        for(Node n: currentCluster.getNodes()) {
            for(Integer partition: n.getPartitionIds()) {
                partitionToNode.put(partition, n.getId());
            }
        }

        return partitionToNode;
    }

    /**
     * Get the latest cluster from all available nodes in the cluster<br>
     * Throws exception if:<br>
     * any node in the RequiredNode list fails to respond.<br>
     * Cluster is in inconsistent state with concurrent versions for cluster
     * metadata on any two nodes.<br>
     * 
     * @param stealerId
     * @param donorId
     * @return
     */
    public static Versioned<Cluster> getLatestCluster(List<Integer> requiredNodes,
                                                      AdminClient adminClient) {
        Versioned<Cluster> latestCluster = new Versioned<Cluster>(adminClient.getAdminClientCluster());
        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();

        clusterList.add(latestCluster);
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            try {
                Versioned<Cluster> versionedCluster = adminClient.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();
                if(null != newClock && !clusterList.contains(versionedCluster)) {
                    // check no two clocks are concurrent.
                    checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occured occured = newClock.compare(latestCluster.getVersion());
                    if(Occured.AFTER.equals(occured))
                        latestCluster = versionedCluster;
                }
            } catch(Exception e) {
                if(null != requiredNodes && requiredNodes.contains(node.getId()))
                    throw new VoldemortException("Failed to get Cluster version from node:" + node,
                                                 e);
                else
                    logger.info("Failed to get Cluster version from node:" + node, e);
            }
        }

        return latestCluster;
    }

    public static void checkNotConcurrent(ArrayList<Versioned<Cluster>> clockList,
                                          VectorClock newClock) {
        for(Versioned<Cluster> versionedCluster: clockList) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            if(Occured.CONCURRENTLY.equals(clock.compare(newClock)))
                throw new VoldemortException("Cluster is in inconsistent state got conflicting clocks "
                                             + clock + " and " + newClock);

        }
    }

    /**
     * Attempt to propagate cluster definition to all nodes in the cluster.
     * 
     * @throws VoldemortException If we can't propagate to a list of require
     *         nodes.
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use
     * @param cluster Cluster definition we wish to propagate
     * @param clock Vector clock to attach to the cluster definition
     * @param requireNodeIds If we can't propagate to these node ids, roll back
     *        and throw an exception
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster cluster,
                                        VectorClock clock,
                                        List<Integer> requireNodeIds) {
        List<Integer> allNodeIds = new ArrayList<Integer>();
        for(Node node: cluster.getNodes()) {
            allNodeIds.add(node.getId());
        }
        propagateCluster(adminClient, cluster, clock, allNodeIds, requireNodeIds);
    }

    /**
     * Attempt to propagate a cluster definition to specified nodes.
     * 
     * @throws VoldemortException If we can't propagate to a list of require
     *         nodes.
     * @param adminClient {@link voldemort.client.protocol.admin.AdminClient}
     *        instance to use.
     * @param cluster Cluster definition we wish to propagate
     * @param clock Vector clock to attach to the cluster definition
     * @param attemptNodeIds Attempt to propagate to these node ids
     * @param requiredNodeIds If we can't propagate can't propagate to these
     *        node ids, roll back and throw an exception
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster cluster,
                                        VectorClock clock,
                                        List<Integer> attemptNodeIds,
                                        List<Integer> requiredNodeIds) {
        List<Integer> failures = new ArrayList<Integer>();

        // copy everywhere else first
        for(int nodeId: attemptNodeIds) {
            if(!requiredNodeIds.contains(nodeId)) {
                try {
                    adminClient.updateRemoteCluster(nodeId, cluster, clock);
                } catch(VoldemortException e) {
                    // ignore these
                    logger.debug("Failed to copy new cluster.xml(" + cluster
                                 + ") on non-required node:" + nodeId, e);
                }
            }
        }

        // attempt copying on all required nodes.
        for(int nodeId: requiredNodeIds) {
            Node node = cluster.getNodeById(nodeId);
            try {
                logger.debug("Updating remote node:" + nodeId + " with cluster:" + cluster);
                adminClient.updateRemoteCluster(node.getId(), cluster, clock);
            } catch(Exception e) {
                failures.add(node.getId());
                logger.debug(e);
            }
        }

        if(failures.size() > 0) {
            throw new VoldemortException("Failed to copy updated cluster.xml:" + cluster
                                         + " on required nodes:" + failures);
        }
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig,
                                                    Cluster cluster,
                                                    int numThreads,
                                                    int numConnPerNode) {
        AdminClientConfig config = new AdminClientConfig().setMaxConnectionsPerNode(numConnPerNode)
                                                          .setMaxThreads(numThreads)
                                                          .setAdminConnectionTimeoutSec(voldemortConfig.getAdminConnectionTimeout())
                                                          .setAdminSocketTimeoutSec(voldemortConfig.getAdminSocketTimeout())
                                                          .setAdminSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new AdminClient(cluster, config);
    }

    public static List<StoreDefinition> getStoreNameList(Cluster cluster, AdminClient adminClient) {
        for(Node node: cluster.getNodes()) {
            try {
                List<StoreDefinition> storeDefList = adminClient.getRemoteStoreDefList(node.getId())
                                                                .getValue();
                return getWritableStores(storeDefList);
            } catch(VoldemortException e) {
                logger.warn(e);
            }
        }

        throw new VoldemortException("Unable to get StoreDefList from any node for cluster:"
                                     + cluster);
    }

    public static List<StoreDefinition> getWritableStores(List<StoreDefinition> storeDefList) {
        List<StoreDefinition> storeNameList = new ArrayList<StoreDefinition>(storeDefList.size());

        for(StoreDefinition def: storeDefList) {
            if(!def.isView() && !rebalancingStoreEngineBlackList.contains(def.getType())) {
                storeNameList.add(def);
            } else {
                logger.debug("ignoring store " + def.getName() + " for rebalancing");
            }
        }
        return storeNameList;
    }

    public static StoreDefinition getMaxReplicationStore(List<StoreDefinition> storeDefList) {
        int maxReplication = 0;
        StoreDefinition maxStore = null;
        for(StoreDefinition def: storeDefList) {
            if(maxReplication < def.getReplicationFactor()) {
                maxReplication = def.getReplicationFactor();
                maxStore = def;
            }
        }

        return maxStore;
    }

    public static List<String> getStoreNames(List<StoreDefinition> storeDefList) {
        List<String> storeList = new ArrayList<String>(storeDefList.size());
        for(StoreDefinition def: storeDefList) {
            storeList.add(def.getName());
        }
        return storeList;
    }

    /**
     * Returns the store definition from the list with the store name specified,
     * else returns null
     * 
     * @param storeDefs The list of store definitions
     * @param storeName The name of the store which is required
     * @return The store definition else null
     */
    public static StoreDefinition getStore(List<StoreDefinition> storeDefs, String storeName) {

        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().compareTo(storeName) == 0) {
                return storeDef;
            }
        }
        return null;
    }

    public static void executorShutDown(ExecutorService executorService, int timeOutSec) {
        try {
            executorService.shutdown();
            executorService.awaitTermination(timeOutSec, TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.warn("Error while stoping executor service.", e);
        }
    }

    public static ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }
}

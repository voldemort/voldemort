package voldemort.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.client.rebalance.RebalanceStealInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.versioning.VectorClock;

/**
 * RebalanceUtils provide basic functionality for rebalancing. Some of these
 * functions are not utils function but are forced move here to allow more
 * granular unit testing.
 * 
 * @author bbansal
 * 
 */
public class RebalanceUtils {

    private static Logger logger = Logger.getLogger(RebalanceUtils.class);

    /**
     * Compares the currentCluster configuration with the desired
     * targetConfiguration and returns a map of Target node-id to map of source
     * node-ids and partitions desired to be stolen/fetched.<br>
     * <b> returned Queue is threadsafe </b>
     * 
     * @param currentCluster
     * @param targetCluster
     * @return Queue (pair(StealerNodeId, RebalanceStealInfo))
     */
    public static Queue<Pair<Integer, List<RebalanceStealInfo>>> getRebalanceTaskQueue(Cluster currentCluster,
                                                                                       Cluster targetCluster,
                                                                                       List<String> storeList) {
        Queue<Pair<Integer, List<RebalanceStealInfo>>> rebalanceTaskQueue = new ConcurrentLinkedQueue<Pair<Integer, List<RebalanceStealInfo>>>();

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        for(Node node: targetCluster.getNodes()) {
            List<RebalanceStealInfo> rebalanceNodeList = getRebalanceNodeTask(currentCluster,
                                                                              targetCluster,
                                                                              storeList,
                                                                              node.getId());
            if(rebalanceNodeList.size() > 0) {
                rebalanceTaskQueue.offer(new Pair<Integer, List<RebalanceStealInfo>>(node.getId(),
                                                                                     rebalanceNodeList));
            }

        }

        return rebalanceTaskQueue;
    }

    private static List<RebalanceStealInfo> getRebalanceNodeTask(Cluster currentCluster,
                                                                 Cluster targetCluster,
                                                                 List<String> storeList,
                                                                 int stealNodeId) {
        Map<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        Map<Integer, Integer> currentPartitionsToNodeMap = getCurrentPartitionMapping(currentCluster);
        List<Integer> targetList = targetCluster.getNodeById(stealNodeId).getPartitionIds();
        List<Integer> currentList;

        if(containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();
        else
            currentList = new ArrayList<Integer>();

        for(int p: targetList) {
            if(!currentList.contains(p)) {
                // new extra partition
                int currentMasterNode = currentPartitionsToNodeMap.get(p);
                // create array if needed
                if(!stealPartitionsMap.containsKey(currentMasterNode)) {
                    stealPartitionsMap.put(currentMasterNode, new ArrayList<Integer>());
                }

                // add partition to list.
                stealPartitionsMap.get(currentMasterNode).add(p);
            }
        }

        List<RebalanceStealInfo> stealInfoList = new ArrayList<RebalanceStealInfo>();
        for(Entry<Integer, List<Integer>> stealEntry: stealPartitionsMap.entrySet()) {
            stealInfoList.add(new RebalanceStealInfo(stealNodeId,
                                                     stealEntry.getKey(),
                                                     stealEntry.getValue(),
                                                     storeList,
                                                     0));
        }

        return stealInfoList;
    }

    public static boolean containsNode(Cluster cluster, int nodeId) {
        try {
            cluster.getNodeById(nodeId);
            return true;
        } catch(VoldemortException e) {
            return false;
        }
    }

    private static Map<Integer, Integer> getCurrentPartitionMapping(Cluster currentCluster) {
        Map<Integer, Integer> partitionToNode = new HashMap<Integer, Integer>();

        for(Node n: currentCluster.getNodes()) {
            for(Integer partition: n.getPartitionIds()) {
                partitionToNode.put(partition, n.getId());
            }
        }

        return partitionToNode;
    }

    /**
     * We should only allow one cluster rebalancing at one time. We need to
     * implement kind of a global lock for that.
     * 
     * TODO: Currently the user is responsible for not starting more than on
     * cluster rebalancing.
     * 
     * @return
     */
    public static boolean getClusterRebalancingToken() {
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

        for(int p: partitionList) {
            donorPartitionList.remove(p);
            if(!stealerPartitionList.contains(p))
                stealerPartitionList.add(p);
        }

        // sort both list
        Collections.sort(stealerPartitionList);
        Collections.sort(donorPartitionList);

        // update both nodes
        stealerNode = updateNode(stealerNode, stealerPartitionList);
        donorNode = updateNode(donorNode, donorPartitionList);

        return updateCluster(cluster, Arrays.asList(stealerNode, donorNode));
    }

    public static Cluster updateCluster(Cluster currentCluster, List<Node> updatedNodeList) {
        List<Node> newNodeList = new ArrayList<Node>(updatedNodeList);
        for(Node currentNode: currentCluster.getNodes()) {
            if(!updatedNodeList.contains(currentNode))
                newNodeList.add(currentNode);
        }
        return new Cluster(currentCluster.getName(), newNodeList);
    }

    public static Node updateNode(Node node, List<Integer> partitionsList) {
        return new Node(node.getId(),
                        node.getHost(),
                        node.getHttpPort(),
                        node.getSocketPort(),
                        node.getAdminPort(),
                        partitionsList,
                        node.getStatus());
    }

    /**
     * propagate the cluster configuration to all nodes.<br>
     * throws an exception if failed to propagate on any of the required nodes.
     * 
     * @param adminClient
     * @param masterNodeId
     * @param cluster
     */
    public static void propagateCluster(AdminClient adminClient,
                                        Cluster cluster,
                                        VectorClock clock,
                                        List<Integer> requiredNodeIds) {
        // attempt requiredNode first.
        for(int nodeId: requiredNodeIds) {
            Node node = cluster.getNodeById(nodeId);
            try {
                logger.debug("Updating remote node:" + nodeId + " with cluster:" + cluster);
                adminClient.updateRemoteCluster(node.getId(), cluster, clock);
            } catch(Exception e) {
                throw new VoldemortException("Failed to copy new cluster.xml(" + cluster
                                             + ") on required node:" + node, e);
            }
        }

        // copy everywhere else
        for(Node node: cluster.getNodes()) {
            if(!requiredNodeIds.contains(node.getId())) {
                try {
                    adminClient.updateRemoteCluster(node.getId(), cluster, clock);
                } catch(VoldemortException e) {
                    logger.warn("Failed to copy new cluster.xml(" + cluster
                                + ") on non-required node:" + node, e);
                }
            }
        }
    }

    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig, Cluster cluster) {
        ClientConfig config = new ClientConfig();
        config.setMaxConnectionsPerNode(1);
        config.setMaxThreads(1);
        config.setConnectionTimeout(voldemortConfig.getAdminConnectionTimeout(),
                                    TimeUnit.MILLISECONDS);
        config.setSocketTimeout(voldemortConfig.getAdminSocketTimeout(), TimeUnit.MILLISECONDS);
        config.setSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new ProtoBuffAdminClientRequestFormat(cluster, config);
    }
}
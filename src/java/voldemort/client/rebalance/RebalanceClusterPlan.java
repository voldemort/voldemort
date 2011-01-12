package voldemort.client.rebalance;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * Compares the currentCluster configuration with the desired
 * targetConfiguration and returns a map of Target node-id to map of source
 * node-ids and partitions desired to be stolen/fetched.<br>
 * <b> returned Queue is threadsafe </b>
 * 
 * @param currentCluster The current cluster definition
 * @param targetCluster The target cluster definition
 * @param storeDefList The list of store definitions to rebalance
 * @param currentROStoreVersions A mapping of nodeId to map of store names to
 *        version ids
 * @param deleteDonorPartition Delete the RW partition on the donor side after
 *        rebalance
 * 
 */
public class RebalanceClusterPlan {

    private Queue<RebalanceNodePlan> rebalanceTaskQueue;
    private List<StoreDefinition> oldStoreDefList;
    private List<StoreDefinition> newStoreDefList;

    public RebalanceClusterPlan(Cluster currentCluster,
                                Cluster targetCluster,
                                List<StoreDefinition> oldStoreDefList,
                                List<StoreDefinition> newStoreDefList,
                                boolean deleteDonorPartition,
                                Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        initialize(currentCluster,
                   targetCluster,
                   oldStoreDefList,
                   newStoreDefList,
                   deleteDonorPartition,
                   currentROStoreVersionsDirs);

    }

    public RebalanceClusterPlan(Cluster currentCluster,
                                Cluster targetCluster,
                                List<StoreDefinition> storeDefList,
                                boolean deleteDonorPartition,
                                Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        initialize(currentCluster,
                   targetCluster,
                   storeDefList,
                   storeDefList,
                   deleteDonorPartition,
                   currentROStoreVersionsDirs);
    }

    private void initialize(Cluster currentCluster,
                            Cluster targetCluster,
                            List<StoreDefinition> oldStoreDefList,
                            List<StoreDefinition> newStoreDefList,
                            boolean deleteDonorPartition,
                            Map<Integer, Map<String, String>> currentROStoreVersionsDirs) {
        this.rebalanceTaskQueue = new ConcurrentLinkedQueue<RebalanceNodePlan>();
        this.oldStoreDefList = oldStoreDefList;
        this.newStoreDefList = newStoreDefList;

        if(currentCluster.getNumberOfPartitions() != targetCluster.getNumberOfPartitions())
            throw new VoldemortException("Total number of partitions should not change !!");

        if(!RebalanceUtils.hasSameStores(oldStoreDefList, newStoreDefList))
            throw new VoldemortException("Either the number of stores has changed or some stores are missing");

        for(Node node: targetCluster.getNodes()) {
            List<RebalancePartitionsInfo> rebalanceNodeList = getRebalanceNodeTask(currentCluster,
                                                                                   targetCluster,
                                                                                   RebalanceUtils.getStoreNames(oldStoreDefList),
                                                                                   node.getId(),
                                                                                   deleteDonorPartition);
            if(rebalanceNodeList.size() > 0) {
                if(currentROStoreVersionsDirs != null && currentROStoreVersionsDirs.size() > 0) {
                    for(RebalancePartitionsInfo partitionsInfo: rebalanceNodeList) {
                        partitionsInfo.setStealerNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getStealerId()));
                        partitionsInfo.setDonorNodeROStoreToDir(currentROStoreVersionsDirs.get(partitionsInfo.getDonorId()));
                    }
                }
                rebalanceTaskQueue.offer(new RebalanceNodePlan(node.getId(), rebalanceNodeList));
            }
        }
    }

    public Queue<RebalanceNodePlan> getRebalancingTaskQueue() {
        return rebalanceTaskQueue;
    }

    /**
     * Returns a map of stealer node to their corresponding node plan
     * 
     * @return Map of stealer node to plan
     */
    public HashMap<Integer, RebalanceNodePlan> getRebalancingTaskQueuePerNode() {
        HashMap<Integer, RebalanceNodePlan> rebalanceMap = Maps.newHashMap();
        Iterator<RebalanceNodePlan> iter = rebalanceTaskQueue.iterator();
        while(iter.hasNext()) {
            RebalanceNodePlan plan = iter.next();
            rebalanceMap.put(plan.getStealerNode(), plan);
        }
        return rebalanceMap;
    }

    /**
     * For a particular stealer node retrieves a list of plans corresponding to
     * each donor node.
     * 
     * @param currentCluster The cluster definition of the current cluster
     * @param targetCluster The cluster definition of the target cluster
     * @param storeList The list of stores
     * @param stealNodeId The node id of the stealer node
     * @param deleteDonorPartition Delete the donor partitions after rebalance
     * @return List of plans per donor node
     */
    private List<RebalancePartitionsInfo> getRebalanceNodeTask(Cluster currentCluster,
                                                               Cluster targetCluster,
                                                               List<String> storeList,
                                                               int stealNodeId,
                                                               boolean deleteDonorPartition) {
        Map<Integer, Integer> currentPartitionsToNodeMap = RebalanceUtils.getCurrentPartitionMapping(currentCluster);
        List<Integer> stealList = getStealList(currentCluster, targetCluster, stealNodeId);

        Map<Integer, List<Integer>> masterPartitionsMap = getStealMasterPartitions(stealList,
                                                                                   currentPartitionsToNodeMap);

        // copies partitions needed to satisfy new replication mapping.
        // these partitions should be copied but not deleted from original node.
        Map<Integer, List<Integer>> replicationPartitionsMap = getReplicationChanges(currentCluster,
                                                                                     targetCluster,
                                                                                     stealNodeId,
                                                                                     currentPartitionsToNodeMap);

        List<RebalancePartitionsInfo> stealInfoList = new ArrayList<RebalancePartitionsInfo>();
        for(Node donorNode: currentCluster.getNodes()) {
            Set<Integer> stealPartitions = new HashSet<Integer>();
            Set<Integer> deletePartitions = new HashSet<Integer>();
            // create Set for steal master partitions
            Set<Integer> stealMasterPartitions = new HashSet<Integer>();

            if(masterPartitionsMap.containsKey(donorNode.getId())) {
                stealPartitions.addAll(masterPartitionsMap.get(donorNode.getId()));
                // add one steal master partition
                stealMasterPartitions.addAll(masterPartitionsMap.get(donorNode.getId()));
                if(deleteDonorPartition)
                    deletePartitions.addAll(masterPartitionsMap.get(donorNode.getId()));
            }

            if(replicationPartitionsMap.containsKey(donorNode.getId())) {
                stealPartitions.addAll(replicationPartitionsMap.get(donorNode.getId()));
            }

            if(stealPartitions.size() > 0) {
                stealInfoList.add(new RebalancePartitionsInfo(stealNodeId,
                                                              donorNode.getId(),
                                                              new ArrayList<Integer>(stealPartitions),
                                                              new ArrayList<Integer>(deletePartitions),
                                                              new ArrayList<Integer>(stealMasterPartitions),
                                                              storeList,
                                                              new HashMap<String, String>(),
                                                              new HashMap<String, String>(),
                                                              0));
            }
        }

        return stealInfoList;
    }

    /**
     * For a particular stealer node find all the partitions it will steal
     * 
     * @param currentCluster The cluster definition of the existing cluster
     * @param targetCluster The target cluster definition
     * @param stealNodeId The id of the stealer node
     * @return Returns a list of partitions which this stealer node will get
     */
    private List<Integer> getStealList(Cluster currentCluster,
                                       Cluster targetCluster,
                                       int stealNodeId) {
        List<Integer> targetList = new ArrayList<Integer>(targetCluster.getNodeById(stealNodeId)
                                                                       .getPartitionIds());

        List<Integer> currentList = new ArrayList<Integer>();
        if(RebalanceUtils.containsNode(currentCluster, stealNodeId))
            currentList = currentCluster.getNodeById(stealNodeId).getPartitionIds();

        // remove all current partitions from targetList
        targetList.removeAll(currentList);

        return targetList;
    }

    /**
     * For a particular stealer node id returns a mapping of donor node ids to
     * their respective partition ids which we need to steal due to the change
     * of replication
     * 
     * @param currentCluster Current cluster definition
     * @param targetCluster Cluster definition of the target
     * @param stealNodeId The node id of the stealer node
     * @param currentPartitionsToNodeMap The mapping of current partitions to
     *        their nodes
     * @return Map of donor node ids to their partitions they'll donate
     */
    private Map<Integer, List<Integer>> getReplicationChanges(Cluster currentCluster,
                                                              Cluster targetCluster,
                                                              int stealNodeId,
                                                              Map<Integer, Integer> currentPartitionsToNodeMap) {
        Map<Integer, List<Integer>> replicationMapping = new HashMap<Integer, List<Integer>>();
        List<Integer> targetList = targetCluster.getNodeById(stealNodeId).getPartitionIds();

        // get changing replication mapping
        RebalanceClusterTool clusterTool = new RebalanceClusterTool(currentCluster,
                                                                    RebalanceUtils.getMaxReplicationStore(this.oldStoreDefList));

        /**
         * Case 1: if newStoreDef = oldStoreDef, gives you replication mapping
         * changes only due to cluster geometry change
         * 
         * Case 2: if newStoreDef != oldStoreDef, also takes into account change
         * in (a) routing strategy [ Assumption is that all stores change their
         * routing strategy at once ] (b) increase in replication factor
         * 
         */
        Multimap<Integer, Integer> replicationChanges = clusterTool.getRemappedReplicas(targetCluster,
                                                                                        RebalanceUtils.getMaxReplicationStore(this.newStoreDefList));

        for(final Entry<Integer, Integer> entry: replicationChanges.entries()) {
            int newReplicationPartition = entry.getValue();
            if(targetList.contains(newReplicationPartition)) {
                // stealerNode need to replicate some new partition now.
                int donorNode = currentPartitionsToNodeMap.get(entry.getKey());
                if(donorNode != stealNodeId)
                    createAndAdd(replicationMapping, donorNode, entry.getKey());
            }
        }

        return replicationMapping;
    }

    /**
     * Converts a list of partitions ids which a stealer is going to receive to
     * a map of donor node ids to the corresponding partitions
     * 
     * @param stealList The partitions ids going to be stolen
     * @param currentPartitionsToNodeMap Mapping of current partitions to their
     *        respective nodes ids
     * @return Returns a mapping of donor node ids to the partitions being
     *         stolen
     */
    private Map<Integer, List<Integer>> getStealMasterPartitions(List<Integer> stealList,
                                                                 Map<Integer, Integer> currentPartitionsToNodeMap) {
        HashMap<Integer, List<Integer>> stealPartitionsMap = new HashMap<Integer, List<Integer>>();
        for(int p: stealList) {
            int donorNode = currentPartitionsToNodeMap.get(p);
            createAndAdd(stealPartitionsMap, donorNode, p);
        }

        return stealPartitionsMap;
    }

    private void createAndAdd(Map<Integer, List<Integer>> map, int key, int value) {
        // create array if needed
        if(!map.containsKey(key)) {
            map.put(key, new ArrayList<Integer>());
        }

        // add partition to list.
        map.get(key).add(value);
    }

    @Override
    public String toString() {

        if(rebalanceTaskQueue.isEmpty()) {
            return "Cluster is already balanced, No rebalancing needed";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("Cluster Rebalancing Plan:\n");
        for(RebalanceNodePlan nodePlan: rebalanceTaskQueue) {
            builder.append("StealerNode:" + nodePlan.getStealerNode() + "\n");
            for(RebalancePartitionsInfo stealInfo: nodePlan.getRebalanceTaskList()) {
                builder.append("\t" + stealInfo + "\n");
            }
        }

        return builder.toString();
    }

    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("cluster-xml", "[REQUIRED] old cluster xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("stores-xml", "[REQUIRED] stores xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("target-stores-xml", "new stores xml file location")
              .withRequiredArg()
              .describedAs("path");
        parser.accepts("target-cluster-xml", "[REQUIRED] new cluster xml file location")
              .withRequiredArg()
              .describedAs("path");

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "cluster-xml",
                                               "stores-xml",
                                               "target-cluster-xml");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String newClusterXml = (String) options.valueOf("target-cluster-xml");
        String oldClusterXml = (String) options.valueOf("cluster-xml");
        String oldStoresXml = (String) options.valueOf("stores-xml");
        String newStoresXml = oldStoresXml;
        if(options.has("target-stores-xml")) {
            newStoresXml = (String) options.valueOf("target-stores-xml");
        }

        if(!Utils.isReadableFile(newClusterXml) || !Utils.isReadableFile(oldClusterXml)
           || !Utils.isReadableFile(oldStoresXml) || !Utils.isReadableFile(newStoresXml)) {
            System.err.println("Could not read metadata files from path provided");
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        ClusterMapper clusterMapper = new ClusterMapper();
        StoreDefinitionsMapper storeDefMapper = new StoreDefinitionsMapper();

        RebalanceClusterPlan plan = new RebalanceClusterPlan(clusterMapper.readCluster(new File(oldClusterXml)),
                                                             clusterMapper.readCluster(new File(newClusterXml)),
                                                             storeDefMapper.readStoreList(new File(oldStoresXml)),
                                                             storeDefMapper.readStoreList(new File(newStoresXml)),
                                                             false,
                                                             null);
        System.out.println(plan);
    }
}

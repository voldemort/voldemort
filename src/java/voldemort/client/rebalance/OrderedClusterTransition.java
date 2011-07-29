package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.KeyDistributionGenerator;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

/**
 * Ordered representation of a cluster transition that guarantees that primary
 * partition movements will take place before replicas.
 * 
 * Why? So that redirections for primary partitions can get over faster
 */
public class OrderedClusterTransition {

    private static final AtomicInteger idGen = new AtomicInteger(0);
    private final Cluster currentCluster;
    private final Cluster targetCluster;
    private final RebalanceClusterPlan rebalanceClusterPlan;
    private final List<RebalancePartitionsInfo> orderedRebalancePartitionsInfoList;
    private final List<StoreDefinition> storeDefs;
    private String printedContent;
    private final int id;

    public OrderedClusterTransition(final Cluster currentCluster,
                                    final Cluster targetCluster,
                                    List<StoreDefinition> storeDefs,
                                    final RebalanceClusterPlan rebalanceClusterPlan) {
        this.id = idGen.incrementAndGet();
        this.currentCluster = currentCluster;
        this.targetCluster = targetCluster;
        this.storeDefs = storeDefs;
        this.rebalanceClusterPlan = rebalanceClusterPlan;
        this.orderedRebalancePartitionsInfoList = orderedClusterPlan(rebalanceClusterPlan);
    }

    public List<StoreDefinition> getStoreDefs() {
        return this.storeDefs;
    }

    public int getId() {
        return id;
    }

    public Cluster getTargetCluster() {
        return targetCluster;
    }

    public Cluster getCurrentCluster() {
        return currentCluster;
    }

    public List<RebalancePartitionsInfo> getOrderedRebalancePartitionsInfoList() {
        return orderedRebalancePartitionsInfoList;
    }

    @Override
    public String toString() {
        if(printedContent == null) {
            StringBuilder sb = new StringBuilder();
            List<ByteArray> keys = KeyDistributionGenerator.generateKeys(KeyDistributionGenerator.DEFAULT_NUM_KEYS);
            sb.append("- Rebalance Task Id : ").append(getId()).append(Utils.NEWLINE);
            sb.append("- Current cluster : ")
              .append(KeyDistributionGenerator.printOverallDistribution(getCurrentCluster(),
                                                                        getStoreDefs(),
                                                                        keys))
              .append(Utils.NEWLINE);
            sb.append("- Target cluster : ")
              .append(KeyDistributionGenerator.printOverallDistribution(getTargetCluster(),
                                                                        getStoreDefs(),
                                                                        keys))
              .append(Utils.NEWLINE);
            sb.append("- Partition distribution : ")
              .append(Utils.NEWLINE)
              .append(getRebalanceClusterPlan().printPartitionDistribution())
              .append(Utils.NEWLINE);
            sb.append("- Ordered rebalance node plan : ")
              .append(Utils.NEWLINE)
              .append(printRebalanceNodePlan(getOrderedRebalancePartitionsInfoList()));
            printedContent = sb.toString();
        }
        return printedContent;
    }

    private String printRebalanceNodePlan(List<RebalancePartitionsInfo> rebalancePartitionInfoList) {
        StringBuilder builder = new StringBuilder();
        for(RebalancePartitionsInfo partitionInfo: rebalancePartitionInfoList) {
            builder.append(partitionInfo).append(Utils.NEWLINE);
        }
        return builder.toString();
    }

    private RebalanceClusterPlan getRebalanceClusterPlan() {
        return rebalanceClusterPlan;
    }

    /**
     * Given a {@link RebalanceClusterPlan}, extracts the queue of
     * {@link RebalanceNodePlan} and finally orders it
     * 
     * @param rebalanceClusterPlan Rebalance cluster plan
     * @return Returns a list of ordered rebalance node plan
     */
    private List<RebalancePartitionsInfo> orderedClusterPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
        Queue<RebalanceNodePlan> rebalancingTaskQueue = rebalanceClusterPlan.getRebalancingTaskQueue();

        List<RebalancePartitionsInfo> clusterRebalancePartitionsInfos = Lists.newArrayList();
        for(RebalanceNodePlan rebalanceNodePlan: rebalancingTaskQueue) {
            clusterRebalancePartitionsInfos.addAll(rebalanceNodePlan.getRebalanceTaskList());
        }

        return orderedPartitionInfos(clusterRebalancePartitionsInfos);
    }

    /**
     * Ordering the list of {@link RebalancePartitionsInfo} such that all
     * primary partition moves come first
     * 
     * @param clusterRebalancePartitionsInfos List of partition info
     * @return Returns a list of ordered {@link RebalancePartitionsInfo}
     */
    private List<RebalancePartitionsInfo> orderedPartitionInfos(List<RebalancePartitionsInfo> clusterRebalancePartitionsInfo) {
        List<RebalancePartitionsInfo> listPrimaries = new ArrayList<RebalancePartitionsInfo>();
        List<RebalancePartitionsInfo> listReplicas = new ArrayList<RebalancePartitionsInfo>();

        for(RebalancePartitionsInfo partitionInfo: clusterRebalancePartitionsInfo) {
            List<Integer> stealMasterPartitions = partitionInfo.getStealMasterPartitions();
            if(stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                listPrimaries.add(partitionInfo);
            } else {
                listReplicas.add(partitionInfo);
            }
        }

        // Add all the plans which list the replicas at the end
        listPrimaries.addAll(listReplicas);

        return listPrimaries;
    }
}
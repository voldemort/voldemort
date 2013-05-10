package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import voldemort.utils.Utils;

import com.google.common.collect.Lists;

// TODO: (refactor) Ordering of rebalancing tasks ought to be solely the
// province of RebalancePlan. Remove or deprecate this class whenever
// RebalancePlan has wholly replaced current planning code.
/**
 * Ordered representation of a cluster transition that guarantees that primary
 * partition movements will take place before replicas.
 * 
 * Why? So that redirections for primary partitions can get over faster
 */
public class OrderedClusterTransition {

    private final List<RebalancePartitionsInfo> orderedRebalancePartitionsInfoList;
    private String printedContent;

    @Deprecated
    public OrderedClusterTransition(final RebalanceTypedBatchPlan rebalanceClusterPlan) {
        this.orderedRebalancePartitionsInfoList = orderedClusterPlan(rebalanceClusterPlan);
    }

    public List<RebalancePartitionsInfo> getOrderedRebalancePartitionsInfoList() {
        return orderedRebalancePartitionsInfoList;
    }

    @Override
    public String toString() {
        if(printedContent == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("- OrderedClusterTransition ").append(Utils.NEWLINE);
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

    /**
     * Given a {@link RebalanceClusterPlan}, extracts the queue of
     * {@link RebalanceNodePlan} and finally orders it
     * 
     * @param rebalanceClusterPlan Rebalance cluster plan
     * @return Returns a list of ordered rebalance node plan
     */
    private List<RebalancePartitionsInfo> orderedClusterPlan(final RebalanceTypedBatchPlan rebalanceClusterPlan) {
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
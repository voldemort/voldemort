package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.Utils;

/**
 * Ordered representation of a cluster transition that guarantees that primary
 * partition movements will take place before replicas
 */
public class OrderedClusterTransition {

    private static final AtomicInteger idGen = new AtomicInteger(0);
    private final Cluster currentCluster;
    private final Cluster targetCluster;
    private final RebalanceClusterPlan rebalanceClusterPlan;
    private final List<RebalanceNodePlan> orderedRebalanceNodePlanList;
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
        this.orderedRebalanceNodePlanList = orderedClusterPlan(rebalanceClusterPlan);
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

    public List<RebalanceNodePlan> getOrderedRebalanceNodePlanList() {
        return orderedRebalanceNodePlanList;
    }

    @Override
    public String toString() {
        if(printedContent == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("- Rebalance Task Id : ").append(getId()).append(Utils.NEWLINE);
            sb.append("- Current cluster : ").append(getCurrentCluster()).append(Utils.NEWLINE);
            sb.append("- Target cluster : ").append(getTargetCluster()).append(Utils.NEWLINE);
            sb.append("- Partition distribution : ")
              .append(Utils.NEWLINE)
              .append(getRebalanceClusterPlan().printPartitionDistribution())
              .append(Utils.NEWLINE);
            sb.append("- Ordered rebalance node plan : ")
              .append(Utils.NEWLINE)
              .append(printRebalanceNodePlan(getOrderedRebalanceNodePlanList()));
            printedContent = sb.toString();
        }
        return printedContent;
    }

    private String printRebalanceNodePlan(List<RebalanceNodePlan> rebalanceNodePlanList) {
        StringBuilder builder = new StringBuilder();
        for(RebalanceNodePlan plan: rebalanceNodePlanList) {
            for(RebalancePartitionsInfo partitionInfo: plan.getRebalanceTaskList()) {
                builder.append(partitionInfo).append(Utils.NEWLINE);
            }
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
    private List<RebalanceNodePlan> orderedClusterPlan(final RebalanceClusterPlan rebalanceClusterPlan) {
        Queue<RebalanceNodePlan> rebalancingTaskQueue = rebalanceClusterPlan.getRebalancingTaskQueue();

        // Make a copy to re-order
        RebalanceNodePlan[] array = new RebalanceNodePlan[rebalancingTaskQueue.size()];
        rebalancingTaskQueue.toArray(array);

        List<RebalanceNodePlan> plans = new ArrayList<RebalanceNodePlan>();
        for(RebalanceNodePlan rebalanceNodePlan: rebalancingTaskQueue) {

            // Order the individual partition plans first
            List<RebalancePartitionsInfo> orderedRebalancePartitionsInfos = orderedPartitionInfos(rebalanceNodePlan);
            plans.add(new RebalanceNodePlan(rebalanceNodePlan.getStealerNode(),
                                            orderedRebalancePartitionsInfos));

        }

        return orderedNodePlans(plans);
    }

    /**
     * Ordering the list of {@link RebalanceNodePlan} such that all plans with
     * primary partition moves come first
     * 
     * @param rebalanceNodePlans List of Node plans
     * @return Returns a list of ordered {@link RebalanceNodePlan}
     */
    private List<RebalanceNodePlan> orderedNodePlans(List<RebalanceNodePlan> rebalanceNodePlans) {
        List<RebalanceNodePlan> first = new ArrayList<RebalanceNodePlan>();
        List<RebalanceNodePlan> second = new ArrayList<RebalanceNodePlan>();

        for(RebalanceNodePlan plan: rebalanceNodePlans) {
            boolean found = false;
            for(RebalancePartitionsInfo partitionInfo: plan.getRebalanceTaskList()) {
                List<Integer> stealMasterPartitions = partitionInfo.getStealMasterPartitions();
                if(stealMasterPartitions != null && !stealMasterPartitions.isEmpty()) {
                    found = true;
                    break;
                }
            }

            if(found) {
                first.add(plan);
            } else {
                second.add(plan);
            }
        }
        first.addAll(second);
        return first;
    }

    /**
     * Ordering {@link RebalancePartitionsInfo} for a single stealer such that
     * it guarantees that primary partition movements will be before an instance
     * which moves replica partitions only.
     * 
     * @param rebalanceNodePlan Node plan for a particular stealer node
     * @return List of ordered {@link RebalancePartitionsInfo}.
     */
    private List<RebalancePartitionsInfo> orderedPartitionInfos(final RebalanceNodePlan rebalanceNodePlan) {
        List<RebalancePartitionsInfo> listPrimaries = new ArrayList<RebalancePartitionsInfo>();
        List<RebalancePartitionsInfo> listReplicas = new ArrayList<RebalancePartitionsInfo>();

        List<RebalancePartitionsInfo> partitionInfos = rebalanceNodePlan.getRebalanceTaskList();

        for(RebalancePartitionsInfo partitionInfo: partitionInfos) {
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
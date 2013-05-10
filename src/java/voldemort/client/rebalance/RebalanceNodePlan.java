package voldemort.client.rebalance;

import java.util.List;

// TODO: (refactor) Rename this class to RebalanceBatchNodeTasks. OR drop it?
/**
 * This class acts as a container for the rebalancing plans for one particular
 * node ( either donor or stealer depending on flag ). Can be one of the
 * following -
 * 
 * <li>For a particular stealer node - partitions desired to be stolen from
 * various donor nodes
 * 
 * <li>For a particular donor node - partitions which need to be donated to
 * various stealer nodes
 * 
 */
public class RebalanceNodePlan {

    private final int nodeId;
    private final boolean isStealer;
    private final List<RebalancePartitionsInfo> rebalanceTaskList;

    public RebalanceNodePlan(int nodeId,
                             List<RebalancePartitionsInfo> rebalanceTaskList,
                             boolean isStealer) {
        this.nodeId = nodeId;
        this.rebalanceTaskList = rebalanceTaskList;
        this.isStealer = isStealer;
    }

    public boolean isStealer() {
        return this.isStealer;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<RebalancePartitionsInfo> getRebalanceTaskList() {
        return rebalanceTaskList;
    }
}

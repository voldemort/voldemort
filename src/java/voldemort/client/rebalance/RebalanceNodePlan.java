package voldemort.client.rebalance;

import java.util.List;

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
    private final boolean isNodeStealer;
    private final List<RebalancePartitionsInfo> rebalanceTaskList;

    public RebalanceNodePlan(int nodeId,
                             List<RebalancePartitionsInfo> rebalanceTaskList,
                             boolean isNodeStealer) {
        this.nodeId = nodeId;
        this.rebalanceTaskList = rebalanceTaskList;
        this.isNodeStealer = isNodeStealer;
    }

    public boolean isNodeStealer() {
        return this.isNodeStealer;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<RebalancePartitionsInfo> getRebalanceTaskList() {
        return rebalanceTaskList;
    }
}

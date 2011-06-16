package voldemort.client.rebalance;

import java.util.List;

/**
 * This class acts as a container for a stealer node id and its corresponding
 * list of {@link RebalancePartitionsInfo} ( which in turn indicates the list of
 * movements from a particular donor node )
 */
public class RebalanceNodePlan {

    private final int stealerNode;
    private final List<RebalancePartitionsInfo> rebalanceTaskList;

    public RebalanceNodePlan(int stealerNode, List<RebalancePartitionsInfo> rebalanceTaskList) {
        this.stealerNode = stealerNode;
        this.rebalanceTaskList = rebalanceTaskList;
    }

    public int getStealerNode() {
        return stealerNode;
    }

    public List<RebalancePartitionsInfo> getRebalanceTaskList() {
        return rebalanceTaskList;
    }
}

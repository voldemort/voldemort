package voldemort.client.rebalance;

import java.util.List;

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

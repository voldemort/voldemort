package voldemort.client.rebalance.task;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.client.rebalance.RebalancePartitionsInfo;

public abstract class RebalanceTask implements Runnable {

    protected final int taskId;
    protected Exception exception;
    protected final RebalanceClientConfig config;
    protected final AdminClient adminClient;
    protected final Semaphore donorPermit;
    protected final AtomicBoolean isComplete;
    protected final List<RebalancePartitionsInfo> stealInfos;

    protected final static int INVALID_REBALANCE_ID = -1;

    public RebalanceTask(final int taskId,
                         final List<RebalancePartitionsInfo> stealInfos,
                         final RebalanceClientConfig config,
                         final Semaphore donorPermit,
                         final AdminClient adminClient) {
        this.stealInfos = stealInfos;
        this.taskId = taskId;
        this.config = config;
        this.adminClient = adminClient;
        this.donorPermit = donorPermit;
        this.exception = null;
        this.isComplete = new AtomicBoolean(false);
    }

    public List<RebalancePartitionsInfo> getStealInfos() {
        return this.stealInfos;
    }

    public boolean isComplete() {
        return this.isComplete.get();
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception getError() {
        return exception;
    }

    @Override
    public String toString() {
        return "Rebalance task : " + getStealInfos();
    }

}

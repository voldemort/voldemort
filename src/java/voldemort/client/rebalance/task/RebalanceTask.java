package voldemort.client.rebalance.task;

import java.util.concurrent.Semaphore;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalanceClientConfig;

public abstract class RebalanceTask implements Runnable {

    protected final int taskId;
    protected Exception exception;
    protected final RebalanceClientConfig config;
    protected final AdminClient adminClient;
    protected final Semaphore donorPermit;

    protected final static int INVALID_REBALANCE_ID = -1;

    public RebalanceTask(final int taskId,
                         final RebalanceClientConfig config,
                         final Semaphore donorPermit,
                         final AdminClient adminClient) {
        this.taskId = taskId;
        this.config = config;
        this.adminClient = adminClient;
        this.donorPermit = donorPermit;
        this.exception = null;
    }

    public boolean hasException() {
        return exception != null;
    }

    public Exception getError() {
        return exception;
    }

}

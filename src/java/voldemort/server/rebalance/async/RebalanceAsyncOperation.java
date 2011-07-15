package voldemort.server.rebalance.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.rebalance.Rebalancer;
import voldemort.store.metadata.MetadataStore;

public abstract class RebalanceAsyncOperation extends AsyncOperation {

    protected final static Logger logger = Logger.getLogger(RebalanceAsyncOperation.class);

    protected final VoldemortConfig voldemortConfig;
    protected final MetadataStore metadataStore;
    protected AdminClient adminClient;
    protected final ExecutorService executors;

    protected Rebalancer rebalancer;

    protected ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    public RebalanceAsyncOperation(Rebalancer rebalancer,
                                   VoldemortConfig voldemortConfig,
                                   MetadataStore metadataStore,
                                   int requestId,
                                   String operationString) {
        super(requestId, operationString);
        this.voldemortConfig = voldemortConfig;
        this.metadataStore = metadataStore;
        this.adminClient = null;
        this.executors = createExecutors(voldemortConfig.getMaxParallelStoresRebalancing());
        this.rebalancer = rebalancer;
    }

    protected void waitForShutdown() {
        try {
            executors.shutdown();
            executors.awaitTermination(voldemortConfig.getRebalancingTimeoutSec(), TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.error("Interrupted while awaiting termination for executors.", e);
        }
    }
}

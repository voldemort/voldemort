package voldemort.cluster.failuredetector;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

public class NodeAccessorRunnable implements Runnable {

    private final FailureDetector failureDetector;

    private final Node node;

    private final CountDownLatch countDownLatch;

    private final AtomicInteger successCounter;

    private final AtomicInteger failureCounter;

    private final UnreachableStoreException exception;

    private final long pollInterval;

    private final long failureDelay;

    public NodeAccessorRunnable(FailureDetector failureDetector,
                                Node node,
                                CountDownLatch countDownLatch,
                                AtomicInteger successCounter,
                                AtomicInteger failureCounter,
                                UnreachableStoreException exception,
                                long pollInterval,
                                long failureDelay) {
        this.failureDetector = failureDetector;
        this.node = node;
        this.countDownLatch = countDownLatch;
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.exception = exception;
        this.pollInterval = pollInterval;
        this.failureDelay = failureDelay;
    }

    public void run() {
        FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();

        try {
            while(countDownLatch.getCount() > 0) {
                if(failureDetector.isAvailable(node)) {
                    if(failureDetectorConfig.getStoreResolver().getStore(node) != null) {
                        failureDetector.recordSuccess(node);

                        if(successCounter != null)
                            successCounter.incrementAndGet();
                    } else {
                        failureDetectorConfig.getTime().sleep(failureDelay);
                        failureDetector.recordException(node, exception);

                        if(failureCounter != null)
                            failureCounter.incrementAndGet();
                    }
                }

                failureDetectorConfig.getTime().sleep(pollInterval);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}

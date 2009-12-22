package voldemort;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.UnreachableStoreException;

public class FailureDetectorTestUtils {

    public static void recordException(FailureDetector failureDetector, Node node) {
        recordException(failureDetector, node, null);
    }

    public static void recordException(FailureDetector failureDetector,
                                       Node node,
                                       UnreachableStoreException e) {
        ((MutableStoreVerifier) failureDetector.getConfig().getStoreVerifier()).setErrorStore(node,
                                                                                              new UnreachableStoreException("test error"));
        failureDetector.recordException(node, e);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node) throws Exception {
        recordSuccess(failureDetector, node, true);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node, boolean shouldWait)
            throws Exception {
        ((MutableStoreVerifier) failureDetector.getConfig().getStoreVerifier()).setErrorStore(node,
                                                                                              null);
        failureDetector.recordSuccess(node);

        if(shouldWait)
            failureDetector.waitForAvailability(node);
    }

}

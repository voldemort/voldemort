package voldemort;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

public class FailureDetectorTestUtils {

    public static void recordException(FailureDetector failureDetector, Node node) {
        recordException(failureDetector, node, null);
    }

    public static void recordException(FailureDetector failureDetector,
                                       Node node,
                                       UnreachableStoreException e) {
        ((MutableStoreResolver) failureDetector.getConfig().getStoreResolver()).setReturnNullStore(node,
                                                                                                   true);
        failureDetector.recordException(node, e);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node) throws Exception {
        recordSuccess(failureDetector, node, true);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node, boolean shouldWait)
            throws Exception {
        ((MutableStoreResolver) failureDetector.getConfig().getStoreResolver()).setReturnNullStore(node,
                                                                                                   false);
        failureDetector.recordSuccess(node);

        if(shouldWait) {
            Time time = failureDetector.getConfig().getTime();

            while(!failureDetector.isAvailable(node))
                time.sleep(50);
        }
    }

}

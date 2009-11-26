package voldemort;

import java.util.HashMap;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicFailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

public class MutableFailureDetectorConfig extends BasicFailureDetectorConfig {

    private Map<Integer, Boolean> nullStores;

    public MutableFailureDetectorConfig(String implementationClassName,
                                        long nodeBannagePeriod,
                                        Map<Integer, Store<ByteArray, byte[]>> stores,
                                        Time time) {
        super(implementationClassName, nodeBannagePeriod, stores, time);
        nullStores = new HashMap<Integer, Boolean>();
    }

    @Override
    public Store<ByteArray, byte[]> getStore(Node node) {
        if(nullStores.get(node.getId()) == null || nullStores.get(node.getId()) == false)
            return super.getStore(node);
        else
            return null;
    }

    public void setReturnNullStore(Node node, boolean shouldReturnNullStore) {
        nullStores.put(node.getId(), shouldReturnNullStore);
    }

    public static FailureDetector createFailureDetector(Class<?> failureDetectorClass,
                                                        Map<Integer, Store<ByteArray, byte[]>> subStores,
                                                        Time time) {
        FailureDetectorConfig config = new MutableFailureDetectorConfig(failureDetectorClass.getName(),
                                                                        1000,
                                                                        subStores,
                                                                        time);
        return FailureDetectorUtils.create(config);
    }

    public static void recordException(FailureDetector failureDetector, Node node) {
        ((MutableFailureDetectorConfig) failureDetector.getConfig()).setReturnNullStore(node, true);
        failureDetector.recordException(node, null);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node) throws Exception {
        ((MutableFailureDetectorConfig) failureDetector.getConfig()).setReturnNullStore(node, false);
        failureDetector.recordSuccess(node);
        failureDetector.waitFor(node);
    }

}

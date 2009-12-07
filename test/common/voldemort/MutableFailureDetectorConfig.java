package voldemort;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicFailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class MutableFailureDetectorConfig extends BasicFailureDetectorConfig {

    private Map<Integer, Boolean> nullStores;

    public MutableFailureDetectorConfig(String implementationClassName,
                                        long nodeBannagePeriod,
                                        Collection<Node> nodes,
                                        Map<Integer, Store<ByteArray, byte[]>> stores,
                                        Time time) {
        super(implementationClassName, nodeBannagePeriod, nodes, stores, time);
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
                                                        Collection<Node> nodes,
                                                        Map<Integer, Store<ByteArray, byte[]>> subStores,
                                                        Time time,
                                                        long bannageMillis) {
        FailureDetectorConfig config = new MutableFailureDetectorConfig(failureDetectorClass.getName(),
                                                                        bannageMillis,
                                                                        nodes,
                                                                        subStores,
                                                                        time);
        return FailureDetectorUtils.create(config);
    }

    public static FailureDetector createFailureDetector(Class<?> failureDetectorClass,
                                                        Collection<Node> nodes,
                                                        Time time,
                                                        long bannageMillis) {
        Map<Integer, Store<ByteArray, byte[]>> subStores = new HashMap<Integer, Store<ByteArray, byte[]>>();

        for(Node node: nodes) {
            subStores.put(node.getId(), new Store<ByteArray, byte[]>() {

                public void close() throws VoldemortException {}

                public boolean delete(ByteArray key, Version version) throws VoldemortException {
                    return false;
                }

                public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
                    return null;
                }

                public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
                        throws VoldemortException {
                    return null;
                }

                public Object getCapability(StoreCapabilityType capability) {
                    return null;
                }

                public String getName() {
                    return null;
                }

                public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {}

                public List<Version> getVersions(ByteArray key) {
                    return null;
                }

            });
        }

        return createFailureDetector(failureDetectorClass, nodes, subStores, time, bannageMillis);
    }

    public static void recordException(FailureDetector failureDetector, Node node) {
        recordException(failureDetector, node, null);
    }

    public static void recordException(FailureDetector failureDetector,
                                       Node node,
                                       UnreachableStoreException e) {
        ((MutableFailureDetectorConfig) failureDetector.getConfig()).setReturnNullStore(node, true);
        failureDetector.recordException(node, e);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node) throws Exception {
        recordSuccess(failureDetector, node, true);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node, boolean shouldWait)
            throws Exception {
        ((MutableFailureDetectorConfig) failureDetector.getConfig()).setReturnNullStore(node, false);
        failureDetector.recordSuccess(node);

        if(shouldWait)
            failureDetector.waitFor(node);
    }

}

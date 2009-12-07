package voldemort;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicStoreResolver;
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

public class MutableStoreResolver extends BasicStoreResolver {

    private Map<Integer, Boolean> nullStores;

    public MutableStoreResolver(Map<Integer, Store<ByteArray, byte[]>> stores) {
        super(stores);
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
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(failureDetectorClass.getName())
                                                                                 .setNodeBannagePeriod(bannageMillis)
                                                                                 .setNodes(nodes)
                                                                                 .setStoreResolver(new MutableStoreResolver(subStores))
                                                                                 .setTime(time);
        return FailureDetectorUtils.create(failureDetectorConfig);
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

        if(shouldWait)
            failureDetector.waitFor(node);
    }

}

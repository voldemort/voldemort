package voldemort;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicStoreResolver;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class MutableStoreResolver extends BasicStoreResolver {

    private Map<Integer, Boolean> nullStores;

    private MutableStoreResolver(Map<Integer, Store<ByteArray, byte[]>> stores) {
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

    public static MutableStoreResolver createMutableStoreResolver(Map<Integer, Store<ByteArray, byte[]>> stores) {
        return new MutableStoreResolver(stores);
    }

    public static MutableStoreResolver createMutableStoreResolver(Collection<Node> nodes) {
        Map<Integer, Store<ByteArray, byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[]>>();

        for(Node node: nodes) {
            stores.put(node.getId(), new Store<ByteArray, byte[]>() {

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

        return new MutableStoreResolver(stores);
    }

}

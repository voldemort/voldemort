package voldemort.cluster.failuredetector;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * MutableStoreVerifier is used when we want to simulate a downed node during
 * testing. For a given Node we can update this StoreVerifier to cause node
 * availability discovery to fail (which we want to be able to control for our
 * tests).
 * 
 */

public class MutableStoreVerifier extends BasicStoreVerifier<ByteArray, byte[], byte[]> {

    private Map<Integer, VoldemortException> errorStores;

    private MutableStoreVerifier(Map<Integer, Store<ByteArray, byte[], byte[]>> stores) {
        super(stores, new ByteArray((byte) 1));
        errorStores = new HashMap<Integer, VoldemortException>();
    }

    @Override
    public void verifyStore(Node node) throws UnreachableStoreException, VoldemortException {
        VoldemortException e = errorStores.get(node.getId());

        if(e == null)
            super.verifyStore(node);
        else
            throw e;
    }

    public void setErrorStore(Node node, VoldemortException voldemortException) {
        errorStores.put(node.getId(), voldemortException);
    }

    public void addStore(Node node) {
        stores.put(node.getId(), createStore());
    }

    public static MutableStoreVerifier create(Map<Integer, Store<ByteArray, byte[], byte[]>> stores) {
        return new MutableStoreVerifier(stores);
    }

    public static MutableStoreVerifier create(Collection<Node> nodes) {
        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();

        for(Node node: nodes) {
            stores.put(node.getId(), createStore());
        }

        return new MutableStoreVerifier(stores);
    }

    private static Store<ByteArray, byte[], byte[]> createStore() {
        return new Store<ByteArray, byte[], byte[]>() {

            @Override
            public void close() throws VoldemortException {}

            @Override
            public boolean delete(ByteArray key, Version version) throws VoldemortException {
                return false;
            }

            @Override
            public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
                    throws VoldemortException {
                return null;
            }

            @Override
            public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                                  Map<ByteArray, byte[]> transforms)
                    throws VoldemortException {
                return null;
            }

            @Override
            public Object getCapability(StoreCapabilityType capability) {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public List<Version> getVersions(ByteArray key) {
                return null;
            }

            @Override
            public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
                    throws VoldemortException {}

            @Override
            public List<Versioned<byte[]>> get(CompositeVoldemortRequest<ByteArray, byte[]> request)
                    throws VoldemortException {
                return null;
            }

            @Override
            public Map<ByteArray, List<Versioned<byte[]>>> getAll(CompositeVoldemortRequest<ByteArray, byte[]> request)
                    throws VoldemortException {
                return null;
            }

            @Override
            public void put(CompositeVoldemortRequest<ByteArray, byte[]> request)
                    throws VoldemortException {}

            @Override
            public boolean delete(CompositeVoldemortRequest<ByteArray, byte[]> request)
                    throws VoldemortException {
                return false;
            }
        };
    }

}

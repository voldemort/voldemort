package voldemort;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.BasicStoreVerifier;
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

public class MutableStoreVerifier extends BasicStoreVerifier<ByteArray, byte[]> {

    private Map<Integer, VoldemortException> errorStores;

    private MutableStoreVerifier(Map<Integer, Store<ByteArray, byte[]>> stores) {
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

    public static MutableStoreVerifier create(Map<Integer, Store<ByteArray, byte[]>> stores) {
        return new MutableStoreVerifier(stores);
    }

    public static MutableStoreVerifier create(Collection<Node> nodes) {
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

                public List<Version> getVersions(ByteArray key) {
                    return null;
                }

                public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {}

            });
        }

        return new MutableStoreVerifier(stores);
    }

}

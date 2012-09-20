package voldemort.store.pausable;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

/**
 * The storage configuration for the PausableStorageEngine
 * 
 * 
 */
public class PausableStorageConfiguration implements StorageConfiguration {

    private static final String TYPE_NAME = "pausable";

    public PausableStorageConfiguration(@SuppressWarnings("unused") VoldemortConfig config) {}

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
        return new PausableStorageEngine<ByteArray, byte[], byte[]>(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeDef.getName()));
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for "
                                     + this.getClass().getCanonicalName());
    }
}

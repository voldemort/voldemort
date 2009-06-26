package voldemort.store.pausable;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

/**
 * The storage configuration for the PausableStorageEngine
 * 
 * @author jay
 * 
 */
public class PausableStorageConfiguration implements StorageConfiguration {

    private final String TYPE_NAME = "pausable";

    public PausableStorageConfiguration(@SuppressWarnings("unused") VoldemortConfig config) {}

    public void close() {}

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        return new PausableStorageEngine<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(name));
    }

    public String getType() {
        return TYPE_NAME;
    }

}

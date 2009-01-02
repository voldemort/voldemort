package voldemort.store.memory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import voldemort.store.KeyWrapper;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.versioning.Versioned;

/**
 * A storage engine that uses a java.util.ConcurrentHashMap to hold the entries
 * 
 * @author jay
 * 
 */
public class InMemoryStorageConfiguration implements StorageConfiguration {

    public StorageEngine<byte[], byte[]> getStore(String name) {
        return new InMemoryStorageEngine<byte[], byte[]>(name,
                                                         new ConcurrentHashMap<KeyWrapper, List<Versioned<byte[]>>>());
    }

    public StorageEngineType getType() {
        return StorageEngineType.MEMORY;
    }

    public void close() {}

}

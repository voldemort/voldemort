package voldemort.store.memory;

import java.util.List;

import voldemort.store.KeyWrapper;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.versioning.Versioned;

import com.google.common.base.ReferenceType;
import com.google.common.collect.ReferenceMap;

/**
 * Identical to the InMemoryStorageConfiguration except that it creates google
 * collections ReferenceMap with Soft references on both keys and values. This
 * behaves like a cache, discarding values when under memory pressure.
 * 
 * @author jay
 * 
 */
public class CacheStorageConfiguration implements StorageConfiguration {

    public void close() {}

    public StorageEngine<byte[], byte[]> getStore(String name) {
        ReferenceMap<KeyWrapper, List<Versioned<byte[]>>> backingMap = new ReferenceMap<KeyWrapper, List<Versioned<byte[]>>>(ReferenceType.STRONG,
                                                                                                                             ReferenceType.SOFT);
        return new InMemoryStorageEngine<byte[], byte[]>(name, backingMap);
    }

    public StorageEngineType getType() {
        return StorageEngineType.CACHE;
    }

}

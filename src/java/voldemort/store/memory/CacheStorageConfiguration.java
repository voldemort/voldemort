/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.memory;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.MapMaker;

/**
 * Identical to the InMemoryStorageConfiguration except that it creates google
 * collections ReferenceMap with Soft references on both keys and values. This
 * behaves like a cache, discarding values when under memory pressure.
 * 
 * 
 */
public class CacheStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "cache";

    public CacheStorageConfiguration() {}

    @SuppressWarnings("unused")
    public CacheStorageConfiguration(VoldemortConfig config) {}

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        ConcurrentMap<ByteArray, List<Versioned<byte[]>>> backingMap = new MapMaker().softValues()
                                                                                     .makeMap();
        return new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeDef.getName(), backingMap);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for " + this.getType()
                                     + " storage engine");
    }
}

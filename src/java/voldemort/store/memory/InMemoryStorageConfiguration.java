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
import java.util.concurrent.ConcurrentHashMap;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * A storage engine that uses a java.util.ConcurrentHashMap to hold the entries
 * 
 * 
 */
public class InMemoryStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "memory";

    public InMemoryStorageConfiguration() {}

    @SuppressWarnings("unused")
    public InMemoryStorageConfiguration(VoldemortConfig config) {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
        return new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeDef.getName(),
                                                                    new ConcurrentHashMap<ByteArray, List<Versioned<byte[]>>>());
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void close() {}

    public void update(StoreDefinition storeDef) {
        throw new VoldemortException("Storage config updates not permitted for "
                                     + this.getClass().getCanonicalName());
    }
}

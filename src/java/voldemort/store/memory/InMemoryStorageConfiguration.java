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

import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * A storage engine that uses a java.util.ConcurrentHashMap to hold the entries
 * 
 * @author jay
 * 
 */
public class InMemoryStorageConfiguration implements StorageConfiguration {

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        return new InMemoryStorageEngine<ByteArray, byte[]>(name,
                                                            new ConcurrentHashMap<ByteArray, List<Versioned<byte[]>>>());
    }

    public StorageEngineType getType() {
        return StorageEngineType.MEMORY;
    }

    public void close() {}

}

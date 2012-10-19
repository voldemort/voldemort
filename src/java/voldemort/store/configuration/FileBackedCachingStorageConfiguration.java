/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.configuration;

import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

/**
 * Storage configuration class for FileBackedCachingStorageEngine
 * 
 * @author csoman
 * 
 */

public class FileBackedCachingStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "file-backed-cache";
    private final String inputPath;

    public FileBackedCachingStorageConfiguration(VoldemortConfig config) {
        this.inputPath = config.getMetadataDirectory();
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        return new FileBackedCachingStorageEngine(storeDef.getName(), inputPath);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void close() {}

    public void update(StoreDefinition storeDef) {

    }
}

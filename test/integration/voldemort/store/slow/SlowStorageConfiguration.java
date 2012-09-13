/*
 * Copyright 2012 LinkedIn, Inc
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
package voldemort.store.slow;

import voldemort.VoldemortException;
import voldemort.common.OpTimeMap;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

/**
 * A storage engine that wraps InMemoryStorageEngine with delays.
 * 
 * 
 */
public class SlowStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "slow";

    private final VoldemortConfig voldemortConfig;

    public SlowStorageConfiguration(VoldemortConfig config) {
        this.voldemortConfig = config;
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
        if(voldemortConfig != null) {
            return new SlowStorageEngine<ByteArray, byte[], byte[]>(storeDef.getName(),
                                                                    this.voldemortConfig.testingGetSlowQueueingDelays(),
                                                                    this.voldemortConfig.testingGetSlowConcurrentDelays());
        }
        return new SlowStorageEngine<ByteArray, byte[], byte[]>(storeDef.getName(),
                                                                new OpTimeMap(0),
                                                                new OpTimeMap(0));
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

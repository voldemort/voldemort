/*
 * Copyright 2009 Geir Magnusson Jr.
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

package voldemort.store.noop;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

/**
 * A storage engine that does nothing. To use :
 * 
 * a) add the following to server.properties
 * 
 * storage.configs = voldemort.store.noop.NoopStorageConfiguration,
 * voldemort.store.bdb.BdbStorageConfiguration
 * 
 * b) use "noop" as the store type in stores.xml
 * 
 * 
 */
public class NoopStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "noop";

    /*
     * property for store.properties to set value reflect (return data sent to
     * it) is a boolean so set to true if you want this
     */
    public static final String REFLECT_PROPERTY = "noop.reflect";

    protected boolean reflect;

    public NoopStorageConfiguration(VoldemortConfig config) {
        reflect = config.getAllProps().getBoolean(REFLECT_PROPERTY, false);
    }

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
        return new NoopStorageEngine(storeDef.getName(), reflect);
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

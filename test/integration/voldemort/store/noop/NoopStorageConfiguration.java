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

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

/**
 * A storage engine that does nothing.  To use :
 *
 * a) add the following to server.properties
 *
 *   storage.configs = voldemort.store.noop.NoopStorageConfiguration, voldemort.store.bdb.BdbStorageConfiguration
 *
 * b) use "noop" as the store type in stores.xml
 *
 * @author geir
 *
 */
public class NoopStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "noop";

    @SuppressWarnings("unused")
    public NoopStorageConfiguration(VoldemortConfig config) {}

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        return new NoopStorageEngine(name);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void close() {}
}

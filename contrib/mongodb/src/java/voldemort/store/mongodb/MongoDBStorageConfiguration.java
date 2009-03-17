/**
 * See the NOTICE.txt file distributed with this work for information regarding
 * copyright ownership.
 * 
 * The authors license this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.store.mongodb;

import org.mongodb.driver.MongoDBException;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

/**
 * Basic StorageConfiguration implementation for MongoDBStorageEngine
 * 
 * @author geir
 */
public class MongoDBStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "mongodb";

    @SuppressWarnings("unused")
    public MongoDBStorageConfiguration(VoldemortConfig config) {}

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        try {
            return new MongoDBStorageEngine(name);
        } catch(MongoDBException e) {
            throw new VoldemortException(e);
        }
    }

    public String getType() {
        return TYPE_NAME;
    }

    public void close() {
    // for now
    }
}

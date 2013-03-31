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

package voldemort.store;

import voldemort.routing.RoutingStrategy;
import voldemort.utils.ByteArray;

/**
 * An abstraction that represents the shared resources of a persistence engine.
 * This could include file handles, db connection pools, caches, etc.
 * 
 * For example for BDB it holds the various environments, for jdbc it holds a
 * connection pool reference
 * 
 * This should be called StorageEngineFactory but that would leave us with the
 * indignity of having a StorageEngineFactoryFactory to handle the mapping of
 * store type => factory. And we can't have that.
 * 
 * 
 */
public interface StorageConfiguration {

    /**
     * Get an initialized storage implementation
     * 
     * @param storeDef store definition
     * @param strategy routing strategy used for the store
     * @return The storage engine
     */
    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy);

    /**
     * Get the type of stores returned by this configuration
     */
    public String getType();

    /**
     * Update the storage configuration at runtime
     * 
     * @param storeDef new store definition object
     */
    public void update(StoreDefinition storeDef);

    /**
     * Close the storage configuration
     */
    public void close();
}

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

package voldemort.client;

import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * An abstraction the represents a connection to a Voldemort cluster and can be
 * used to create StoreClients to interact with individual stores
 * 
 * @author jay
 * 
 */
public interface StoreClientFactory {

    public <K, V> StoreClient<K, V> getStoreClient(String storeName);

    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> inconsistencyResolver);

}
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

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Versioned;

/**
 * An abstraction the represents a connection to a Voldemort cluster and can be
 * used to create {@link voldemort.client.StoreClient StoreClient} instances to
 * interact with individual stores. The factory abstracts away any connection
 * pools, thread pools, or other details that will be shared by all the
 * individual
 * 
 * {@link voldemort.client.StoreClient StoreClient}
 * 
 * 
 */
public interface StoreClientFactory {

    /**
     * Get a {@link voldemort.client.StoreClient} for the given store.
     * 
     * @param <K> The type of the key of the
     *        {@link voldemort.client.StoreClient}
     * @param <V> The type of the value of the
     *        {@link voldemort.client.StoreClient}
     * @param storeName The name of the store
     * @return A fully constructed {@link voldemort.client.StoreClient}
     */
    public <K, V> StoreClient<K, V> getStoreClient(String storeName);

    /**
     * Get a {@link voldemort.client.StoreClient} for the given store.
     * 
     * @param <K> The type of the key of the
     *        {@link voldemort.client.StoreClient}
     * @param <V> The type of the value of the
     *        {@link voldemort.client.StoreClient}
     * @param storeName The name of the store
     * @param inconsistencyResolver The
     *        {@link voldemort.versioning.InconsistencyResolver} that should be
     *        used to resolve inconsistencies.
     * @return A fully constructed {@link voldemort.client.StoreClient}
     */
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> inconsistencyResolver);

    /**
     * Get the underlying store, not the public StoreClient interface
     * 
     * @param storeName The name of the store
     * @param resolver The inconsistency resolver
     * @return The appropriate store
     */
    <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                         InconsistencyResolver<Versioned<V>> resolver);

    /**
     * Close the store client
     */
    public void close();

    /**
     * Returns the FailureDetector specific to the cluster against which this
     * client factory is based.
     * 
     * @return FailureDetector
     */

    public FailureDetector getFailureDetector();

}

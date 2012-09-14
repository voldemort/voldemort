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

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The basic interface used for storage and storage decorators. Allows the usual
 * crud operations.
 * 
 * Note that certain operations rely on the correct implementation of equals and
 * hashCode for the key. As such, arrays as keys should be avoided.
 * 
 * 
 */
@Threadsafe
public interface Store<K, V, T> {

    /**
     * Get the value associated with the given key
     * 
     * @param key The key to check for
     * @return The value associated with the key or an empty list if no values
     *         are found.
     * @throws VoldemortException
     */
    public List<Versioned<V>> get(K key, T transforms) throws VoldemortException;

    /**
     * Get the values associated with the given keys and returns them in a Map
     * of keys to a list of versioned values. Note that the returned map will
     * only contain entries for the keys which have a value associated with
     * them.
     * 
     * @param keys The keys to check for.
     * @param transforms Transforms per key
     * @return A Map of keys to a list of versioned values.
     * @throws VoldemortException
     */
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException;

    /**
     * Associate the value with the key and version in this store
     * 
     * @param key The key to use
     * @param value The value to store and its version.
     */
    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException;

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version The current value of the key
     * @return True if anything was deleted
     */
    public boolean delete(K key, Version version) throws VoldemortException;

    /**
     * @return The name of the store.
     */
    public String getName();

    /**
     * Close the store.
     * 
     * @throws VoldemortException If closing fails.
     */
    public void close() throws VoldemortException;

    /**
     * Get some capability of the store. Examples would be the serializer used,
     * or the routing strategy. This provides a mechanism to verify that the
     * store hierarchy has some set of capabilities without knowing the precise
     * layering.
     * 
     * @param capability The capability type to retrieve
     * @return The given capability
     * @throws NoSuchCapabilityException if the capability is not present
     */
    public Object getCapability(StoreCapabilityType capability);

    /**
     * Get all the versions for a key
     * 
     * @param key The key
     * @return List of versions
     */
    public List<Version> getVersions(K key);

    /**
     * Given a set of keys, check if it exists
     * 
     * @param keys Set of keys to check
     * @return Map of key to boolean
     */
    public Map<K, Boolean> hasKeys(Iterable<K> keys);

}

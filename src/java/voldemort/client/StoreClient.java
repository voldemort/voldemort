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

import java.util.List;
import java.util.Map;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.cluster.Node;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The user-facing interface to a Voldemort store. Gives basic put/get/delete
 * plus helper functions.
 * 
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 */
@Threadsafe
public interface StoreClient<K, V> {

    /**
     * Get the value associated with the given key or null if there is no value
     * associated with this key. This method strips off all version information
     * and is only useful when no further storage operations will be done on
     * this key.
     * 
     * @param key The key
     */
    public V getValue(K key);

    /**
     * Get the value associated with the given key or defaultValue if there is
     * no value associated with the key. This method strips off all version
     * information and is only useful when no further storage operations will be
     * done on this key.
     * 
     * @param key The key for which to fetch the associated value
     * @param defaultValue A value to return if there is no value associated
     *        with this key
     * @return Either the value stored for the key or the default value.
     */
    public V getValue(K key, V defaultValue);

    /**
     * Get the versioned value associated with the given key or null if no value
     * is associated with the key.
     * 
     * @param key The key for which to fetch the value.
     * @return The versioned value, or null if no value is stored for this key.
     */
    public Versioned<V> get(K key);

    /**
     * Get the versioned value associated with the given key and apply the given
     * transforms to it before returning the value. Returns null if no value is
     * associated with the key
     * 
     * @param key the key for which the value is fetched
     * @param transforms the transforms to be applied on the value fetched from
     *        the store
     * @return the transformed versioned value, or null if no value is stored
     *         for this key
     */
    public Versioned<V> get(K key, Object transforms);

    /**
     * Gets the versioned values associated with the given keys and returns them
     * in a Map of keys to versioned values. Note that the returned map will
     * only contain entries for the keys which have a value associated with
     * them.
     * 
     * @param keys The keys for which to fetch the values.
     * @return A Map of keys to versioned values.
     */
    public Map<K, Versioned<V>> getAll(Iterable<K> keys);

    /**
     * Like {@link voldemort.client.StoreClient#getAll(Iterable) getAll}, except
     * that the transforms are applied on the value associated with each key
     * before returning the results
     * 
     * @param keys the keys for which the values are fetched
     * @param transforms the map of transforms, describing the transform to be
     *        applied to the value for each key
     * @return A map of keys to transformed versioned values
     */
    public Map<K, Versioned<V>> getAll(Iterable<K> keys, Map<K, Object> transforms);

    /**
     * Get the versioned value associated with the given key or the defaultValue
     * if no value is associated with the key.
     * 
     * @param key The key for which to fetch the value.
     * @return The versioned value, or the defaultValue if no value is stored
     *         for this key.
     */
    public Versioned<V> get(K key, Versioned<V> defaultValue);

    /**
     * Associated the given value to the key, clobbering any existing values
     * stored for the key.
     * 
     * @param key The key
     * @param value The value
     * @return version The version of the object
     */
    public Version put(K key, V value);

    /**
     * Like {@link voldemort.client.StoreClient #put(Object, Object)}, except
     * that the given transforms are applied on the value before writing it to
     * the store
     * 
     * @param key the key
     * @param value the value
     * @param transforms the transforms to be applied on the value
     * @return version The version of the object
     */
    public Version put(K key, V value, Object transforms);

    /**
     * Put the given Versioned value into the store for the given key if the
     * version is greater to or concurrent with existing values. Throw an
     * ObsoleteVersionException otherwise.
     * 
     * @param key The key
     * @param versioned The value and its versioned
     * @throws ObsoleteVersionException
     */
    public Version put(K key, Versioned<V> versioned) throws ObsoleteVersionException;

    /**
     * Put the versioned value to the key, ignoring any ObsoleteVersionException
     * that may be thrown
     * 
     * @param key The key
     * @param versioned The versioned value
     * @return true if the put succeeded
     */
    public boolean putIfNotObsolete(K key, Versioned<V> versioned);

    /**
     * Apply the given action repeatedly until no ObsoleteVersionException is
     * thrown. This is useful for implementing a read-modify-store loop that
     * could be pre-empted by another concurrent update, and should be repeated
     * until it succeeds.
     * 
     * @param action The action to apply. This is meant as a callback for the
     *        user to extend to provide their own logic.
     * @return true if the action is successfully applied, false if the 3
     *         attempts all result in ObsoleteVersionException
     */
    public boolean applyUpdate(UpdateAction<K, V> action);

    /**
     * Apply the given action repeatedly until no ObsoleteVersionException is
     * thrown or maxTries unsuccessful attempts have been made. This is useful
     * for implementing a read-modify-store loop.
     * 
     * @param action The action to apply
     * @return true if the action is successfully applied, false if maxTries
     *         failed attempts have been made
     */
    public boolean applyUpdate(UpdateAction<K, V> action, int maxTries);

    /**
     * Delete any version of the given key which equal to or less than the
     * current versions
     * 
     * @param key The key
     * @return true if anything is deleted
     */
    public boolean delete(K key);

    /**
     * Delete the specified version and any prior versions of the given key
     * 
     * @param key The key to delete
     * @param version The version of the key
     * @return true if anything is deleted
     */
    public boolean delete(K key, Version version);

    /**
     * Returns the list of nodes which should have this key.
     * 
     * @param key
     * @return a list of Nodes which should hold this key
     */
    public List<Node> getResponsibleNodes(K key);
}

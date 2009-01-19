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

import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The user-facing interface to a Voldemort store. Gives basic put/get/delete
 * plus helper functions.
 * 
 * @author jay
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 */
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
     */
    public void put(K key, V value);

    /**
     * Put the given Versioned value into the store for the given key if the
     * version is greater to or concurrent with existing values. Throw an
     * ObsoleteVersionException otherwise.
     * 
     * @param key The key
     * @param versioned The value and its versioned
     * @throws ObsoleteVersionException
     */
    public void put(K key, Versioned<V> versioned) throws ObsoleteVersionException;

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

}

/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import voldemort.server.storage.KeyLockHandle;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * A base storage class which is actually responsible for data persistence. This
 * interface implies all the usual responsibilities of a Store implementation,
 * and in addition
 * <ol>
 * <li>The implementation MUST throw an ObsoleteVersionException if the user
 * attempts to put a version which is strictly before an existing version
 * (concurrent is okay)</li>
 * <li>The implementation MUST increment this version number when the value is
 * stored.</li>
 * <li>The implementation MUST contain an ID identifying it as part of the
 * cluster</li>
 * </ol>
 * 
 * A hash value can be produced for known subtrees of a StorageEngine
 * 
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * @param <T> The type of the transforms
 * 
 */
public interface StorageEngine<K, V, T> extends Store<K, V, T> {

    /**
     * Get an iterator over pairs of entries in the store. The key is the first
     * element in the pair and the versioned value is the second element.
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @return An iterator over the entries in this StorageEngine.
     */
    public ClosableIterator<Pair<K, Versioned<V>>> entries();

    /**
     * Get an iterator over keys in the store.
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @return An iterator over the keys in this StorageEngine.
     */
    public ClosableIterator<K> keys();

    /**
     * Get an iterator over pairs of entries in a store's partition. The key is
     * the first element in the pair and the versioned value is the second
     * element.
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @param partition partition whose entries are to be fetched
     * @return An iterator over the entries in this StorageEngine.
     */
    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partition);

    /**
     * Get an iterator over keys in the store's partition
     * 
     * Note that the iterator need not be threadsafe, and that it must be
     * manually closed after use.
     * 
     * @param partition partition whose keys are to be fetched
     * @return An iterator over the keys in this StorageEngine.
     */
    public ClosableIterator<K> keys(int partition);

    /**
     * Truncate all entries in the store
     */
    public void truncate();

    /**
     * Are partitions persisted in distinct files? In other words is the data
     * stored on disk on a per-partition basis? This is really for the read-only
     * use case in which each partition is stored in a distinct file.
     * 
     * @return Boolean indicating if partitions are persisted in distinct files
     *         (read-only use case).
     */
    public boolean isPartitionAware();

    /**
     * Does the storage engine support efficient scanning of a single partition?
     * 
     * @return true if the storage engine implements the capability. false
     *         otherwise
     */
    public boolean isPartitionScanSupported();

    /**
     * A lot of storage engines support efficient methods for performing large
     * number of writes (puts/deletes) against the data source. This method puts
     * the storage engine in this batch write mode
     * 
     * @return true if the storage engine took successful action to switch to
     *         'batch-write' mode
     */
    public boolean beginBatchModifications();

    /**
     * Atomically update storage with the list of versioned values for the given
     * key, to improve storage efficiency.
     * 
     * @param key Key to write
     * @param values List of versioned values to be written atomically.
     * @return list of obsolete versions that were rejected
     */
    public List<Versioned<V>> multiVersionPut(K key, List<Versioned<V>> values);

    /**
     * Returns the list of versions stored for the key, at the same time locking
     * the key for any writes until
     * {@link StorageEngine#putAndUnlock(Object, KeyLockHandle)} is called with
     * the same lock handle. The idea here is to facilitate custom atomic
     * Read-Modify-Write logic outside the storage engine
     * 
     * @param key
     * @return
     */
    public KeyLockHandle<V> getAndLock(K key);

    /**
     * Takes the handle issued from a prior
     * {@link StorageEngine#getAndLock(Object)} call, and update the key with
     * the set of values provided in the handle, also releasing the lock held on
     * the key.
     * 
     * FIXME VC need also a way to release the lock alone for error handling
     * 
     * @param key
     * @param handle handle object with new list of versions to be stored
     */
    public void putAndUnlock(K key, KeyLockHandle<V> handle);

    /**
     * Release any lock held by a prior
     * {@link AbstractStorageEngine#getAndLock(Object)} call
     * 
     * @param handle
     */
    public void releaseLock(KeyLockHandle<V> handle);

    /**
     * 
     * @return true if the storage engine successfully returned to normal mode
     */
    public boolean endBatchModifications();
}

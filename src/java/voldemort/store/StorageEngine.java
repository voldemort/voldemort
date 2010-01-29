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
 * @author jay
 * 
 * @param <K> The type of the key being stored
 * @param <V> The type of the value being stored
 * 
 */
public interface StorageEngine<K, V> extends Store<K, V> {

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
     * Truncate all entries in the store
     */
    public void truncate();
}

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
package voldemort.server.storage;

import java.util.List;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.store.StorageEngine;
import voldemort.versioning.Versioned;

/**
 * Class encapsulating the state necessary to lock a key on the underlying
 * storage and the list of versions stored for the key
 * 
 */
@NotThreadsafe
public class KeyLockHandle<V> {

    /**
     * List of values obtained from or written into storage, during getAndLock
     * and putAndUnlock respectively.
     */
    private List<Versioned<V>> values;
    /**
     * Object representing the underlying storage lock for the key
     */
    private final Object keyLock;
    /**
     * Track whether this handle is open or closed. Handles are opened by
     * {@link StorageEngine#getAndLock(Object)} and closed by
     * {@link StorageEngine#releaseLock(KeyLockHandle)} or
     * {@link StorageEngine#putAndUnlock(Object, KeyLockHandle)}
     */
    private boolean closed;

    public KeyLockHandle(List<Versioned<V>> values, Object keyLock) {
        this.values = values;
        this.keyLock = keyLock;
        this.closed = false;
    }

    public void setValues(List<Versioned<V>> values) {
        this.values = values;
    }

    public List<Versioned<V>> getValues() {
        return values;
    }

    public Object getKeyLock() {
        return keyLock;
    }

    public void close() {
        if(closed) {
            throw new IllegalStateException("Handle already closed");
        }
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}

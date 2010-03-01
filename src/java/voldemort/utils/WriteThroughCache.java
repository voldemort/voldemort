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

package voldemort.utils;

import java.util.HashMap;
import java.util.Hashtable;

import voldemort.VoldemortException;

/**
 * Extends Java HashMap {@link HashMap} as a Write through cache <br>
 * provide a hook to write custom write back strategies
 * 
 * 
 */
public abstract class WriteThroughCache<K, V> extends Hashtable<K, V> {

    private static final long serialVersionUID = 1L;

    public abstract V readBack(K key);

    public abstract void writeBack(K key, V value);

    /**
     * get() calls the readBack function if the value is not in HashMap
     * otherwise serve copy from hash directly.
     */
    @SuppressWarnings("unchecked")
    @Override
    synchronized public V get(Object key) {
        if(!this.containsKey(key)) {
            super.put((K) key, readBack((K) key));
        }

        return super.get(key);
    }

    /**
     * Updates the value in HashMap and writeBack as Atomic step
     */
    @Override
    synchronized public V put(K key, V value) {
        V oldValue = this.get(key);
        try {
            super.put(key, value);
            writeBack(key, value);
            return oldValue;
        } catch(Exception e) {
            super.put(key, oldValue);
            writeBack(key, oldValue);
            throw new VoldemortException("Failed to put(" + key + ", " + value
                                         + ") in write through cache", e);
        }
    }
}

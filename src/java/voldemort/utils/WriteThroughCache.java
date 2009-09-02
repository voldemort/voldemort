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
 * @author bbansal
 * 
 */
public abstract class WriteThroughCache<K, V> extends Hashtable<K, V> {

    public abstract V readBack(K key);

    public abstract void writeBack(K key, V value);

    /**
     * get() calls the readBack function if the value is not in HashMap
     * otherwise serve copy from hash directly.
     */
    @Override
    public V get(Object key) {
        if(!this.containsKey(key)) {
            synchronized(this) {
                this.put((K) key, readBack((K) key));
            }
        }

        return this.get(key);
    }

    /**
     * Updates the value in HashMap and writeBack as Atomic step
     */
    @Override
    public V put(K key, V value) {
        synchronized(this) {
            V oldValue = this.get(key);
            try {
                this.put(key, value);
                writeBack(key, value);
                return oldValue;
            } catch(Exception e) {
                this.put(key, oldValue);
                writeBack(key, oldValue);
                throw new VoldemortException("Failed to put(" + key + ", " + value
                                             + ") in write through cache", e);
            }
        }
    }
}
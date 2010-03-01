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

package voldemort.store.stats;

import java.util.List;
import java.util.Map;

import javax.management.MBeanOperationInfo;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that tracks basic usage statistics
 * 
 * 
 */
public class StatTrackingStore<K, V> extends DelegatingStore<K, V> {

    private StoreStats stats;

    public StatTrackingStore(Store<K, V> innerStore, StoreStats parentStats) {
        super(innerStore);
        this.stats = new StoreStats(parentStats);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return super.delete(key, version);
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.DELETE, System.nanoTime() - start);
        }
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return super.get(key);
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.GET, System.nanoTime() - start);
        }
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        long start = System.nanoTime();
        try {
            return super.getAll(keys);
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.GET_ALL, System.nanoTime() - start);
        }
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        long start = System.nanoTime();
        try {
            super.put(key, value);
        } catch(ObsoleteVersionException e) {
            stats.recordTime(Tracked.OBSOLETE, System.nanoTime() - start);
            throw e;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.PUT, System.nanoTime() - start);
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.STAT_TRACKER.equals(capability))
            return this.stats;
        else
            return super.getCapability(capability);
    }

    public StoreStats getStats() {
        return stats;
    }

    @JmxOperation(description = "Reset statistics.", impact = MBeanOperationInfo.ACTION)
    public void resetStatistics() {
        this.stats = new StoreStats();
    }
}

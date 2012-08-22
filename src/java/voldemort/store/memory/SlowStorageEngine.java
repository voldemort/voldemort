/*
 * Copyright 2012 LinkedIn, Inc
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

package voldemort.store.memory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A wrapped version of InMemoryStorageEngine that can add delay to each
 * operation type. Useful for unit testing.
 * 
 * Any operation with queueingDelays of more than 0 ms blocks until it has
 * slept. It will queue up behind other queuingDelays operations that need to
 * sleep first.
 * 
 * Any operation with concurrentDelays of more than 0 ms sleeps for the
 * specified amount of time. This delay does not block other operations. I.e.,
 * multiple operations sleep for the specified concurrentDelays simultaneously.
 * 
 * Both queueingDelays and concurrentDelays may be specified for each operation.
 * queueingDelays are done before concurrentDelays; time spent in queueingDelays
 * does not affect concurrentDelays.
 * 
 */
public class SlowStorageEngine<K, V, T> implements StorageEngine<K, V, T> {

    public static class OperationDelays {

        public long getMs;
        public long getVersionsMs;
        public long getAllMs;
        public long putMs;
        public long deleteMs;

        public OperationDelays() {
            this.getMs = 0;
            this.getVersionsMs = 0;
            this.getAllMs = 0;
            this.putMs = 0;
            this.deleteMs = 0;
        }

        public OperationDelays(long getMs,
                               long getVersionsMs,
                               long getAllMs,
                               long putMs,
                               long deleteMs) {
            this.getMs = getMs;
            this.getVersionsMs = getVersionsMs;
            this.getAllMs = getAllMs;
            this.putMs = putMs;
            this.deleteMs = deleteMs;
        }
    }

    private final InMemoryStorageEngine<K, V, T> imStore;
    private final OperationDelays queueingDelays;
    private final OperationDelays concurrentDelays;

    public SlowStorageEngine(String name) {
        this(name, new OperationDelays(), new OperationDelays());
    }

    public SlowStorageEngine(String name,
                             OperationDelays queueingDelays,
                             OperationDelays concurrentDelays) {
        imStore = new InMemoryStorageEngine<K, V, T>(name);
        this.queueingDelays = queueingDelays;
        this.concurrentDelays = concurrentDelays;
    }

    private synchronized void queueingSleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void concurrentSleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    public boolean delete(K key, Version version) {
        if(queueingDelays.deleteMs > 0)
            queueingSleep(queueingDelays.deleteMs);
        if(concurrentDelays.deleteMs > 0)
            concurrentSleep(concurrentDelays.deleteMs);
        return imStore.delete(key, version);
    }

    public List<Version> getVersions(K key) {
        if(queueingDelays.getVersionsMs > 0)
            queueingSleep(queueingDelays.getVersionsMs);
        if(concurrentDelays.getVersionsMs > 0)
            concurrentSleep(concurrentDelays.getVersionsMs);
        return imStore.getVersions(key);
    }

    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        if(queueingDelays.getMs > 0)
            queueingSleep(queueingDelays.getMs);
        if(concurrentDelays.getMs > 0)
            concurrentSleep(concurrentDelays.getMs);
        return imStore.get(key, transform);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        if(queueingDelays.getAllMs > 0)
            queueingSleep(queueingDelays.getAllMs);
        if(concurrentDelays.getAllMs > 0)
            concurrentSleep(concurrentDelays.getAllMs);
        return imStore.getAll(keys, transforms);
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        if(queueingDelays.putMs > 0)
            queueingSleep(queueingDelays.putMs);
        if(concurrentDelays.putMs > 0)
            concurrentSleep(concurrentDelays.putMs);
        imStore.put(key, value, transforms);
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return imStore.entries();
    }

    public ClosableIterator<K> keys() {
        return imStore.keys();
    }

    public void truncate() {
        imStore.truncate();
    }

    public boolean isPartitionAware() {
        return imStore.isPartitionAware();
    }

    public String getName() {
        return imStore.getName();
    }

    public void close() {
        imStore.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return imStore.getCapability(capability);
    }

}

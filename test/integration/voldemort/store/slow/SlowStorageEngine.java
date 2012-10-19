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

package voldemort.store.slow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.common.OpTimeMap;
import voldemort.common.VoldemortOpCode;
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

    private final StorageEngine<K, V, T> innerStorageEngine;
    private final OpTimeMap queueingDelays;
    private final OpTimeMap concurrentDelays;

    public SlowStorageEngine(StorageEngine<K, V, T> innerStorageEngine) {
        this(innerStorageEngine, new OpTimeMap(0), new OpTimeMap(0));
    }

    public SlowStorageEngine(StorageEngine<K, V, T> innerStorageEngine,
                             OpTimeMap queueingDelays,
                             OpTimeMap concurrentDelays) {
        this.innerStorageEngine = innerStorageEngine;
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

    private void delayByOp(byte opCode) {
        if(queueingDelays.getOpTime(opCode) > 0)
            queueingSleep(queueingDelays.getOpTime(opCode));
        if(concurrentDelays.getOpTime(opCode) > 0)
            concurrentSleep(concurrentDelays.getOpTime(opCode));
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    public boolean delete(K key, Version version) {
        delayByOp(VoldemortOpCode.DELETE_OP_CODE);
        return innerStorageEngine.delete(key, version);
    }

    public List<Version> getVersions(K key) {
        delayByOp(VoldemortOpCode.GET_VERSION_OP_CODE);
        return innerStorageEngine.getVersions(key);
    }

    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        delayByOp(VoldemortOpCode.GET_OP_CODE);
        return innerStorageEngine.get(key, transform);
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        delayByOp(VoldemortOpCode.GET_ALL_OP_CODE);
        return innerStorageEngine.getAll(keys, transforms);
    }

    public void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        delayByOp(VoldemortOpCode.PUT_OP_CODE);
        innerStorageEngine.put(key, value, transforms);
    }

    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return innerStorageEngine.entries();
    }

    public ClosableIterator<K> keys() {
        return innerStorageEngine.keys();
    }

    public void truncate() {
        innerStorageEngine.truncate();
    }

    public boolean isPartitionAware() {
        return innerStorageEngine.isPartitionAware();
    }

    public String getName() {
        return innerStorageEngine.getName();
    }

    public void close() {
        innerStorageEngine.close();
    }

    public Object getCapability(StoreCapabilityType capability) {
        return innerStorageEngine.getCapability(capability);
    }

}

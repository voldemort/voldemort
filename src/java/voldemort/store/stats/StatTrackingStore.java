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

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import javax.management.MBeanOperationInfo;

/**
 * A store wrapper that tracks basic usage statistics
 * 
 * @author jay
 * 
 */
public class StatTrackingStore<K, V> extends DelegatingStore<K, V> {

    private StoreStats stats = new StoreStats();

    public StatTrackingStore(Store<K, V> innerStore) {
        super(innerStore);
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
        } catch (ObsoleteVersionException e) {
            // Don't rethrow this, as not to log it. Merely track it in JMX
            stats.recordTime(Tracked.OBSOLETE, System.nanoTime() - start);
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.PUT, System.nanoTime() - start);
        }
    }

    public Map<Tracked, RequestCounter> getCounters() {
        return stats.getCounters();
    }

    @JmxGetter(name = "numberOfCallsToGetAll", description = "The number of calls to GET_ALL since the last reset.")
    public long getNumberOfCallsToGetAll() {
        return stats.getCount(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET_ALL calls to complete.")
    public double getAverageGetAllCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "GetAllThroughput", description = "Throughput of GET_ALL requests.")
    public float getGetAllThroughput() {
        return stats.getThroughput(Tracked.GET_ALL);
    }

    @JmxGetter(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.")
    public long getNumberOfCallsToGet() {
        return stats.getCount(Tracked.GET);
    }

    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET);
    }

    @JmxGetter(name = "GetThroughput", description = "Throughput of GET requests.")
    public float getGetThroughput() {
        return stats.getThroughput(Tracked.GET);
    }

    @JmxGetter(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.")
    public long getNumberOfCallsToPut() {
        return stats.getCount(Tracked.PUT);
    }

    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.PUT);
    }

    @JmxGetter(name = "PutThroughput", description = "Throughput of PUT requests.")
    public float getPutThroughput() {
        return stats.getThroughput(Tracked.PUT);
    }

    @JmxGetter(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.")
    public long getNumberOfCallsToDelete() {
        return stats.getCount(Tracked.DELETE);
    }

    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "DeleteThroughput", description = "Throughput of DELETE requests.")
    public float getDeleteThroughput() {
        return stats.getThroughput(Tracked.DELETE);
    }

    @JmxGetter(name = "numberOfObsoleteVersions",
            description = "Number of ObsoleteVersionExceptions since the last reset.")
    public long getNumberOfObsoleteVersions() {
        return stats.getCount(Tracked.OBSOLETE);
    }

    @JmxGetter(name = "numberOfExceptions", description = "The number of exceptions since the last reset.")
    public long getNumberOfExceptions() {
        return stats.getCount(Tracked.EXCEPTION);
    }

    @JmxOperation(description = "Reset statistics.", impact = MBeanOperationInfo.ACTION)
    public void resetStatistics() {
        this.stats = new StoreStats();
    }
}

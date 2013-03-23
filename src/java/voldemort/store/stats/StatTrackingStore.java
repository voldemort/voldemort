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
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that tracks basic usage statistics
 * 
 * 
 */
public class StatTrackingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private StoreStats stats;

    public StatTrackingStore(Store<ByteArray, byte[], byte[]> innerStore, StoreStats parentStats) {
        super(innerStore);
        this.stats = new StoreStats(parentStats);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
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
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> result = null;
        long start = System.nanoTime();
        try {
            result = super.get(key, transforms);
            return result;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            long duration = System.nanoTime() - start;
            long totalBytes = 0;
            boolean returningEmpty = true;
            if(result != null) {
                returningEmpty = result.size() == 0;
                for(Versioned<byte[]> bytes: result) {
                    totalBytes += bytes.getValue().length;
                }
            }
            stats.recordGetTime(duration, returningEmpty, totalBytes);
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        Map<ByteArray, List<Versioned<byte[]>>> result = null;
        long start = System.nanoTime();
        try {
            result = super.getAll(keys, transforms);
            return result;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            long duration = System.nanoTime() - start;
            long totalBytes = 0;
            int requestedValues = 0;
            int returnedValues = 0;

            // Determine how many values were requested
            for(ByteArray k: keys) {
                requestedValues++;
            }

            if(result != null) {
                // Determine the number of values being returned
                returnedValues = result.keySet().size();
                // Determine the total size of the response
                for(List<Versioned<byte[]>> value: result.values()) {
                    for(Versioned<byte[]> bytes: value) {
                        totalBytes += bytes.getValue().length;
                    }
                }
            }

            stats.recordGetAllTime(duration, requestedValues, returnedValues, totalBytes);
        }
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        long start = System.nanoTime();
        try {
            super.put(key, value, transforms);
        } catch(ObsoleteVersionException e) {
            stats.recordTime(Tracked.OBSOLETE, System.nanoTime() - start);
            throw e;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordPutTimeAndSize(System.nanoTime() - start, value.getValue().length);
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

    @Override
    public List<Versioned<byte[]>> get(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        List<Versioned<byte[]>> result = null;
        long start = System.nanoTime();
        try {
            result = super.get(request);
            return result;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            long duration = System.nanoTime() - start;
            long totalBytes = 0;
            boolean returningEmpty = true;
            if(result != null) {
                returningEmpty = result.size() == 0;
                for(Versioned<byte[]> bytes: result) {
                    totalBytes += bytes.getValue().length;
                }
            }
            stats.recordGetTime(duration, returningEmpty, totalBytes);
        }
    }

    @Override
    // TODO: Validate all the keys in the request object
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        Map<ByteArray, List<Versioned<byte[]>>> result = null;
        long start = System.nanoTime();
        try {
            result = super.getAll(request);
            return result;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            long duration = System.nanoTime() - start;
            long totalBytes = 0;
            int requestedValues = 0;
            int returnedValues = 0;

            // Determine how many values were requested
            for(ByteArray k: request.getIterableKeys()) {
                requestedValues++;
            }

            if(result != null) {
                // Determine the number of values being returned
                returnedValues = result.keySet().size();
                // Determine the total size of the response
                for(List<Versioned<byte[]>> value: result.values()) {
                    for(Versioned<byte[]> bytes: value) {
                        totalBytes += bytes.getValue().length;
                    }
                }
            }

            stats.recordGetAllTime(duration, requestedValues, returnedValues, totalBytes);
        }
    }

    @Override
    public void put(CompositeVoldemortRequest<ByteArray, byte[]> request) throws VoldemortException {
        long start = System.nanoTime();
        try {
            super.put(request);
        } catch(ObsoleteVersionException e) {
            stats.recordTime(Tracked.OBSOLETE, System.nanoTime() - start);
            throw e;
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordPutTimeAndSize(System.nanoTime() - start,
                                       request.getValue().getValue().length);
        }

    }

    @Override
    public boolean delete(CompositeVoldemortRequest<ByteArray, byte[]> request)
            throws VoldemortException {
        long start = System.nanoTime();
        try {
            return super.delete(request);
        } catch(VoldemortException e) {
            stats.recordTime(Tracked.EXCEPTION, System.nanoTime() - start);
            throw e;
        } finally {
            stats.recordTime(Tracked.DELETE, System.nanoTime() - start);
        }
    }
}

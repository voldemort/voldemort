package voldemort.store.stats;

import java.util.List;

import javax.management.MBeanOperationInfo;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that tracks basic usage statistics
 * 
 * @author jay
 * 
 */
public class StatTrackingStore<K, V> extends DelegatingStore<K, V> {

    private volatile int callsToGet = 0;
    private volatile double avgGetCompletionTime = 0.0d;

    private volatile int callsToPut = 0;
    private volatile double avgPutCompletionTime = 0.0d;

    private volatile int callsToDelete = 0;
    private volatile double avgDeleteCompletionTime = 0.0d;

    private volatile int exceptionsThrown = 0;

    public StatTrackingStore(Store<K, V> innerStore) {
        super(innerStore);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        callsToDelete++;
        long start = System.nanoTime();
        try {
            return super.delete(key, version);
        } catch (VoldemortException e) {
            exceptionsThrown++;
            throw e;
        } finally {
            long elapsed = System.nanoTime() - start;
            avgDeleteCompletionTime += (elapsed - avgDeleteCompletionTime) / callsToDelete;
        }
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        callsToGet++;
        long start = System.nanoTime();
        try {
            return super.get(key);
        } catch (VoldemortException e) {
            exceptionsThrown++;
            throw e;
        } finally {
            long elapsed = System.nanoTime() - start;
            avgGetCompletionTime += (elapsed - avgGetCompletionTime) / callsToGet;
        }
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        callsToPut++;
        long start = System.nanoTime();
        try {
            super.put(key, value);
        } catch (VoldemortException e) {
            exceptionsThrown++;
            throw e;
        } finally {
            long elapsed = System.nanoTime() - start;
            avgPutCompletionTime += (elapsed - avgPutCompletionTime) / callsToPut;
        }
    }

    @JmxGetter(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.")
    public int getNumberOfCallsToGet() {
        return callsToGet;
    }

    @JmxGetter(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.")
    public int getNumberOfCallsToPut() {
        return callsToPut;
    }

    @JmxGetter(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.")
    public int getNumberOfCallsToDelete() {
        return callsToDelete;
    }

    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return avgGetCompletionTime / Time.NS_PER_MS;
    }

    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return avgPutCompletionTime / Time.NS_PER_MS;
    }

    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return avgDeleteCompletionTime / Time.NS_PER_MS;
    }

    @JmxOperation(description = "Reset statistics.", impact = MBeanOperationInfo.ACTION)
    public void resetStatistics() {
        callsToGet = 0;
        callsToPut = 0;
        callsToDelete = 0;
        avgGetCompletionTime = 0d;
        avgPutCompletionTime = 0d;
        avgDeleteCompletionTime = 0d;
    }

}

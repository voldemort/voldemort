package voldemort.store.retention;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStoreListener;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Wraps the storage layer and ensures we don't return any values that are
 * stale. Optionally, deletes the expired versions.
 * 
 */
public class RetentionEnforcingStore extends DelegatingStore<ByteArray, byte[], byte[]> implements
        MetadataStoreListener {

    private volatile StoreDefinition storeDef;
    private boolean deleteExpiredEntries;
    private volatile long retentionTimeMs;
    private Time time;

    public RetentionEnforcingStore(Store<ByteArray, byte[], byte[]> innerStore,
                                   StoreDefinition storeDef,
                                   boolean deleteExpiredEntries,
                                   Time time) {
        super(innerStore);
        updateStoreDefinition(storeDef);
        this.deleteExpiredEntries = deleteExpiredEntries;
        this.time = time;
    }

    @Override
    public void updateRoutingStrategy(RoutingStrategy routingStrategyMap) {
        return; // no-op
    }

    /**
     * Updates the store definition object and the retention time based on the
     * updated store definition
     */
    @Override
    public void updateStoreDefinition(StoreDefinition storeDef) {
        this.storeDef = storeDef;
        if(storeDef.hasRetentionPeriod())
            this.retentionTimeMs = storeDef.getRetentionDays() * Time.MS_PER_DAY;
    }

    /**
     * Performs the filtering of the expired entries based on retention time.
     * Optionally, deletes them also
     * 
     * @param key the key whose value is to be deleted if needed
     * @param vals set of values to be filtered out
     * @return filtered list of values which are currently valid
     */
    private List<Versioned<byte[]>> filterExpiredEntries(ByteArray key, List<Versioned<byte[]>> vals) {
        Iterator<Versioned<byte[]>> valsIterator = vals.iterator();
        while(valsIterator.hasNext()) {
            Versioned<byte[]> val = valsIterator.next();
            VectorClock clock = (VectorClock) val.getVersion();
            // omit if expired
            if(clock.getTimestamp() < (time.getMilliseconds() - this.retentionTimeMs)) {
                valsIterator.remove();
                // delete stale value if configured
                if(deleteExpiredEntries) {
                    getInnerStore().delete(key, clock);
                }
            }
        }
        return vals;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> results = getInnerStore().getAll(keys, transforms);
        if(!storeDef.hasRetentionPeriod())
            return results;

        for(ByteArray key: results.keySet()) {
            List<Versioned<byte[]>> filteredVals = filterExpiredEntries(key, results.get(key));
            // remove/update the entry for the key, depending on how much is
            // filtered
            if(!filteredVals.isEmpty())
                results.put(key, filteredVals);
            else
                results.remove(key);
        }
        return results;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<byte[]>> vals = getInnerStore().get(key, transforms);
        if(!storeDef.hasRetentionPeriod())
            return vals;
        return filterExpiredEntries(key, vals);
    }
}

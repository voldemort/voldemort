package voldemort.store.routed;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.quota.QuotaExceededException;
import voldemort.utils.JmxUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;

/**
 * Tracks all the exceptions we see, down at the routing layer also including
 * ones that will be eventually propagated up to the client from the routing
 * layer
 * 
 */
public class PipelineRoutedStats {

    protected final ConcurrentHashMap<Class<? extends Exception>, AtomicLong> errCountMap;
    protected final AtomicLong severeExceptionCount;
    protected final AtomicLong benignExceptionCount;
    protected final String name;

    private boolean isRegistered = false;
    private ObjectName jmxObjectName = null;
    private int referenceCount;

    protected PipelineRoutedStats(String name) {
        this.name = Utils.notNull(name);
        errCountMap = new ConcurrentHashMap<Class<? extends Exception>, AtomicLong>();
        errCountMap.put(InvalidMetadataException.class, new AtomicLong(0));
        errCountMap.put(InsufficientOperationalNodesException.class, new AtomicLong(0));
        errCountMap.put(InsufficientZoneResponsesException.class, new AtomicLong(0));
        errCountMap.put(UnreachableStoreException.class, new AtomicLong(0));
        errCountMap.put(StoreTimeoutException.class, new AtomicLong(0));
        errCountMap.put(ObsoleteVersionException.class, new AtomicLong(0));
        errCountMap.put(QuotaExceededException.class, new AtomicLong(0));

        severeExceptionCount = new AtomicLong(0);
        benignExceptionCount = new AtomicLong(0);
        referenceCount = 0;
    }

    @JmxGetter(name = "numSevereExceptions", description = "Number of exceptions considered serious errors")
    public long getNumSevereExceptions() {
        return severeExceptionCount.get();
    }

    @JmxGetter(name = "numBenignExceptions", description = "Number of exceptions considered benign")
    public long getNumBenignExceptions() {
        return benignExceptionCount.get();
    }

    @JmxGetter(name = "numInsufficientOperationalNodesExceptions", description = "Number of client operations failed due to sufficient nodes not being up")
    public long getNumInsufficientOperationalNodesExceptions() {
        return errCountMap.get(InsufficientOperationalNodesException.class).get();
    }

    @JmxGetter(name = "numInsufficientZoneResponsesExceptions", description = "Number of client operations failed due to sufficient nodes not up across zones")
    public long getNumInsufficientZoneResponsesExceptions() {
        return errCountMap.get(InsufficientZoneResponsesException.class).get();
    }

    @JmxGetter(name = "numInvalidMetadataExceptions", description = "Number of times the metadata was invalid at the client")
    public long getNumInvalidMetadataExceptions() {
        return errCountMap.get(InvalidMetadataException.class).get();
    }

    @JmxGetter(name = "numUnreachableStoreExceptions", description = "Number of requests incomplete since some server could not be reached")
    public long getNumUnreachableStoreExceptions() {
        return errCountMap.get(UnreachableStoreException.class).get();
    }

    @JmxGetter(name = "numStoreTimeoutExceptions", description = "Number of requests timed out since some server was overloaded/unavailable")
    public long getNumStoreTimeoutExceptions() {
        return errCountMap.get(StoreTimeoutException.class).get();
    }

    @JmxGetter(name = "numObsoleteVersionExceptions", description = "Number of requests that got a ObsoleteVersionException as response")
    public long getNumObsoleteVersionExceptions() {
        return errCountMap.get(ObsoleteVersionException.class).get();
    }

    @JmxGetter(name = "numQuotaExceededExceptions", description = "Number of client operations failed due to exceeding quota")
    public long getNumQuotaExceededExceptions() {
        return errCountMap.get(QuotaExceededException.class).get();
    }

    @JmxGetter(name = "getExceptionCountsAsString", description = "Returns counts of all the Exceptions seen so far as a string")
    public String getExceptionCountsAsString() {
        StringBuilder result = new StringBuilder();
        Iterator<Entry<Class<? extends Exception>, AtomicLong>> itr = errCountMap.entrySet()
                                                                                 .iterator();
        while(itr.hasNext()) {
            Entry<Class<? extends Exception>, AtomicLong> pair = itr.next();
            result.append(pair.getKey().getName() + ":" + pair.getValue().get() + "\n");
        }
        return result.toString();
    }

    public void reportException(Exception e) {
        if(isSevere(e))
            severeExceptionCount.incrementAndGet();
        else
            benignExceptionCount.incrementAndGet();
        errCountMap.putIfAbsent(e.getClass(), new AtomicLong(0));
        errCountMap.get(e.getClass()).incrementAndGet();
    }

    public boolean isSevere(Exception ve) {
        if(ve instanceof InsufficientOperationalNodesException
           || ve instanceof InsufficientZoneResponsesException
           || ve instanceof InvalidMetadataException)
            return true;
        else
            return false;
    }

    /**
     * Multiple {@link PipelineRoutedStore} share the same
     * {@link PipelineRoutedStats} object IF Mbean is not registered, the first
     * caller will register it. All callers increment the counter.
     */
    public synchronized void registerJmxIfRequired() {
        referenceCount++;
        if(isRegistered == false) {
            String domain = JmxUtils.getPackageName(this.getClass());
            this.jmxObjectName = JmxUtils.createObjectName(domain, this.name);
            JmxUtils.registerMbean(this, this.jmxObjectName);
            isRegistered = true;
        }
    }

    /**
     * Last caller of this method will unregister the Mbean. All callers
     * decrement the counter.
     */
    public synchronized void unregisterJmxIfRequired() {
        referenceCount--;
        if (isRegistered == true && referenceCount <= 0) {
            JmxUtils.unregisterMbean(this.jmxObjectName);
            isRegistered = false;
        }
    }


}

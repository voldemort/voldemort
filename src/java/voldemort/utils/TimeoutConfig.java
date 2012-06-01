package voldemort.utils;

import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the timeouts for various voldemort operations
 * 
 */
public class TimeoutConfig {

    private long getTimeoutMs;

    private long putTimeoutMs;

    private long getAllTimeoutMs;

    private long deleteTimeoutMs;

    private long getVersionsTimeoutMs;

    private boolean partialGetAllAllowed;

    public TimeoutConfig(long globalTimeout, boolean allowPartialGetAlls) {
        this(globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             globalTimeout,
             allowPartialGetAlls);
    }

    public TimeoutConfig(long getTimeout,
                         long putTimeout,
                         long deleteTimeout,
                         long getAllTimeout,
                         long getVersionsTimeout,
                         boolean allowPartialGetAlls) {
        getTimeoutMs(getTimeout, TimeUnit.MILLISECONDS);
        putTimeoutMs(putTimeout, TimeUnit.MILLISECONDS);
        deleteTimeoutMs(deleteTimeout, TimeUnit.MILLISECONDS);
        getAllTimeoutMs(getAllTimeout, TimeUnit.MILLISECONDS);
        getVersionsTimeoutMs(getVersionsTimeout, TimeUnit.MILLISECONDS);
        setPartialGetAllAllowed(allowPartialGetAlls);
    }

    public long getTimeoutMs(TimeUnit unit) {
        return unit.convert(getTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public long getTimeoutMs() {
        return getTimeoutMs;
    }

    public void getTimeoutMs(long getTimeoutMs, TimeUnit unit) {
        this.getTimeoutMs = unit.toMillis(getTimeoutMs);
    }

    public long getVersionsTimeoutMs(TimeUnit unit) {
        return unit.convert(getVersionsTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public long getVersionsTimeoutMs() {
        return getVersionsTimeoutMs;
    }

    public void getVersionsTimeoutMs(long getTimeoutMs, TimeUnit unit) {
        this.getVersionsTimeoutMs = unit.toMillis(getTimeoutMs);
    }

    public long putTimeoutMs(TimeUnit unit) {
        return unit.convert(putTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public long putTimeoutMs() {
        return putTimeoutMs;
    }

    public void putTimeoutMs(long putTimeoutMs, TimeUnit unit) {
        this.putTimeoutMs = unit.toMillis(putTimeoutMs);
    }

    public long getAllTimeoutMs(TimeUnit unit) {
        return unit.convert(getAllTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public long getAllTimeoutMs() {
        return getAllTimeoutMs;
    }

    public void getAllTimeoutMs(long getAllTimeoutMs, TimeUnit unit) {
        this.getAllTimeoutMs = unit.toMillis(getAllTimeoutMs);
    }

    public long deleteTimeoutMs(TimeUnit unit) {
        return unit.convert(deleteTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public long deleteTimeoutMs() {
        return deleteTimeoutMs;
    }

    public void deleteTimeoutMs(long deleteTimeoutMs, TimeUnit unit) {
        this.deleteTimeoutMs = unit.toMillis(deleteTimeoutMs);
    }

    public boolean isPartialGetAllAllowed() {
        return partialGetAllAllowed;
    }

    public void setPartialGetAllAllowed(boolean allowPartialGetAlls) {
        this.partialGetAllAllowed = allowPartialGetAlls;
    }

}

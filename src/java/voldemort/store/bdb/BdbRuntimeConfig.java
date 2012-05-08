package voldemort.store.bdb;

import voldemort.server.VoldemortConfig;
import voldemort.utils.Time;

import com.sleepycat.je.LockMode;

/**
 * Runtime (i.e., post Environment creation) configuration for BdbStorageEngine
 * 
 */
public class BdbRuntimeConfig {

    public static final long DEFAULT_STATS_CACHE_TTL_MS = 5 * Time.MS_PER_SECOND;
    public static final LockMode DEFAULT_LOCK_MODE = LockMode.READ_UNCOMMITTED;
    public static final boolean DEFAULT_MINIMIZE_SCAN_IMPACT = false;

    private long statsCacheTtlMs = DEFAULT_STATS_CACHE_TTL_MS;
    private LockMode lockMode = DEFAULT_LOCK_MODE;
    private boolean minimizeScanImpact = DEFAULT_MINIMIZE_SCAN_IMPACT;

    public BdbRuntimeConfig() {

    }

    public BdbRuntimeConfig(VoldemortConfig config) {
        LockMode lockMode = config.getBdbReadUncommitted() ? LockMode.READ_UNCOMMITTED
                                                          : LockMode.DEFAULT;
        setLockMode(lockMode);
        setStatsCacheTtlMs(config.getBdbStatsCacheTtlMs());
        setMinimizeScanImpact(config.getBdbMinimizeScanImpact());
    }

    public long getStatsCacheTtlMs() {
        return statsCacheTtlMs;
    }

    public BdbRuntimeConfig setStatsCacheTtlMs(long statsCacheTtlMs) {
        this.statsCacheTtlMs = statsCacheTtlMs;
        return this;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public BdbRuntimeConfig setLockMode(LockMode lockMode) {
        this.lockMode = lockMode;
        return this;
    }

    public boolean getMinimizeScanImpact() {
        return minimizeScanImpact;
    }

    public void setMinimizeScanImpact(boolean minimizeScanImpact) {
        this.minimizeScanImpact = minimizeScanImpact;
    }
}

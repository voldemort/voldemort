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
    public static final boolean DEFAULT_EXPOSE_SPACE_UTIL = true;
    public static final boolean DEFAULT_MINIMIZE_SCAN_IMPACT = false;
    public static final boolean DEFAULT_TURNOFF_CHECKPOINTER_BATCH_WRITES = false;

    private long statsCacheTtlMs = DEFAULT_STATS_CACHE_TTL_MS;
    private LockMode lockMode = DEFAULT_LOCK_MODE;
    private boolean exposeSpaceUtil = DEFAULT_EXPOSE_SPACE_UTIL;
    private boolean minimizeScanImpact = DEFAULT_MINIMIZE_SCAN_IMPACT;
    private boolean checkpointerOffForBatchWrites = DEFAULT_TURNOFF_CHECKPOINTER_BATCH_WRITES;

    public BdbRuntimeConfig() {

    }

    public BdbRuntimeConfig(VoldemortConfig config) {
        LockMode lockMode = config.getBdbReadUncommitted() ? LockMode.READ_UNCOMMITTED
                                                          : LockMode.DEFAULT;
        setLockMode(lockMode);
        setStatsCacheTtlMs(config.getBdbStatsCacheTtlMs());
        setExposeSpaceUtil(config.getBdbExposeSpaceUtilization());
        setMinimizeScanImpact(config.getBdbMinimizeScanImpact());
        setCheckpointerOffForBatchWrites(config.getBdbCheckpointerOffForBatchWrites());
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

    public void setExposeSpaceUtil(boolean expose) {
        this.exposeSpaceUtil = expose;
    }

    public boolean getExposeSpaceUtil() {
        return this.exposeSpaceUtil;
    }

    public boolean getMinimizeScanImpact() {
        return minimizeScanImpact;
    }

    public void setMinimizeScanImpact(boolean minimizeScanImpact) {
        this.minimizeScanImpact = minimizeScanImpact;
    }

    public boolean isCheckpointerOffForBatchWrites() {
        return checkpointerOffForBatchWrites;
    }

    public void setCheckpointerOffForBatchWrites(boolean checkpointerOffForBulkWrites) {
        this.checkpointerOffForBatchWrites = checkpointerOffForBulkWrites;
    }

}

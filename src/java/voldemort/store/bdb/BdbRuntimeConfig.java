package voldemort.store.bdb;

import com.sleepycat.je.LockMode;
import voldemort.server.VoldemortConfig;
import voldemort.utils.Time;

/**
 * Runtime (i.e., post Environment creation) configuration for BdbStorageEngine
 *
 */
public class BdbRuntimeConfig {

    public static final long DEFAULT_STATS_CACHE_TTL_MS = 5 * Time.MS_PER_SECOND;
    public static final LockMode DEFAULT_LOCK_MODE = LockMode.READ_UNCOMMITTED;
    public static final boolean DEFAULT_CURSOR_PRELOAD = false;

    private long statsCacheTtlMs = DEFAULT_STATS_CACHE_TTL_MS;
    private LockMode lockMode = DEFAULT_LOCK_MODE;
    private boolean cursorPreload = DEFAULT_CURSOR_PRELOAD;

    public BdbRuntimeConfig() {

    }

    public BdbRuntimeConfig(VoldemortConfig config) {
        LockMode lockMode = config.getBdbReadUncommitted() ? LockMode.READ_UNCOMMITTED
                                                           : LockMode.DEFAULT;
        setLockMode(lockMode);
        setCursorPreload(config.getBdbCursorPreload());
        setStatsCacheTtlMs(config.getBdbStatsCacheTtlMs());
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

    public boolean getCursorPreload() {
        return cursorPreload;
    }

    public BdbRuntimeConfig setCursorPreload(boolean cursorPreload) {
        this.cursorPreload = cursorPreload;
        return this;
    }
}

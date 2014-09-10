package voldemort.store.bdb.stats;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.utils.CachedCallable;
import voldemort.utils.Utils;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.StatsConfig;

public class BdbEnvironmentStats {

    // Don't fetch entry count/btree stats more than twice a day
    private final static long INVASIVE_STATS_TTL_MS = 12 * 3600 * 1000;

    private final Environment environment;
    private final Database database;
    private final CachedCallable<EnvironmentStats> fastStats;
    private final CachedCallable<SpaceUtilizationStats> fastSpaceStats;
    private final CachedCallable<Long> entryCount;
    private final CachedCallable<DatabaseStats> btreeStats;
    private final boolean exposeSpaceStats;

    private final AtomicLong numExceptions;
    private final AtomicLong numLockTimeoutExceptions;
    private final AtomicLong numEnvironmentFailureExceptions;

    public BdbEnvironmentStats(Environment environment,
                               Database database,
                               long ttlMs,
                               boolean exposeSpaceUtil) {
        this.environment = environment;
        this.database = database;
        this.exposeSpaceStats = exposeSpaceUtil;
        Callable<EnvironmentStats> fastStatsCallable = new Callable<EnvironmentStats>() {

            public EnvironmentStats call() throws Exception {
                return getEnvironmentStats(true);
            }
        };
        fastStats = new CachedCallable<EnvironmentStats>(fastStatsCallable, ttlMs);

        Callable<SpaceUtilizationStats> fastDbStatsCallable = new Callable<SpaceUtilizationStats>() {

            public SpaceUtilizationStats call() throws Exception {
                return getSpaceUtilizationStats();
            }
        };
        fastSpaceStats = new CachedCallable<SpaceUtilizationStats>(fastDbStatsCallable, ttlMs);

        Callable<Long> entryCountCallable = new Callable<Long>() {

            public Long call() throws Exception {
                return getEntryCountUncached();
            }
        };
        entryCount = new CachedCallable<Long>(entryCountCallable, INVASIVE_STATS_TTL_MS);

        Callable<DatabaseStats> btreeStatsCallable = new Callable<DatabaseStats>() {

            public DatabaseStats call() throws Exception {
                return getBtreeStatsUncached();
            }
        };
        btreeStats = new CachedCallable<DatabaseStats>(btreeStatsCallable, INVASIVE_STATS_TTL_MS);

        numExceptions = new AtomicLong(0);
        numLockTimeoutExceptions = new AtomicLong(0);
        numEnvironmentFailureExceptions = new AtomicLong(0);
    }

    private EnvironmentStats getEnvironmentStats(boolean fast) {
        StatsConfig config = new StatsConfig();
        config.setFast(fast);
        return environment.getStats(config);
    }

    private SpaceUtilizationStats getSpaceUtilizationStats() {
        return new SpaceUtilizationStats(environment);
    }

    private SpaceUtilizationStats getFastSpaceUtilizationStats() {
        try {
            return fastSpaceStats.call();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    private EnvironmentStats getFastStats() {
        try {
            return fastStats.call();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    private Long getEntryCountUncached() {
        return database.count();
    }

    public DatabaseStats getBtreeStatsUncached() throws Exception {
        // fast stats does not provide detailed Btree structure.
        // This is invasive and will affect performance.
        return database.getStats(new StatsConfig().setFast(false));
    }

    public void reportException(DatabaseException de) {
        numExceptions.incrementAndGet();
        if(de instanceof LockTimeoutException) {
            numLockTimeoutExceptions.incrementAndGet();
        } else if(de instanceof EnvironmentFailureException) {
            numEnvironmentFailureExceptions.incrementAndGet();
        }
    }

    @JmxGetter(name = "FastStatsAsString")
    public String getFastStatsAsString() {
        return getFastStats().toString();
    }

    // 1. Caching

    @JmxGetter(name = "NumCacheMiss")
    public long getNumCacheMiss() {
        return getFastStats().getNCacheMiss();
    }

    @JmxGetter(name = "NumNotResident")
    public long getNumNotResident() {
        return getFastStats().getNNotResident();
    }

    @JmxGetter(name = "TotalCacheSize")
    public long getTotalCacheSize() {
        return getFastStats().getSharedCacheTotalBytes();
    }

    @JmxGetter(name = "AllotedCacheSize")
    public long getAllotedCacheSize() {
        return getFastStats().getCacheTotalBytes();
    }

    @JmxGetter(name = "EvictionPasses")
    public long getEvictedLNs() {
        return getFastStats().getNEvictPasses();
    }

    @JmxGetter(name = "BINFetches")
    public long getBINFetches() {
        return getFastStats().getNBINsFetch();
    }

    @JmxGetter(name = "BINFetchMisses")
    public long getBINFetchMisses() {
        return getFastStats().getNBINsFetchMiss();
    }

    @JmxGetter(name = "INFetches")
    public long getINFetches() {
        return getFastStats().getNUpperINsFetch();
    }

    @JmxGetter(name = "INFetchMisses")
    public long getINFetchMisses() {
        return getFastStats().getNUpperINsFetchMiss();
    }

    @JmxGetter(name = "LNFetches")
    public long getLNFetches() {
        return getFastStats().getNLNsFetch();
    }

    @JmxGetter(name = "LNFetchMisses")
    public long getLNFetchMisses() {
        return getFastStats().getNLNsFetchMiss();
    }

    @JmxGetter(name = "CachedBINs")
    public long getCachedBINs() {
        return getFastStats().getNCachedBINs();
    }

    @JmxGetter(name = "CachedINs")
    public long getCachedUpperINs() {
        return getFastStats().getNCachedUpperINs();
    }

    @JmxGetter(name = "EvictedBINs")
    public long getEvictedBINs() {
        EnvironmentStats stats = getFastStats();
        return stats.getNBINsEvictedCacheMode() + stats.getNBINsEvictedCritical()
               + stats.getNBINsEvictedDaemon() + stats.getNBINsEvictedManual();
    }

    @JmxGetter(name = "EvictedINs")
    public long getEvictedINs() {
        EnvironmentStats stats = getFastStats();
        return stats.getNUpperINsEvictedCacheMode() + stats.getNUpperINsEvictedCritical()
               + stats.getNUpperINsEvictedDaemon() + stats.getNUpperINsEvictedManual();
    }

    // 2. IO
    @JmxGetter(name = "NumRandomWrites")
    public long getNumRandomWrites() {
        return getFastStats().getNRandomWrites();
    }

    public long getNumRandomWriteBytes() {
        return getFastStats().getNRandomWriteBytes();
    }

    @JmxGetter(name = "NumRandomWriteKB")
    public long getNumRandomWriteKB() {
        return getNumRandomWriteBytes() / 1000;
    }

    @JmxGetter(name = "NumRandomReads")
    public long getNumRandomReads() {
        return getFastStats().getNRandomReads();
    }

    public long getNumRandomReadBytes() {
        return getFastStats().getNRandomReadBytes();
    }

    @JmxGetter(name = "NumRandomReadKB")
    public long getNumRandomReadKB() {
        return getNumRandomReadBytes() / 1000;
    }

    @JmxGetter(name = "NumSequentialWrites")
    public long getNumSequentialWrites() {
        return getFastStats().getNSequentialWrites();
    }

    public long getNumSequentialWriteBytes() {
        return getFastStats().getNSequentialWriteBytes();
    }

    @JmxGetter(name = "NumSequentialWriteKB")
    public long getNumSequentialWriteKB() {
        return getNumSequentialWriteBytes() / 1000;
    }

    @JmxGetter(name = "NumSequentialReads")
    public long getNumSequentialReads() {
        return getFastStats().getNSequentialReads();
    }

    public long getNumSequentialReadBytes() {
        return getFastStats().getNSequentialReadBytes();
    }

    @JmxGetter(name = "NumSequentialReadKB")
    public long getNumSequentialReadKB() {
        return getNumSequentialReadBytes() / 1000;
    }

    @JmxGetter(name = "NumFSyncs")
    public long getNumFSyncs() {
        return getFastStats().getNFSyncs();
    }

    // 3. Cleaning & Checkpointing

    @JmxGetter(name = "NumCleanerEntriesRead")
    public long getNumCleanerEntriesRead() {
        return getFastStats().getNCleanerEntriesRead();
    }

    @JmxGetter(name = "FileDeletionBacklog")
    public long getFileDeletionBacklog() {
        return getFastStats().getFileDeletionBacklog();
    }

    public long getFileDeletionBacklogBytes() {
        String logFileMaxStr = environment.getConfig()
                                          .getConfigParam(EnvironmentConfig.LOG_FILE_MAX);
        long logFileMax = Long.parseLong(logFileMaxStr);
        return getFileDeletionBacklog() * logFileMax;
    }

    @JmxGetter(name = "FileDeletionBacklogKB")
    public long getFileDeletionBacklogKB() {
        return getFileDeletionBacklogBytes() / 1000;
    }

    @JmxGetter(name = "CleanerBacklog")
    public long getCleanerBacklog() {
        return getFastStats().getCleanerBacklog();
    }

    @JmxGetter(name = "NumCleanerRuns")
    public long getNumCleanerRuns() {
        return getFastStats().getNCleanerRuns();
    }

    @JmxGetter(name = "NumCleanerDeletions")
    public long getNumCleanerDeletions() {
        return getFastStats().getNCleanerDeletions();
    }

    @JmxGetter(name = "NumCheckpoints")
    public long getNumCheckpoints() {
        return getFastStats().getNCheckpoints();
    }

    @JmxGetter(name = "TotalSpace")
    public long getTotalSpace() {
        if(this.exposeSpaceStats)
            return getFastSpaceUtilizationStats().getTotalSpaceUsed();
        else
            return 0;
    }

    @JmxGetter(name = "TotalSpaceUtilized")
    public long getTotalSpaceUtilized() {
        if(this.exposeSpaceStats)
            return getFastSpaceUtilizationStats().getTotalSpaceUtilized();
        else
            return 0;
    }

    @JmxGetter(name = "UtilizationSummary", description = "Displays the disk space utilization for an environment.")
    public String getUtilizationSummaryAsString() {
        return getFastSpaceUtilizationStats().getSummariesAsString();
    }

    // 4. Latching/Locking

    @JmxGetter(name = "BtreeLatches")
    public long getBtreeLatches() {
        return getFastStats().getRelatchesRequired();
    }

    @JmxGetter(name = "NumAcquiresWithContention")
    public long getNumAcquiresWithContention() {
        return getFastStats().getNAcquiresWithContention();
    }

    @JmxGetter(name = "NumAcquiresNoWaiters")
    public long getNumAcquiresNoWaiters() {
        return getFastStats().getNAcquiresNoWaiters();
    }

    // 5. Exceptions & general statistics
    @JmxGetter(name = "numExceptions")
    public long getNumExceptions() {
        return numExceptions.longValue();
    }

    @JmxGetter(name = "numLockTimeoutExceptions")
    public long getNumLockTimeoutExceptions() {
        return numLockTimeoutExceptions.longValue();
    }

    @JmxGetter(name = "numEnvironmentFailureExceptions")
    public long getNumEnvironmentFailureExceptions() {
        return numEnvironmentFailureExceptions.longValue();
    }

    @JmxOperation(description = "Obtain the number of k-v entries in the store")
    public long getEntryCount() throws Exception {
        return entryCount.call();
    }

    @JmxOperation(description = "Obtain statistics about the BTree Index for a store")
    public String getBtreeStats() throws Exception {
        return btreeStats.call().toString();
    }

    // Compound statistics derived from raw statistics

    @JmxGetter(name = "NumWritesTotal")
    public long getNumWritesTotal() {
        return getNumRandomWrites() + getNumSequentialWrites();
    }

    public long getNumWriteBytesTotal() {
        return getNumSequentialWriteBytes() + getNumRandomWriteBytes();
    }

    @JmxGetter(name = "NumWriteKBTotal")
    public long getNumWriteKBTotal() {
        return getNumWriteBytesTotal() / 1000;
    }

    @JmxGetter(name = "NumReadsTotal")
    public long getNumReadsTotal() {
        return getNumRandomReads() + getNumSequentialReads();
    }

    public long getNumReadBytesTotal() {
        return getNumRandomReadBytes() + getNumSequentialReadBytes();
    }

    @JmxGetter(name = "NumReadKBTotal")
    public long getNumReadKBTotal() {
        return getNumReadBytesTotal() / 1000;
    }

    @JmxGetter(name = "PercentRandomWrites")
    public double getPercentRandomWrites() {
        return Utils.safeGetPercentage(getNumRandomWrites(), getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageRandomWriteBytes")
    public double getPercentageRandomWriteBytes() {
        return Utils.safeGetPercentage(getNumRandomWriteBytes(), getNumWriteBytesTotal());
    }

    @JmxGetter(name = "PercentageRandomReads")
    public double getPercentageRandomReads() {
        return Utils.safeGetPercentage(getNumRandomReads(), getNumReadsTotal());
    }

    @JmxGetter(name = "PercentageRandomReadBytes")
    public double getPercentageRandomReadBytes() {
        return Utils.safeGetPercentage(getNumRandomReadBytes(), getNumReadBytesTotal());
    }

    @JmxGetter(name = "PercentageReads")
    public double getPercentageReads() {
        return Utils.safeGetPercentage(getNumReadsTotal(), getNumReadsTotal() + getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageReadBytes")
    public double getPercentageReadBytes() {
        return Utils.safeGetPercentage(getNumReadBytesTotal(), getNumWriteBytesTotal()
                                                               + getNumReadBytesTotal());
    }

    @JmxGetter(name = "PercentageCacheHits")
    public double getPercentageCacheHits() {
        return 1.0d - getPercentageCacheMisses();
    }

    @JmxGetter(name = "PercentageCacheMisses")
    public double getPercentageCacheMisses() {
        return Utils.safeGetPercentage(getNumCacheMiss(), getNumReadsTotal() + getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageContended")
    public double getPercentageContended() {
        return Utils.safeGetPercentage(getNumAcquiresWithContention(),
                                       getNumAcquiresWithContention() + getNumAcquiresNoWaiters());
    }

    @JmxGetter(name = "PercentageUtilization")
    public double getPercentageUtilization() {
        return Utils.safeGetPercentage(getTotalSpaceUtilized(), getTotalSpace());
    }

    @JmxGetter(name = "PercentageBINMiss")
    public double getPercentageBINMiss() {
        return Utils.safeGetPercentage(getBINFetchMisses(), getBINFetches());
    }

    @JmxGetter(name = "PercentageINMiss")
    public double getPercentageINMiss() {
        return Utils.safeGetPercentage(getINFetchMisses(), getINFetches());
    }

    @JmxGetter(name = "PercentageLNMiss")
    public double getPercentageLNMiss() {
        return Utils.safeGetPercentage(getLNFetchMisses(), getLNFetches());
    }
}

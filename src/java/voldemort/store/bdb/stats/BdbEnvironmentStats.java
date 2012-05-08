package voldemort.store.bdb.stats;

import java.util.concurrent.Callable;

import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.CachedCallable;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

public class BdbEnvironmentStats {

    private final Environment environment;
    private final CachedCallable<EnvironmentStats> fastStats;
    private final CachedCallable<SpaceUtilizationStats> fastSpaceStats;

    public BdbEnvironmentStats(Environment environment, long ttlMs) {
        this.environment = environment;
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

    @JmxGetter(name = "EvictionPasses")
    public long getEvictedLNs() {
        return getFastStats().getNEvictPasses();
    }

    // 2. IO
    @JmxGetter(name = "NumRandomWrites")
    public long getNumRandomWrites() {
        return getFastStats().getNRandomWrites();
    }

    @JmxGetter(name = "NumRandomWriteBytes")
    public long getNumRandomWriteBytes() {
        return getFastStats().getNRandomWriteBytes();
    }

    @JmxGetter(name = "NumRandomReads")
    public long getNumRandomReads() {
        return getFastStats().getNRandomReads();
    }

    @JmxGetter(name = "NumRandomReadBytes")
    public long getNumRandomReadBytes() {
        return getFastStats().getNRandomReadBytes();
    }

    @JmxGetter(name = "NumSequentialWrites")
    public long getNumSequentialWrites() {
        return getFastStats().getNSequentialWrites();
    }

    @JmxGetter(name = "NumSequentialWriteBytes")
    public long getNumSequentialWriteBytes() {
        return getFastStats().getNSequentialWriteBytes();
    }

    @JmxGetter(name = "NumSequentialReads")
    public long getNumSequentialReads() {
        return getFastStats().getNSequentialReads();
    }

    @JmxGetter(name = "NumSequentialReadBytes")
    public long getNumSequentialReadBytes() {
        return getFastStats().getNSequentialReadBytes();
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

    @JmxGetter(name = "FileDeletionBacklogBytes")
    public long getFileDeletionBacklogBytes() {
        String logFileMaxStr = environment.getConfig()
                                          .getConfigParam(EnvironmentConfig.LOG_FILE_MAX);
        long logFileMax = Long.parseLong(logFileMaxStr);
        return getFileDeletionBacklog() * logFileMax;
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
        return getFastStats().getNCleanerRuns();
    }

    @JmxGetter(name = "NumCheckpoints")
    public long getNumCheckpoints() {
        return getFastStats().getNCheckpoints();
    }

    @JmxGetter(name = "TotalSpace")
    public long getTotalSpace() {
        return getFastSpaceUtilizationStats().getTotalSpaceUsed();
    }

    @JmxGetter(name = "TotalSpaceUtilized")
    public long getTotalSpaceUtilized() {
        return getFastSpaceUtilizationStats().getTotalSpaceUtilized();
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

    // Compound statistics derived from raw statistics

    @JmxGetter(name = "NumWritesTotal")
    public long getNumWritesTotal() {
        return getNumRandomWrites() + getNumSequentialWrites();
    }

    @JmxGetter(name = "NumWriteBytesTotal")
    public long getNumWriteBytesTotal() {
        return getNumSequentialWriteBytes() + getNumRandomWriteBytes();
    }

    @JmxGetter(name = "PercentRandomWrites")
    public double getPercentRandomWrites() {
        return safeGetPercentage(getNumRandomWrites(), getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageRandomWriteBytes")
    public double getPercentageRandomWriteBytes() {
        return safeGetPercentage(getNumRandomWriteBytes(), getNumRandomWriteBytes()
                                                           + getNumSequentialWriteBytes());
    }

    @JmxGetter(name = "NumReadsTotal")
    public long getNumReadsTotal() {
        return getNumRandomReads() + getNumSequentialReads();
    }

    @JmxGetter(name = "NumReadBytesTotal")
    public long getNumReadBytesTotal() {
        return getNumRandomReadBytes() + getNumSequentialReadBytes();
    }

    @JmxGetter(name = "PercentageRandomReads")
    public double getPercentageRandomReads() {
        return safeGetPercentage(getNumRandomReads(), getNumReadsTotal());
    }

    @JmxGetter(name = "PercentageRandomReadBytes")
    public double getPercentageRandomReadBytes() {
        return safeGetPercentage(getNumRandomWriteBytes(), getNumRandomReadBytes()
                                                           + getNumSequentialReadBytes());
    }

    @JmxGetter(name = "PercentageReads")
    public double getPercentageReads() {
        return safeGetPercentage(getNumReadsTotal(), getNumReadsTotal() + getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageReadBytes")
    public double getPercentageReadBytes() {
        return safeGetPercentage(getNumReadBytesTotal(), getNumWriteBytesTotal()
                                                         + getNumReadBytesTotal());
    }

    @Experimental
    @JmxGetter(name = "PercentageCacheHits")
    public double getPercentageCacheHits() {
        return 1.0d - getPercentageCacheMisses();
    }

    @Experimental
    @JmxGetter(name = "PercentageCacheMisses")
    public double getPercentageCacheMisses() {
        return safeGetPercentage(getNumCacheMiss(), getNumReadsTotal() + getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageContended")
    public double getPercentageContended() {
        return safeGetPercentage(getNumAcquiresWithContention(), getNumAcquiresWithContention()
                                                                 + getNumAcquiresNoWaiters());
    }

    @JmxGetter(name = "PercentageBINMiss")
    public double getPercentageBINMiss() {
        return safeGetPercentage(getBINFetchMisses(), getBINFetches());
    }

    @JmxGetter(name = "PercentageINMiss")
    public double getPercentageINMiss() {
        return safeGetPercentage(getINFetchMisses(), getINFetches());
    }

    @JmxGetter(name = "PercentageLNMiss")
    public double getPercentageLNMiss() {
        return safeGetPercentage(getLNFetchMisses(), getLNFetches());
    }

    @JmxGetter(name = "PercentageUtilization")
    public double getPercentageUtilization() {
        return safeGetPercentage(getTotalSpaceUtilized(), getTotalSpace());
    }

    public static double safeGetPercentage(long rawNum, long total) {
        return total == 0 ? 0.0d : rawNum / (float) total;
    }
}

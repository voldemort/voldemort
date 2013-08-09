package voldemort.store.bdb.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

/**
 * Aggregated statistics about all the bdb environments on this server.
 * 
 */
public class AggregatedBdbEnvironmentStats {

    List<BdbEnvironmentStats> environmentStatsTracked;

    public AggregatedBdbEnvironmentStats() {
        environmentStatsTracked = Collections.synchronizedList(new ArrayList<BdbEnvironmentStats>());
    }

    public void trackEnvironment(BdbEnvironmentStats stats) {
        environmentStatsTracked.add(stats);
    }

    /**
     * Calls the provided metric getter on all the tracked environments and
     * obtains their values
     * 
     * @param metricGetterName
     * @return
     */
    private List<Long> collectLongMetric(String metricGetterName) {
        List<Long> vals = new ArrayList<Long>();
        for(BdbEnvironmentStats envStats: environmentStatsTracked) {
            vals.add((Long) ReflectUtils.callMethod(envStats,
                                                    BdbEnvironmentStats.class,
                                                    metricGetterName,
                                                    new Class<?>[0],
                                                    new Object[0]));
        }
        return vals;
    }

    // 1. Caching
    @JmxGetter(name = "NumCacheMiss")
    public long getNumCacheMiss() {
        return Utils.sumLongList(collectLongMetric("getNumCacheMiss"));
    }

    @JmxGetter(name = "AllotedCacheSize")
    public long getAllotedCacheSize() {
        return Utils.sumLongList(collectLongMetric("getAllotedCacheSize"));
    }

    @JmxGetter(name = "EvictionPasses")
    public long getEvictedLNs() {
        return Utils.sumLongList(collectLongMetric("getEvictedLNs"));
    }

    @JmxGetter(name = "BINFetches")
    public long getBINFetches() {
        return Utils.sumLongList(collectLongMetric("getBINFetches"));
    }

    @JmxGetter(name = "BINFetchMisses")
    public long getBINFetchMisses() {
        return Utils.sumLongList(collectLongMetric("getBINFetchMisses"));
    }

    @JmxGetter(name = "INFetches")
    public long getINFetches() {
        return Utils.sumLongList(collectLongMetric("getINFetches"));
    }

    @JmxGetter(name = "INFetchMisses")
    public long getINFetchMisses() {
        return Utils.sumLongList(collectLongMetric("getINFetchMisses"));

    }

    @JmxGetter(name = "LNFetches")
    public long getLNFetches() {
        return Utils.sumLongList(collectLongMetric("getLNFetches"));
    }

    @JmxGetter(name = "LNFetchMisses")
    public long getLNFetchMisses() {
        return Utils.sumLongList(collectLongMetric("getLNFetchMisses"));
    }

    @JmxGetter(name = "CachedBINs")
    public long getCachedBINs() {
        return Utils.sumLongList(collectLongMetric("getCachedBINs"));
    }

    @JmxGetter(name = "CachedINs")
    public long getCachedUpperINs() {
        return Utils.sumLongList(collectLongMetric("getCachedUpperINs"));
    }

    @JmxGetter(name = "EvictedBINs")
    public long getEvictedBINs() {
        return Utils.sumLongList(collectLongMetric("getEvictedBINs"));
    }

    @JmxGetter(name = "EvictedINs")
    public long getEvictedINs() {
        return Utils.sumLongList(collectLongMetric("getEvictedINs"));
    }

    // 2. IO
    @JmxGetter(name = "NumRandomWrites")
    public long getNumRandomWrites() {
        return Utils.sumLongList(collectLongMetric("getNumRandomWrites"));
    }

    @JmxGetter(name = "NumRandomWriteBytes")
    public long getNumRandomWriteBytes() {
        return Utils.sumLongList(collectLongMetric("getNumRandomWriteBytes"));
    }

    @JmxGetter(name = "NumRandomReads")
    public long getNumRandomReads() {
        return Utils.sumLongList(collectLongMetric("getNumRandomReads"));
    }

    @JmxGetter(name = "NumRandomReadBytes")
    public long getNumRandomReadBytes() {
        return Utils.sumLongList(collectLongMetric("getNumRandomReadBytes"));
    }

    @JmxGetter(name = "NumSequentialWrites")
    public long getNumSequentialWrites() {
        return Utils.sumLongList(collectLongMetric("getNumSequentialWrites"));
    }

    @JmxGetter(name = "NumSequentialWriteBytes")
    public long getNumSequentialWriteBytes() {
        return Utils.sumLongList(collectLongMetric("getNumSequentialWriteBytes"));
    }

    @JmxGetter(name = "NumSequentialReads")
    public long getNumSequentialReads() {
        return Utils.sumLongList(collectLongMetric("getNumSequentialReads"));
    }

    @JmxGetter(name = "NumSequentialReadBytes")
    public long getNumSequentialReadBytes() {
        return Utils.sumLongList(collectLongMetric("getNumSequentialReadBytes"));
    }

    @JmxGetter(name = "NumFSyncs")
    public long getNumFSyncs() {
        return Utils.sumLongList(collectLongMetric("getNumFSyncs"));
    }

    // 3. Cleaning & Checkpointing

    @JmxGetter(name = "NumCleanerEntriesRead")
    public long getNumCleanerEntriesRead() {
        return Utils.sumLongList(collectLongMetric("getNumCleanerEntriesRead"));
    }

    @JmxGetter(name = "FileDeletionBacklog")
    public long getFileDeletionBacklog() {
        return Utils.sumLongList(collectLongMetric("getFileDeletionBacklog"));
    }

    @JmxGetter(name = "FileDeletionBacklogBytes")
    public long getFileDeletionBacklogBytes() {
        return Utils.sumLongList(collectLongMetric("getFileDeletionBacklogBytes"));
    }

    @JmxGetter(name = "CleanerBacklog")
    public long getCleanerBacklog() {
        return Utils.sumLongList(collectLongMetric("getCleanerBacklog"));
    }

    @JmxGetter(name = "NumCleanerRuns")
    public long getNumCleanerRuns() {
        return Utils.sumLongList(collectLongMetric("getNumCleanerRuns"));
    }

    @JmxGetter(name = "NumCleanerDeletions")
    public long getNumCleanerDeletions() {
        return Utils.sumLongList(collectLongMetric("getNumCleanerDeletions"));
    }

    @JmxGetter(name = "NumCheckpoints")
    public long getNumCheckpoints() {
        return Utils.sumLongList(collectLongMetric("getNumCheckpoints"));
    }

    @JmxGetter(name = "TotalSpace")
    public long getTotalSpace() {
        return Utils.sumLongList(collectLongMetric("getTotalSpace"));
    }

    @JmxGetter(name = "TotalSpaceUtilized")
    public long getTotalSpaceUtilized() {
        return Utils.sumLongList(collectLongMetric("getTotalSpaceUtilized"));
    }

    // 4. Latching/Locking

    @JmxGetter(name = "BtreeLatches")
    public long getBtreeLatches() {
        return Utils.sumLongList(collectLongMetric("getBtreeLatches"));
    }

    @JmxGetter(name = "NumAcquiresWithContention")
    public long getNumAcquiresWithContention() {
        return Utils.sumLongList(collectLongMetric("getNumAcquiresWithContention"));
    }

    @JmxGetter(name = "NumAcquiresNoWaiters")
    public long getNumAcquiresNoWaiters() {
        return Utils.sumLongList(collectLongMetric("getNumAcquiresNoWaiters"));
    }

    // 5. Exceptions & general statistics
    @JmxGetter(name = "numExceptions")
    public long getNumExceptions() {
        return Utils.sumLongList(collectLongMetric("getNumExceptions"));
    }

    @JmxGetter(name = "numLockTimeoutExceptions")
    public long getNumLockTimeoutExceptions() {
        return Utils.sumLongList(collectLongMetric("getNumLockTimeoutExceptions"));
    }

    @JmxGetter(name = "numEnvironmentFailureExceptions")
    public long getNumEnvironmentFailureExceptions() {
        return Utils.sumLongList(collectLongMetric("getNumEnvironmentFailureExceptions"));
    }

    // Compound statistics derived from raw statistics
    @JmxGetter(name = "NumWritesTotal")
    public long getNumWritesTotal() {
        return Utils.sumLongList(collectLongMetric("getNumWritesTotal"));
    }

    @JmxGetter(name = "NumWriteBytesTotal")
    public long getNumWriteBytesTotal() {
        return Utils.sumLongList(collectLongMetric("getNumWriteBytesTotal"));
    }

    @JmxGetter(name = "NumReadsTotal")
    public long getNumReadsTotal() {
        return Utils.sumLongList(collectLongMetric("getNumReadsTotal"));
    }

    @JmxGetter(name = "NumReadBytesTotal")
    public long getNumReadBytesTotal() {
        return Utils.sumLongList(collectLongMetric("getNumReadBytesTotal"));
    }

    @JmxGetter(name = "PercentRandomWrites")
    public double getPercentRandomWrites() {
        return Utils.safeGetPercentage(getNumRandomWrites(), getNumWritesTotal());
    }

    @JmxGetter(name = "PercentageRandomWriteBytes")
    public double getPercentageRandomWriteBytes() {
        return Utils.safeGetPercentage(getNumRandomWriteBytes(), getNumRandomWriteBytes()
                                                                 + getNumSequentialWriteBytes());
    }

    @JmxGetter(name = "PercentageRandomReads")
    public double getPercentageRandomReads() {
        return Utils.safeGetPercentage(getNumRandomReads(), getNumReadsTotal());
    }

    @JmxGetter(name = "PercentageRandomReadBytes")
    public double getPercentageRandomReadBytes() {
        return Utils.safeGetPercentage(getNumRandomWriteBytes(), getNumRandomReadBytes()
                                                                 + getNumSequentialReadBytes());
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

package voldemort.store.bdb.stats;

import java.util.concurrent.Callable;

import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.utils.CachedCallable;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

public class BdbEnvironmentStats {

    private final Environment environment;
    private final CachedCallable<EnvironmentStats> fastStats;
    private final CachedCallable<DbSpace> fastDbStats;

    public BdbEnvironmentStats(Environment environment, long ttlMs) {
        this.environment = environment;
        Callable<EnvironmentStats> fastStatsCallable = new Callable<EnvironmentStats>() {

            public EnvironmentStats call() throws Exception {
                return getEnvironmentStats(true);
            }
        };
        fastStats = new CachedCallable<EnvironmentStats>(fastStatsCallable, ttlMs);

        Callable<DbSpace> fastDbStatsCallable = new Callable<DbSpace>() {

            public DbSpace call() throws Exception {
                return getBdbStats();
            }
        };
        fastDbStats = new CachedCallable<DbSpace>(fastDbStatsCallable, ttlMs);
    }

    private EnvironmentStats getEnvironmentStats(boolean fast) {
        StatsConfig config = new StatsConfig();
        config.setFast(fast);
        return environment.getStats(config);
    }

    private DbSpace getBdbStats() {
        return new DbSpace(environment);
    }

    private DbSpace getFastDbStats() {
        try {
            return fastDbStats.call();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    @JmxGetter(name = "BdbTotalSize")
    public long getBdbTotalSize() {
        return getFastDbStats().getTotal().totalSize();
    }

    @JmxGetter(name = "BdbTotalUsage")
    public long getBdbTotalUsage() {
        return getFastDbStats().getTotal().utilization();
    }

    @JmxOperation(description = "Displays the disk space utilization for an environment.")
    public String getBdbSummariesAsString() {
        return getFastDbStats().getSummariesAsString();
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

    @JmxGetter(name = "NumCacheMiss")
    public long getNumCacheMiss() {
        return getFastStats().getNCacheMiss();
    }

    @JmxGetter(name = "NumNotResident")
    public long getNumNotResident() {
        return getFastStats().getNNotResident();
    }

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

    @JmxGetter(name = "NumAcquiresWithContention")
    public long getNumAcquiresWithContention() {
        return getFastStats().getNAcquiresWithContention();
    }

    @JmxGetter(name = "NumAcquiresNoWaiters")
    public long getNumAcquiresNoWaiters() {
        return getFastStats().getNAcquiresNoWaiters();
    }

    @JmxGetter(name = "NumCheckpoints")
    public long getNumCheckpoints() {
        return getFastStats().getNCheckpoints();
    }

    @JmxGetter(name = "NumCleanerEntriesRead")
    public long getNumCleanerEntriesRead() {
        return getFastStats().getNCleanerEntriesRead();
    }

    @JmxGetter(name = "NumFSyncs")
    public long getNumFSyncs() {
        return getFastStats().getNFSyncs();
    }

    @JmxGetter(name = "NumCleanerRuns")
    public long getNumCleanerRuns() {
        return getFastStats().getNCleanerRuns();
    }

    @JmxGetter(name = "NumCleanerDeletions")
    public long getNumCleanerDeletions() {
        return getFastStats().getNCleanerRuns();
    }

    // Compound statistics

    @JmxGetter(name = "NumWritesTotal")
    public long getNumWritesTotal() {
        return getNumRandomWrites() + getNumSequentialWrites();
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

    @JmxGetter(name = "PercentageRandomReads")
    public double getPercentageRandomReads() {
        return safeGetPercentage(getNumRandomReads(), getNumReadsTotal());
    }

    @JmxGetter(name = "PercentageRandomReadBytes")
    public double getPercentageRandomReadBytes() {
        return safeGetPercentage(getNumRandomWriteBytes(), getNumRandomReadBytes()
                                                           + getNumSequentialReadBytes());
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

    public static double safeGetPercentage(long rawNum, long total) {
        return total == 0 ? 0.0d : rawNum / (float) total;
    }
}

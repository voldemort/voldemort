package voldemort.store.bdb.stats;


import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import voldemort.VoldemortException;
import voldemort.utils.CachedCallable;

import java.util.concurrent.Callable;

// TODO: pass this to a script to add JMX annotations and generate BdbEnvironmentStatsJmx
public class BdbEnvironmentStats {

    private final Environment environment;
    private final CachedCallable<EnvironmentStats> fastStats;

    public BdbEnvironmentStats(Environment environment, long ttlMs) {
        this.environment = environment;
        Callable<EnvironmentStats> fastStatsCallable = new Callable<EnvironmentStats>() {

            public EnvironmentStats call() throws Exception {
                return getEnvironmentStats(true);
            }
        };
        fastStats = new CachedCallable<EnvironmentStats>(fastStatsCallable, ttlMs);
    }

    private EnvironmentStats getEnvironmentStats(boolean fast) {
        StatsConfig config = new StatsConfig();
        config.setFast(fast);
        return environment.getStats(config);
    }

    private EnvironmentStats getFastStats() {
        try {
            return fastStats.call();
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    public String getFastStatsAsString() {
        return getFastStats().toString();
    }

    public long getNumNotResident() {
        return getFastStats().getNNotResident();
    }

    public long getNumRandomWrites() {
        return getFastStats().getNRandomWrites();
    }

    public long getNumRandomWriteBytes() {
        return getFastStats().getNRandomWriteBytes();
    }

    public long getNumRandomReads() {
        return getFastStats().getNRandomReads();
    }

    public long getNumRandomReadBytes() {
        return getFastStats().getNRandomReadBytes();
    }

    public long getNumSequentialWrites() {
        return getFastStats().getNSequentialWrites();
    }

    public long getNumSequentialWriteBytes() {
        return getFastStats().getNSequentialWriteBytes();
    }

    public long getNumSequentialReads() {
        return getFastStats().getNSequentialReads();
    }

    public long getNumSequentialReadBytes() {
        return getFastStats().getNSequentialReadBytes();
    }

    public long getFileDeletionBacklog() {
        return getFastStats().getFileDeletionBacklog();
    }

    public long getFileDeletionBacklogBytes() {
        String logFileMaxStr = environment.getConfig()
                                          .getConfigParam(EnvironmentConfig.LOG_FILE_MAX);
        long logFileMax = Long.parseLong(logFileMaxStr);
        return getFileDeletionBacklog() * logFileMax;
    }

    public long getCleanerBacklog() {
        return getFastStats().getCleanerBacklog();
    }

    public long getNumAcquiredWithContention() {
        return getFastStats().getNAcquiresWithContention();
    }

    public long getNumAcquireNoWaiters() {
        return getFastStats().getNAcquiresNoWaiters();
    }

    public long getNumCheckpoints() {
        return getFastStats().getNCheckpoints();
    }

    public long getNumCleanerEntriesRead() {
        return getFastStats().getNCleanerEntriesRead();
    }

    public long getNumFSyncs() {
        return getFastStats().getNFSyncs();
    }

    public long getNumCleanerRuns() {
        return getFastStats().getNCleanerRuns();
    }

    public long getNumCleanerDeletions() {
        return getFastStats().getNCleanerRuns();
    }

    // Compound statistics

    public double getPercentRandomWrites() {
       return safeGetPercentage(getNumRandomWrites(), getNumRandomWrites() + getNumSequentialWrites());
    }

    public double getPercentageRandomWriteBytes() {
        return safeGetPercentage(getNumRandomWriteBytes(), getNumRandomWriteBytes() +
                                                           getNumSequentialWriteBytes());
    }

    public double getPercentageRandomReads() {
        return safeGetPercentage(getNumRandomReads(), getNumRandomReads() + getNumSequentialReads());
    }

    public double getPercentageRandomReadBytes() {
        return safeGetPercentage(getNumRandomWriteBytes(), getNumRandomReadBytes() +
                                                           getNumSequentialReadBytes());
    }

    public static double safeGetPercentage(long rawNum, long total) {
        return total == 0 ? 0.0d : rawNum / (float)total;
    }
}

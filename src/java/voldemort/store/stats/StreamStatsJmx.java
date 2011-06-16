package voldemort.store.stats;

import org.apache.log4j.Logger;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;

@JmxManaged(description = "Streaming related statistics")
public class StreamStatsJmx {
    private final static Logger logger = Logger.getLogger(StreamStatsJmx.class);

    private final StreamStats stats;

    public StreamStatsJmx(StreamStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "streamOperationIds", description = "Get a list of all stream operations.")
    public String getStreamOperationIds() {
        try {
            return stats.getHandleIds().toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }

    @JmxGetter(name = "allStreamOperations", description = "Get status of all stream operations.")
    public String getAllStreamOperations() {
        try {
            return stats.getHandles().toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }

    @JmxOperation(description = "Get the status of a stream operation with specified id.")
    public String getStreamOperation(long handleId) {
        try {
            return stats.getHandle(handleId).toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }

    @JmxOperation(description = "Clear out finished tasks.")
    public void clearFinished() {
        stats.clearFinished();
    }

    // Disk statistics

    @JmxGetter(name = "averageFetchKeysDiskTimeMs", description = "The avg. disk time in ms per FETCH_KEYS operation.")
    public double getAvgFetchKeysDiskTimeMs() {
        return stats.getDiskCounter(StreamStats.Operation.FETCH_KEYS).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageFetchEntriesDiskTimeMs", description = "The avg. disk time in ms per FETCH_ENTRIES operation.")
    public double getAvgFetchEntriesDiskTimeMs() {
        return stats.getDiskCounter(StreamStats.Operation.FETCH_ENTRIES).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageFetchFileDiskTimeMs", description = "The avg. disk time in ms per FETCH_FILE operation.")
    public double getAvgFetchFileDiskTimeMs() {
        return stats.getDiskCounter(StreamStats.Operation.FETCH_FILE).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageUpdateDiskTimeMs", description = "The avg. disk time in ms per UPDATE operation.")
    public double getAvgUpdateDiskTimeMs() {
        return stats.getDiskCounter(StreamStats.Operation.UPDATE).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageSlopDiskTimeMs", description = "The avg. disk time in ms per UPDATE_SLOP operation.")
    public double getAvgSlopDiskTimeMs() {
        return stats.getDiskCounter(StreamStats.Operation.SLOP).getAverageTimeInMs();
    }


    // Network statistics

    @JmxGetter(name = "averageFetchKeysNetworkTimeMs", description = "The avg. network time in ms per FETCH_KEYS operation.")
    public double getAvgFetchKeysNetworkTimeMs() {
        return stats.getNetworkCounter(StreamStats.Operation.FETCH_KEYS).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageFetchEntriesNetworkTimeMs", description = "The avg. network time in ms per FETCH_ENTRIES operation.")
    public double getAvgFetchEntriesNetworkTimeMs() {
        return stats.getNetworkCounter(StreamStats.Operation.FETCH_ENTRIES).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageFetchFileNetworkTimeMs", description = "The avg. network time in ms per FETCH_FILE operation.")
    public double getAvgFetchFileNetworkTimeMs() {
        return stats.getNetworkCounter(StreamStats.Operation.FETCH_FILE).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageUpdateNetworkTimeMs", description = "The avg. network time in ms per UPDATE operation.")
    public double getAvgUpdateNetworkTimeMs() {
        return stats.getNetworkCounter(StreamStats.Operation.UPDATE).getAverageTimeInMs();
    }

    @JmxGetter(name = "averageSlopNetworkTimeMs", description = "The avg. network time in ms per UPDATE_SLOP operation.")
    public double getAvgSlopNetworkTimeMs() {
        return stats.getNetworkCounter(StreamStats.Operation.SLOP).getAverageTimeInMs();
    }
}

package voldemort.store.readonly.fetcher;

import org.apache.log4j.Logger;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.JmxUtils;

import java.util.HashMap;
import java.util.Map;

/*
 * This class is used to capture the aggregated metrics for all the file pulls from Hdfs,
 * and expose them through JMX Bean.
 */
public class HdfsFetcherAggStats {
    // A singleton for all the data pull tasks
    private static final HdfsFetcherAggStats stats;

    private static Logger logger = Logger.getLogger(HdfsFetcherAggStats.class);

    // Total bytes fetched by all the data pull tasks
    private long totalBytesFetched;
    // Total Hdfs fetch retry number
    private long totalFetchRetries;
    // Total checksum failures
    private long totalCheckSumFailures;
    // Total file read failures
    private long totalFileReadFailures;
    // Total authentication failures
    private long totalAuthenticationFailures;
    // Total file not found failures
    private long totalFileNotFoundFailures;
    // Total quota exceed failures
    private long totalQuotaExceedFailures;
    // Total unauthorized store failures
    private long totalUnauthorizedStoreFailures;

    // Parallel active fetch number
    // This metric is only used to capture the active fetches number right now
    private int parallelFetches;
    // Total Hdfs fetch number
    private long totalFetches;

    // TODO: Max number of parallel fetches (if there are more than one running at the same time)
    // Need to use Tehuti lib

    // Total number for incomplete fetches
    private long totalIncompleteFetches;

    // TODO: File fetcher average and max connection times (NameNode and DataNode)
    // Not sure how to extract those metrics

    // Maintain the mapping between store name and its corresponding HdfsCopyStats,
    // so that we can get the aggregated transfer rates for all the stores
    private Map<String, HdfsCopyStats> copyStatsMap = new HashMap<String, HdfsCopyStats>();

    // Register the global aggregated stats object in Bean Server
    static {
        stats = new HdfsFetcherAggStats();
        JmxUtils.registerMbean("hdfs-fetcher-agg-stats", stats);
    }

    private HdfsFetcherAggStats() {}

    public static HdfsFetcherAggStats getStats() {
        return stats;
    }

    public synchronized void recordBytesTransferred(long bytesTransferred) {
        totalBytesFetched += bytesTransferred;
    }

    public synchronized void storeFetch() {
        ++totalFetches;
    }
    public synchronized void singleFileFetchStart(boolean isRetry) {
        ++parallelFetches;
        if (isRetry) {
            ++totalFetchRetries;
        }
    }

    public synchronized void singleFileFetchEnd() {
        --parallelFetches;
    }

    public synchronized void checkSumFailed() {
        ++totalCheckSumFailures;
    }

    public synchronized void authenticateFailed() {
        ++totalAuthenticationFailures;
    }

    public synchronized void fileNotFound() {
        ++totalFileNotFoundFailures;
    }

    public synchronized void fileReadFailed() {
        ++totalFileReadFailures;
    }

    public synchronized void quotaCheckFailed() {
        ++totalQuotaExceedFailures;
    }

    public synchronized void unauthorizedStorePush() {
        ++totalUnauthorizedStoreFailures;
    }

    public synchronized void incompleteFetch() {
        ++totalIncompleteFetches;
    }

    public synchronized void addStoreCopyStats(String storeName, HdfsCopyStats copyStats) {
        copyStatsMap.put(storeName, copyStats);
    }

    public synchronized void removeStoreCopyStats(String storeName) {
        copyStatsMap.remove(storeName);
    }

    public synchronized HdfsCopyStats getStoreCopyStatus(String storeName) {
        return copyStatsMap.get(storeName);
    }

    @JmxGetter(name = "totalBytesFetched", description = "The total bytes transferred from HDFS so far.")
    public synchronized long getTotalBytesFetched() {
        return totalBytesFetched;
    }

    @JmxGetter(name = "totalFetchRetries", description = "The total fetch retry number so far.")
    public synchronized long getTotalFetchRetries() {
        return totalFetchRetries;
    }

    @JmxGetter(name = "totalCheckSumFailures", description = "The total data file checksum failures happened so far.")
    public synchronized long getTotalCheckSumFailures() {
        return totalCheckSumFailures;
    }

    @JmxGetter(name = "totalAuthenticationFailures", description = "The total authentication failures happened so far.")
    public synchronized long getTotalAuthenticationFailures() {
        return totalAuthenticationFailures;
    }

    @JmxGetter(name = "totalFileNotFoundFailures", description = "The total file-not-found failures happened so far.")
    public synchronized long getTotalFileNotFoundFailures() {
        return totalFileNotFoundFailures;
    }

    @JmxGetter(name = "totalFileReadFailures", description = "The total HDFS file read failures happened so far.")
    public synchronized long getTotalFileReadFailures() {
        return totalFileReadFailures;
    }

    @JmxGetter(name = "totalQuotaExceedFailures", description = "The total quota exceed failures happened so far.")
    public synchronized long getTotalQuotaExceedFailures() {
        return totalQuotaExceedFailures;
    }

    @JmxGetter(name = "totalUnauthorizedStoreFailures", description = "The total unauthorized store push failures happened so far.")
    public synchronized long getTotalUnauthorizedStoreFailures() {
        return  totalUnauthorizedStoreFailures;
    }

    @JmxGetter(name = "parallelFetches", description = "The total number of active fetches right now.")
    public synchronized int getParallelFetches() {
        return parallelFetches;
    }

    @JmxGetter(name = "totalFetches", description = "The total HDFS fetch number so far.")
    public synchronized long getTotalFetches() {
        return totalFetches;
    }

    @JmxGetter(name = "totalIncompleteFetches", description = "The total incomplete fetch number so far.")
    public synchronized long getTotalIncompleteFetches() {
        return totalIncompleteFetches;
    }

    @JmxGetter(name = "totalDataFetchRate", description = "The total data fetch rate right now.")
    public synchronized double getTotalDataFetchRate() {
        return HdfsFetcher.getDataFetchRate();
        /*
        double fetchRate = 0;
        for (HdfsCopyStats copyStats : copyStatsMap.values()) {
            fetchRate += copyStats.getBytesTransferredPerSecond();
        }

        return fetchRate;
        */
    }
}

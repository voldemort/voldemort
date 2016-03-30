package voldemort.store.readonly.fetcher;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.stats.StoreStats;
import voldemort.utils.JmxUtils;

import java.util.concurrent.TimeUnit;

/*
 * This class is used to capture the aggregated metrics for all the file pulls from Hdfs,
 * and expose them through JMX Bean.
 */
public class HdfsFetcherAggStats {
    // A singleton for all the data pull tasks
    private static final HdfsFetcherAggStats stats;

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

    // Metrics repository for data transfer rate
    private static final MetricsRepository metricsRepository;
    private static final Sensor dataTransferRateSensor;
    private static final Metric dataTransferMetric;

    // Register the global aggregated stats object in Bean Server
    static {
        stats = new HdfsFetcherAggStats();
        JmxUtils.registerMbean("hdfs-fetcher-agg-stats", stats);

        // Metric repository
        metricsRepository = new MetricsRepository();
        MetricConfig dataTransferRateConfig = new MetricConfig().timeWindow(StoreStats.timeWindow, TimeUnit.MILLISECONDS);
        dataTransferRateSensor = metricsRepository.sensor("hdfs-fetcher-agg-data-transfer");
        Rate dataTransferStat = new Rate(TimeUnit.SECONDS);
        dataTransferMetric = dataTransferRateSensor.add("data-transfer.rate", dataTransferStat, dataTransferRateConfig);
    }

    private HdfsFetcherAggStats() {}

    public static HdfsFetcherAggStats getStats() {
        return stats;
    }

    public synchronized void recordBytesTransferred(long bytesTransferred) {
        totalBytesFetched += bytesTransferred;
        dataTransferRateSensor.record(bytesTransferred);
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
        return dataTransferMetric.value();
    }
}

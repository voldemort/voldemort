package voldemort.store.stats;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;

import com.google.common.collect.ImmutableList;

/**
 * A wrapper class to expose store stats via JMX
 * 
 * 
 */
@JmxManaged
public class StoreStatsJmx {

    private StoreStats stats;

    public StoreStatsJmx(StoreStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "numberOfCallsToGetAll", description = "The number of calls to GET_ALL since the last reset.")
    public long getNumberOfCallsToGetAll() {
        return stats.getCount(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET_ALL calls to complete.")
    public double getAverageGetAllCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "GetAllThroughput", description = "Throughput of GET_ALL requests.")
    public float getGetAllThroughput() {
        return stats.getThroughput(Tracked.GET_ALL);
    }

    @JmxGetter(name = "GetAllThroughputInBytes", description = "Throughput of GET_ALL requests in bytes.")
    public float getGetAllThroughputInBytes() {
        return stats.getThroughputInBytes(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averageGetAllCount", description = "The avg. number of keys in a GET_ALL request.")
    public double getAverageGetAllCount() {
        return stats.getGetAllAverageCount();
    }

    @JmxGetter(name = "maxGetAllCount", description = "The max number of keys in a GET_ALL request.")
    public long getMaxGetAllCount() {
        return stats.getGetAllMaxCount();
    }

    @JmxGetter(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.")
    public long getNumberOfCallsToGet() {
        return stats.getCount(Tracked.GET);
    }

    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET);
    }

    @JmxGetter(name = "GetThroughput", description = "Throughput of GET requests")
    public float getGetThroughput() {
        return stats.getThroughput(Tracked.GET);
    }

    @JmxGetter(name = "GetThroughputInBytes", description = "Throughput of GET requests in bytes.")
    public float getGetThroughputInBytes() {
        return stats.getThroughputInBytes(Tracked.GET);
    }

    @JmxGetter(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.")
    public long getNumberOfCallsToPut() {
        return stats.getCount(Tracked.PUT);
    }

    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.PUT);
    }

    @JmxGetter(name = "PutThroughput", description = "Throughput of PUT requests.")
    public float getPutThroughput() {
        return stats.getThroughput(Tracked.PUT);
    }

    @JmxGetter(name = "PutThroughputInBytes", description = "Throughput of PUT requests in bytes.")
    public float getPutThroughputInBytes() {
        return stats.getThroughputInBytes(Tracked.PUT);
    }

    @JmxGetter(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.")
    public long getNumberOfCallsToDelete() {
        return stats.getCount(Tracked.DELETE);
    }

    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "DeleteThroughput", description = "Throughput of DELETE requests.")
    public float getDeleteThroughput() {
        return stats.getThroughput(Tracked.DELETE);
    }

    @JmxGetter(name = "numberOfObsoleteVersions", description = "Number of ObsoleteVersionExceptions since the last reset.")
    public long getNumberOfObsoleteVersions() {
        return stats.getCount(Tracked.OBSOLETE);
    }

    @JmxGetter(name = "numberOfExceptions", description = "The number of exceptions since the last reset.")
    public long getNumberOfExceptions() {
        return stats.getCount(Tracked.EXCEPTION);
    }

    @JmxGetter(name = "averageOperationTimeInMs", description = "The total number of all operations that have occured.")
    public double getAvgOperationCompletionTimeInMs() {
        double sum = 0.0;
        double weightedTime = 0.0;
        for(Tracked stat: ImmutableList.of(Tracked.DELETE,
                                           Tracked.GET,
                                           Tracked.GET_ALL,
                                           Tracked.PUT)) {
            sum += stats.getCount(stat);
            weightedTime += stats.getCount(stat) * stats.getAvgTimeInMs(stat);
        }
        if(sum <= 0)
            return 0;
        else
            return weightedTime / sum;
    }

    @JmxGetter(name = "AllOperationThroughput", description = "The throughput of all operations.")
    public double getOperationThroughput() {
        return stats.getThroughput(Tracked.DELETE) + stats.getThroughput(Tracked.GET)
               + stats.getThroughput(Tracked.GET_ALL) + stats.getThroughput(Tracked.PUT);
    }

    @JmxGetter(name = "AllOperationThroughputInBytes", description = "Throughput of all operations in bytes.")
    public double getOperationThroghputInBytes() {
        return stats.getThroughputInBytes(Tracked.GET)
               + stats.getThroughputInBytes(Tracked.GET_ALL)
               + stats.getThroughputInBytes(Tracked.PUT);
    }

    @JmxGetter(name = "percentGetReturningEmptyResponse", description = "The percentage of calls to GET for which no value was found.")
    public double getPercentGetReturningEmptyResponse() {
        return numEmptyResponses(stats.getNumEmptyResponses(Tracked.GET),
                                 stats.getCount(Tracked.GET));
    }

    @JmxGetter(name = "percentGetAllReturningEmptyResponse", description = "The percentage of calls to GET_ALL for which no value was found, taking into account multiple returned key-values.")
    public double getPercentGetAllReturningEmptyResponse() {
        return numEmptyResponses(stats.getNumEmptyResponses(Tracked.GET_ALL),
                                 stats.getGetAllAggregatedCount());
    }

    private double numEmptyResponses(long numEmpty, long total) {
        return total == 0 ? 0.0d : numEmpty / (float) total;
    }

    @JmxGetter(name = "maxPutLatencyInMs", description = "Maximum latency in ms of PUT")
    public long getMaxPutLatency() {
        return stats.getMaxLatencyInMs(Tracked.PUT);
    }

    @JmxGetter(name = "maxGetLatencyInMs", description = "Maximum latency in ms of GET")
    public long getMaxGetLatency() {
        return stats.getMaxLatencyInMs(Tracked.GET);
    }

    @JmxGetter(name = "maxGetAllLatencyInMs", description = "Maximum latency in ms of GET_ALL")
    public long getMaxGetAllLatency() {
        return stats.getMaxLatencyInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "maxDeleteLatencyInMs", description = "Maximum latency in ms of DELETE")
    public long getMaxDeleteLatency() {
        return stats.getMaxLatencyInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "q95PutLatencyInMs", description = "")
    public long getQ95PutLatency() {
        return stats.getQ95LatencyInMs(Tracked.PUT);
    }

    @JmxGetter(name = "q95GetLatencyInMs", description = "")
    public long getQ95GetLatency() {
        return stats.getQ95LatencyInMs(Tracked.GET);
    }

    @JmxGetter(name = "q95GetAllLatencyInMs", description = "")
    public long getQ95GetAllLatency() {
        return stats.getQ95LatencyInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "q95DeleteLatencyInMs", description = "")
    public long getQ95DeleteLatency() {
        return stats.getQ95LatencyInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "q99PutLatencyInMs", description = "")
    public long getQ99PutLatency() {
        return stats.getQ99LatencyInMs(Tracked.PUT);
    }

    @JmxGetter(name = "q99GetLatencyInMs", description = "")
    public long getQ99GetLatency() {
        return stats.getQ99LatencyInMs(Tracked.GET);
    }

    @JmxGetter(name = "q99GetAllLatencyInMs", description = "")
    public long getQ99GetAllLatency() {
        return stats.getQ99LatencyInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "q99DeleteLatencyInMs", description = "")
    public long getQ99DeleteLatency() {
        return stats.getQ99LatencyInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "maxPutSizeInBytes", description = "Maximum size of value returned in bytes by PUT.")
    public long getMaxPutSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.PUT);
    }

    @JmxGetter(name = "maxGetAllSizeInBytes", description = "Maximum size of value returned in bytes by GET_ALL.")
    public long getMaxGetAllSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.GET_ALL);
    }

    @JmxGetter(name = "maxGetSizeInBytes", description = "Maximum size of value returned in bytes by GET.")
    public long getMaxGetSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.GET);
    }

    @JmxGetter(name = "averageGetValueSizeInBytes", description = "Average size in bytes of GET request")
    public double getAverageGetSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.GET);
    }

    @JmxGetter(name = "averageGetAllSizeInBytes", description = "Average size in bytes of GET_ALL request")
    public double getAverageGetAllSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averagePutSizeInBytes", description = "Average size in bytes of PUT request")
    public double getAveragePutSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.PUT);
    }

}

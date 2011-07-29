package voldemort.store.stats;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;

import com.google.common.collect.ImmutableList;
import voldemort.annotations.metrics.Attribute;
import voldemort.annotations.metrics.Sensor;
import voldemort.metrics.DataType;
import voldemort.metrics.MetricType;

/**
 * A wrapper class to expose store stats via JMX
 * 
 * 
 */
@Sensor
@JmxManaged
public class StoreStatsJmx {

    private StoreStats stats;

    public StoreStatsJmx(StoreStats stats) {
        this.stats = stats;
    }

    @Attribute(name = "numberOfCallsToGetAll", description = "The number of calls to GET_ALL since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfCallsToGetAll", description = "The number of calls to GET_ALL since the last reset.")
    public long getNumberOfCallsToGetAll() {
        return stats.getCount(Tracked.GET_ALL);
    }

    @Attribute(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET_ALL calls to complete.",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET_ALL calls to complete.")
    public double getAverageGetAllCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET_ALL);
    }

    @Attribute(name = "GetAllThroughput", description = "Throughput of GET_ALL requests.",
               metricType = MetricType.GAUGE, dataType = DataType.DOUBLE)
    @JmxGetter(name = "GetAllThroughput", description = "Throughput of GET_ALL requests.")
    public float getGetAllThroughput() {
        return stats.getThroughput(Tracked.GET_ALL);
    }

    @Attribute(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfCallsToGet", description = "The number of calls to GET since the last reset.")
    public long getNumberOfCallsToGet() {
        return stats.getCount(Tracked.GET);
    }

    @Attribute(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.GET);
    }

    @Attribute(name = "GetThroughput", description = "Throughput of GET requests.",
               metricType = MetricType.GAUGE, dataType = DataType.DOUBLE)
    @JmxGetter(name = "GetThroughput", description = "Throughput of GET requests.")
    public float getGetThroughput() {
        return stats.getThroughput(Tracked.GET);
    }

    @Attribute(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfCallsToPut", description = "The number of calls to PUT since the last reset.")
    public long getNumberOfCallsToPut() {
        return stats.getCount(Tracked.PUT);
    }

    @Attribute(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for PUT calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.PUT);
    }

    @Attribute(name = "PutThroughput", description = "Throughput of PUT requests.",
               metricType = MetricType.GAUGE, dataType = DataType.DOUBLE)
    @JmxGetter(name = "PutThroughput", description = "Throughput of PUT requests.")
    public float getPutThroughput() {
        return stats.getThroughput(Tracked.PUT);
    }

    @Attribute(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfCallsToDelete", description = "The number of calls to DELETE since the last reset.")
    public long getNumberOfCallsToDelete() {
        return stats.getCount(Tracked.DELETE);
    }

    @Attribute(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for DELETE calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return stats.getAvgTimeInMs(Tracked.DELETE);
    }

    @Attribute(name = "DeleteThroughput", description = "Throughput of DELETE requests.",
               metricType = MetricType.GAUGE, dataType = DataType.DOUBLE)
    @JmxGetter(name = "DeleteThroughput", description = "Throughput of DELETE requests.")
    public float getDeleteThroughput() {
        return stats.getThroughput(Tracked.DELETE);
    }

    @Attribute(name = "numberOfObsoleteVersions", description = "Number of ObsoleteVersionExceptions since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfObsoleteVersions", description = "Number of ObsoleteVersionExceptions since the last reset.")
    public long getNumberOfObsoleteVersions() {
        return stats.getCount(Tracked.OBSOLETE);
    }

    @Attribute(name = "numberOfExceptions", description = "The number of exceptions since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.LONG)
    @JmxGetter(name = "numberOfExceptions", description = "The number of exceptions since the last reset.")
    public long getNumberOfExceptions() {
        return stats.getCount(Tracked.EXCEPTION);
    }

    @Attribute(name = "averageOperationTimeInMs", description = "The total nuber of all operations that have occured.",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "averageOperationTimeInMs", description = "The total nuber of all operations that have occured.")
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

    @Attribute(name = "AllOperationThroughput", description = "The number of exceptions since the last reset.",
               metricType = MetricType.GAUGE, dataType = DataType.DOUBLE)
    @JmxGetter(name = "AllOperationThroughput", description = "The number of exceptions since the last reset.")
    public double getOperationThroughput() {
        return stats.getThroughput(Tracked.DELETE) + stats.getThroughput(Tracked.GET)
               + stats.getThroughput(Tracked.GET_ALL) + stats.getThroughput(Tracked.PUT);
    }


    @Attribute(name = "percentGetReturningEmptyResponse", description = "The percentage of calls to GET for which no value was found.",
               metricType = MetricType.GAUGE, dataType = DataType.PERCENT)
    @JmxGetter(name = "percentGetReturningEmptyResponse", description = "The percentage of calls to GET for which no value was found.")
    public double getPercentGetReturningEmptyResponse() {
        return numEmptyResponses(stats.getNumEmptyResponses(Tracked.GET), stats.getCount(Tracked.GET));
    }

    @Attribute(name = "percentGetAllReturningEmptyResponse", description = "The percentage of calls to GET_ALL for which no value was found, taking into account multiple returned key-values.",
               metricType = MetricType.GAUGE, dataType = DataType.PERCENT)
    @JmxGetter(name = "percentGetAllReturningEmptyResponse", description = "The percentage of calls to GET_ALL for which no value was found, taking into account multiple returned key-values.")
    public double getPercentGetAllReturningEmptyResponse() {
        return numEmptyResponses(stats.getNumEmptyResponses(Tracked.GET_ALL), stats.getGetAllAggregatedCount());
    }

    private double numEmptyResponses(long numEmpty, long total) {
        return total == 0 ? 0.0d : numEmpty / (float)total;
    }

    @Attribute(name = "maxPutLatencyInMs", description = "Maximum latency in ms of PUT",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "maxPutLatencyInMs", description = "Maximum latency in ms of PUT")
    public long getMaxPutLatency() {
        return stats.getMaxLatencyInMs(Tracked.PUT);
    }

    @Attribute(name = "maxGetLatencyInMs", description = "Maximum latency in ms of GET",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "maxGetLatencyInMs", description = "Maximum latency in ms of GET")
    public long getMaxGetLatency() {
        return stats.getMaxLatencyInMs(Tracked.GET);
    }

    @Attribute(name = "maxGetAllLatencyInMs", description = "Maximum latency in ms of GET_ALL",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "maxGetAllLatencyInMs", description = "Maximum latency in ms of GET_ALL")
    public long getMaxGetAllLatency() {
        return stats.getMaxLatencyInMs(Tracked.GET_ALL);
    }

    @Attribute(name = "maxDeleteLatencyInMs", description = "Maximum latency in ms of DELETE",
               metricType = MetricType.GAUGE, dataType = DataType.DURATION)
    @JmxGetter(name = "maxDeleteLatencyInMs", description = "Maximum latency in ms of DELETE")
    public long getMaxDeleteLatency() {
        return stats.getMaxLatencyInMs(Tracked.DELETE);
    }

    @Attribute(name = "maxPutSizeInBytes", description = "Maximum size of value returned in bytes by PUT.",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "maxPutSizeInBytes", description = "Maximum size of value returned in bytes by PUT.")
    public long getMaxPutSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.PUT);
    }

    @Attribute(name = "maxGetAllSizeInBytes", description = "Maximum size of value returned in bytes by GET_ALL.",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "maxGetAllSizeInBytes", description = "Maximum size of value returned in bytes by GET_ALL.")
    public long getMaxGetAllSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.GET_ALL);
    }

    @Attribute(name = "maxGetSizeInBytes", description = "Maximum size of value returned in bytes by GET.",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "maxGetSizeInBytes", description = "Maximum size of value returned in bytes by GET.")
    public long getMaxGetSizeInBytes() {
        return stats.getMaxSizeInBytes(Tracked.GET);
    }

    @Attribute(name = "averageGetValueSizeInBytes", description = "Average size in bytes of GET request",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "averageGetValueSizeInBytes", description = "Average size in bytes of GET request")
    public double getAverageGetSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.GET);
    }

    @Attribute(name = "averageGetAllSizeInBytes", description = "Average size in bytes of GET_ALL request",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "averageGetAllSizeInBytes", description = "Average size in bytes of GET_ALL request")
    public double getAverageGetAllSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.GET_ALL);
    }

    @Attribute(name = "averagePutSizeInBytes", description = "Average size in bytes of PUT request",
               metricType = MetricType.GAUGE, dataType = DataType.STORAGE)
    @JmxGetter(name = "averagePutSizeInBytes", description = "Average size in bytes of PUT request")
    public double getAveragePutSizeInBytes() {
        return stats.getAvgSizeinBytes(Tracked.PUT);
    }

}

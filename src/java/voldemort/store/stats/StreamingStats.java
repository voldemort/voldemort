package voldemort.store.stats;

import java.util.HashMap;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.Time;

public class StreamingStats {

    public enum Operation {
        FETCH_KEYS,
        FETCH_ENTRIES,
        FETCH_FILE,
        UPDATE_ENTRIES,
        SLOP_UPDATE
    }

    private static final int STREAMING_STATS_RESET_INTERVAL_MS = 60000;
    private StreamingStats parent;
    private HashMap<Operation, SimpleCounter> networkTimeCounterMap;
    private HashMap<Operation, SimpleCounter> storageTimeCounterMap;
    private HashMap<Operation, SimpleCounter> streamingPutCounterMap;
    private HashMap<Operation, SimpleCounter> streamingFetchCounterMap;
    private HashMap<Operation, SimpleCounter> streamingScanCounterMap;

    public StreamingStats() {
        networkTimeCounterMap = new HashMap<StreamingStats.Operation, SimpleCounter>();
        storageTimeCounterMap = new HashMap<StreamingStats.Operation, SimpleCounter>();
        streamingPutCounterMap = new HashMap<StreamingStats.Operation, SimpleCounter>();
        streamingFetchCounterMap = new HashMap<StreamingStats.Operation, SimpleCounter>();
        streamingScanCounterMap = new HashMap<StreamingStats.Operation, SimpleCounter>();

        // create the counters for each operation
        networkTimeCounterMap.put(Operation.FETCH_KEYS,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        networkTimeCounterMap.put(Operation.FETCH_ENTRIES,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        networkTimeCounterMap.put(Operation.UPDATE_ENTRIES,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        networkTimeCounterMap.put(Operation.SLOP_UPDATE,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));

        storageTimeCounterMap.put(Operation.FETCH_KEYS,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        storageTimeCounterMap.put(Operation.FETCH_ENTRIES,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        storageTimeCounterMap.put(Operation.UPDATE_ENTRIES,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        storageTimeCounterMap.put(Operation.SLOP_UPDATE,
                                  new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));

        streamingPutCounterMap.put(Operation.SLOP_UPDATE,
                                   new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        streamingPutCounterMap.put(Operation.UPDATE_ENTRIES,
                                   new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));

        streamingFetchCounterMap.put(Operation.FETCH_KEYS,
                                     new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        streamingFetchCounterMap.put(Operation.FETCH_ENTRIES,
                                     new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        streamingFetchCounterMap.put(Operation.FETCH_FILE,
                                     new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));

        streamingScanCounterMap.put(Operation.FETCH_KEYS,
                                    new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
        streamingScanCounterMap.put(Operation.FETCH_ENTRIES,
                                    new SimpleCounter(STREAMING_STATS_RESET_INTERVAL_MS));
    }

    public StreamingStats(StreamingStats parent) {
        this();
        this.parent = parent;
    }

    public void reportNetworkTime(Operation op, long networkTimeMs) {
        networkTimeCounterMap.get(op).count(networkTimeMs);
        if(parent != null)
            parent.reportNetworkTime(op, networkTimeMs);
    }

    public void reportStorageTime(Operation op, long storageTimeMs) {
        storageTimeCounterMap.get(op).count(storageTimeMs);
        if(parent != null)
            parent.reportStorageTime(op, storageTimeMs);
    }

    public void reportStreamingFetch(Operation op) {
        streamingFetchCounterMap.get(op).count();
        if(parent != null)
            parent.reportStreamingFetch(op);
    }

    public void reportStreamingScan(Operation op) {
        streamingScanCounterMap.get(op).count();
        if(parent != null)
            parent.reportStreamingScan(op);
    }

    public void reportStreamingPut(Operation op) {
        streamingPutCounterMap.get(op).count();
        if(parent != null)
            parent.reportStreamingPut(op);
    }

    // Mbeans for FETCH_KEYS
    @JmxGetter(name = "avgFetchKeysNetworkTimeMs", description = "average time spent on network, for fetch keys")
    public double getAvgFetchKeysNetworkTimeMs() {
        return networkTimeCounterMap.get(Operation.FETCH_KEYS).getAvgEventValue() / Time.NS_PER_MS;
    }

    @JmxGetter(name = "avgFetchKeysStorageTimeMs", description = "average time spent on storage, for fetch keys")
    public double getAvgFetchKeysStorageTimeMs() {
        return storageTimeCounterMap.get(Operation.FETCH_KEYS).getAvgEventValue() / Time.NS_PER_MS;
    }

    @JmxGetter(name = "getFetchKeysFetchRate", description = "rate at which keys are fetched per second")
    public double getFetchKeysFetchRate() {
        return streamingFetchCounterMap.get(Operation.FETCH_KEYS).getEventRate();
    }

    @JmxGetter(name = "getFetchKeysScanRate", description = "rate at which keys are scanned per second")
    public double getFetchKeysScanRate() {
        return streamingScanCounterMap.get(Operation.FETCH_KEYS).getEventRate();
    }

    // Mbeans for FETCH_ENTRIES
    @JmxGetter(name = "avgFetchEntriesNetworkTimeMs", description = "average time spent on network, for streaming operations")
    public double getAvgFetchEntriesNetworkTimeMs() {
        return networkTimeCounterMap.get(Operation.FETCH_ENTRIES).getAvgEventValue()
               / Time.NS_PER_MS;
    }

    @JmxGetter(name = "avgFetchEntriesStorageTimeMs", description = "average time spent on storage, for streaming operations")
    public double getAvgFetchEntriesStorageTimeMs() {
        return storageTimeCounterMap.get(Operation.FETCH_ENTRIES).getAvgEventValue()
               / Time.NS_PER_MS;
    }

    @JmxGetter(name = "getFetchEntriesFetchRate", description = "rate at which entries are fetched per second")
    public double getFetchEntriesFetchRate() {
        return streamingFetchCounterMap.get(Operation.FETCH_ENTRIES).getEventRate();
    }

    @JmxGetter(name = "getFetchEntriesScanRate", description = "rate at which entries are scanned per second")
    public double getFetchEntriesScanRate() {
        return streamingScanCounterMap.get(Operation.FETCH_ENTRIES).getEventRate();
    }

    // Mbeans for FETCH_FILE
    @JmxGetter(name = "getFetchFileFetchRate", description = "rate at which RO files are fetched per second")
    public double getFetchFileFetchRate() {
        return streamingFetchCounterMap.get(Operation.FETCH_FILE).getEventRate();
    }

    // Mbeans for UPDATE_ENTRIES
    @JmxGetter(name = "avgUpdateEntriesNetworkTimeMs", description = "average time spent on network, for streaming operations")
    public double getAvgUpdateEntriesNetworkTimeMs() {
        return networkTimeCounterMap.get(Operation.UPDATE_ENTRIES).getAvgEventValue()
               / Time.NS_PER_MS;
    }

    @JmxGetter(name = "avgUpdateEntriesStorageTimeMs", description = "average time spent on storage, for streaming operations")
    public double getAvgUpdateEntriesStorageTimeMs() {
        return storageTimeCounterMap.get(Operation.UPDATE_ENTRIES).getAvgEventValue()
               / Time.NS_PER_MS;
    }

    @JmxGetter(name = "getUpdateEntriesPutRate", description = "rate at which entries are streaming in per second")
    public double getUpdateEntriesPutRate() {
        return streamingPutCounterMap.get(Operation.UPDATE_ENTRIES).getEventRate();
    }

    // Mbeans for SLOP_UPDATE
    @JmxGetter(name = "avgSlopUpdateNetworkTimeMs", description = "average time spent on network, for streaming operations")
    public double getAvgSlopUpdateNetworkTimeMs() {
        return networkTimeCounterMap.get(Operation.SLOP_UPDATE).getAvgEventValue() / Time.NS_PER_MS;
    }

    @JmxGetter(name = "avgSlopUpdateStorageTimeMs", description = "average time spent on storage, for streaming operations")
    public double getAvgSlopUpdateStorageTimeMs() {
        return storageTimeCounterMap.get(Operation.SLOP_UPDATE).getAvgEventValue() / Time.NS_PER_MS;
    }

    @JmxGetter(name = "getSlopUpdatePutRate", description = "Rate at which slop entries are written to the server per second")
    public double getSlopUpdatePutRate() {
        return streamingPutCounterMap.get(Operation.SLOP_UPDATE).getEventRate();
    }
}

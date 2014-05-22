package voldemort.rest;

import java.util.Set;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;

public abstract class RestResponseSender {

    protected MessageEvent messageEvent;
    protected static final long NS_PER_MS = 1000000;
    protected static final long INVALID_START_TIME_IN_MS = -1;

    private final static Logger logger = Logger.getLogger(RestResponseSender.class);

    // Adding a counter to watch the vector clock size - sum of num of
    // entries in each clock
    protected int numVectorClockEntries = 0;

    public RestResponseSender(MessageEvent messageEvent) {
        this.messageEvent = messageEvent;
    }

    public void sendResponse() throws Exception {
        sendResponse(null, false, INVALID_START_TIME_IN_MS);
    }

    public abstract void sendResponse(StoreStats perfomanceStats,
                                      boolean isFromLocalZone,
                                      long startTimeInMs) throws Exception;

    public void recordStats(StoreStats performanceStats, long startTimeInMs, Tracked operation) {
        long duration = System.currentTimeMillis() - startTimeInMs;
        performanceStats.recordTime(operation, duration * NS_PER_MS);
    }

    protected void debugLog(String operationType,
                            String storeName,
                            String keyStr,
                            Long originTimeInMS,
                            Long responseTimeStampInMS,
                            int totalVectorClockEntries) {
        long duration = responseTimeStampInMS - originTimeInMS;
        logger.debug("Received a " + operationType + " response for key(s): " + keyStr
                     + " , store: " + storeName + " , origin time (in ms): " + originTimeInMS
                     + " , response received at time(in ms): " + responseTimeStampInMS
                     + " , num vector clock entries: " + totalVectorClockEntries
                     + " , duration from RESTClient to CoordinatorRestResponseSender(in ms): "
                     + duration);

    }

    protected String getKeysHexString(Set<ByteArray> keys) {
        return RestUtils.getKeysHexString(keys.iterator());
    }
}

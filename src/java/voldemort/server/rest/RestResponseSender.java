package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;

public abstract class RestResponseSender {

    protected MessageEvent messageEvent;
    protected static final long NS_PER_MS = 1000000;
    protected static final long INVALID_START_TIME_IN_MS = -1;

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
}

package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;

import voldemort.store.stats.StoreStats;

public abstract class RestResponseSender {

    protected MessageEvent messageEvent;
    protected long toNanoSeconds = 1000000;
    protected long defaultStartTimeInMS = -1;

    public RestResponseSender(MessageEvent messageEvent) {
        this.messageEvent = messageEvent;
    }

    public void sendResponse() throws Exception {
        sendResponse(null, false, defaultStartTimeInMS);
    }

    public abstract void sendResponse(StoreStats perfomanceStats,
                                      boolean isFromLocalZone,
                                      long startTimeInMs) throws Exception;

}

package voldemort.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;

public class DeleteResponseSender extends RestResponseSender {

    private final static Logger logger = Logger.getLogger(DeleteResponseSender.class);

    // Storing keyname storename for logging purposes
    private ByteArray key;
    private String storeName;

    public DeleteResponseSender(MessageEvent messageEvent) {
        super(messageEvent);
    }

    /**
     * Constructor called by coordinator worker thread
     * 
     * @param messageEvent
     * @param storeName
     * @param key
     */
    public DeleteResponseSender(MessageEvent messageEvent, String storeName, ByteArray key) {
        super(messageEvent);
        this.storeName = storeName;
        this.key = key;
    }

    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) {
        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NO_CONTENT);

        // Set the right headers
        response.setHeader(CONTENT_LENGTH, "0");

        // Write the response to the Netty Channel
        if(logger.isDebugEnabled()) {
            String keyStr = RestUtils.getKeyHexString(key);
            debugLog("DELETE", this.storeName, keyStr, startTimeInMs, System.currentTimeMillis(), 0);
        }
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.DELETE);
        }
    }
}

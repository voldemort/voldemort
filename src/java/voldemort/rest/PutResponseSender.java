package voldemort.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;

public class PutResponseSender extends RestResponseSender {

    private final VectorClock successfulPutVC;
    private final static Logger logger = Logger.getLogger(PutResponseSender.class);

    // Storing keyname storename for logging purposes
    private ByteArray key;
    private String storeName;

    public PutResponseSender(MessageEvent messageEvent) {
        this(messageEvent, null);
    }

    public PutResponseSender(MessageEvent messageEvent, VectorClock successfulPutVC) {
        super(messageEvent);
        this.successfulPutVC = successfulPutVC;
    }

    /**
     * Constructor called by Coordinator worker thread
     * 
     * @param messageEvent
     * @param successfulPutVC
     * @param storeName
     * @param key
     */
    public PutResponseSender(MessageEvent messageEvent,
                             VectorClock successfulPutVC,
                             String storeName,
                             ByteArray key) {
        super(messageEvent);
        this.successfulPutVC = successfulPutVC;
        this.key = key;
        this.storeName = storeName;
    }

    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) {
        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CREATED);

        // Set the right headers
        response.setHeader(CONTENT_LENGTH, 0);

        if(this.successfulPutVC != null) {
            numVectorClockEntries += successfulPutVC.getVersionMap().size();
            String serializedVC = RestUtils.getSerializedVectorClock(successfulPutVC);
            response.setHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, serializedVC);
        }

        // Write the response to the Netty Channel
        if(logger.isDebugEnabled()) {
            String keyStr = RestUtils.getKeyHexString(key);
            debugLog("PUT",
                     this.storeName,
                     keyStr,
                     startTimeInMs,
                     System.currentTimeMillis(),
                     numVectorClockEntries);
        }
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.PUT);
        }
    }
}

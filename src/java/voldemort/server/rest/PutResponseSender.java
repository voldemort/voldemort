package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.coordinator.CoordinatorUtils;
import voldemort.coordinator.VoldemortHttpRequestHandler;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.versioning.VectorClock;

public class PutResponseSender extends RestResponseSender {

    private final VectorClock successfulPutVC;

    public PutResponseSender(MessageEvent messageEvent) {
        this(messageEvent, null);
    }

    public PutResponseSender(MessageEvent messageEvent, VectorClock successfulPutVC) {
        super(messageEvent);
        this.successfulPutVC = successfulPutVC;
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
            String serializedVC = CoordinatorUtils.getSerializedVectorClock(successfulPutVC);
            response.setHeader(VoldemortHttpRequestHandler.X_VOLD_VECTOR_CLOCK, serializedVC);
        }

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.PUT);
        }
    }
}

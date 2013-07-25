package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;

public class DeleteResponseSender extends RestResponseSender {

    public DeleteResponseSender(MessageEvent messageEvent) {
        super(messageEvent);
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
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.DELETE);
        }
    }
}

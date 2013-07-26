package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;

public class GetMetadataResponseSender extends RestResponseSender {

    private final static Logger logger = Logger.getLogger(GetMetadataResponseSender.class);
    private final byte[] responseValue;

    public GetMetadataResponseSender(MessageEvent messageEvent, byte[] responseValue) {
        super(messageEvent);
        this.responseValue = responseValue;
    }

    /**
     * Sends a normal HTTP response containing the serialization information in
     * a XML format
     */
    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) throws Exception {

        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer(this.responseValue.length);
        responseContent.writeBytes(responseValue);

        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");

        // 3. Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        if(logger.isDebugEnabled()) {
            logger.debug("Response = " + response);
        }

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.GET);
        }
    }
}

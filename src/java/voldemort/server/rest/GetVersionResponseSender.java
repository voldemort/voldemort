package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.coordinator.CoordinatorUtils;
import voldemort.store.stats.StoreStats;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class GetVersionResponseSender extends RestResponseSender {

    private List<Version> versionedValues;
    private ByteArray key;
    private String storeName;

    public GetVersionResponseSender(MessageEvent messageEvent,
                                    ByteArray key,
                                    List<Version> versionedValues,
                                    String storeName) {
        super(messageEvent);
        this.versionedValues = versionedValues;
        this.key = key;
        this.storeName = storeName;
    }

    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) throws Exception {

        String contentLocationKey = "/" + this.storeName + "/"
                                    + new String(Base64.encodeBase64(key.get()));

        List<VectorClock> vectorClocks = new ArrayList<VectorClock>();
        for(Version versionedValue: versionedValues) {
            vectorClocks.add((VectorClock) versionedValue);
        }

        String eTags = CoordinatorUtils.getSerializedVectorClocks(vectorClocks);
        byte[] responseContent = eTags.getBytes();
        ChannelBuffer responseContentBuffer = ChannelBuffers.dynamicBuffer(responseContent.length);
        responseContentBuffer.writeBytes(responseContent);

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // Set the right headers
        response.setHeader(CONTENT_TYPE, "binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(CONTENT_LOCATION, contentLocationKey);

        // Copy the data into the payload
        response.setContent(responseContentBuffer);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

    }

}

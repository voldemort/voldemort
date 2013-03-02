package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ETAG;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.client.DefaultStoreClient;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class GetRequestExecutor implements Runnable {

    private ByteArray key;
    private Versioned<Object> defaultValue;
    private MessageEvent getRequestMessageEvent;
    private ChannelBuffer responseContent;
    DefaultStoreClient<Object, Object> storeClient;
    private final Logger logger = Logger.getLogger(GetRequestExecutor.class);

    public GetRequestExecutor(ByteArray key,
                              Versioned<Object> defaultValue,
                              MessageEvent requestEvent,
                              DefaultStoreClient<Object, Object> storeClient) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.getRequestMessageEvent = requestEvent;
        this.storeClient = storeClient;
    }

    public void writeResponse(Versioned<Object> responseVersioned) {

        byte[] value = (byte[]) responseVersioned.getValue();

        // Set the value as the HTTP response payload
        byte[] responseValue = (byte[]) responseVersioned.getValue();
        this.responseContent = ChannelBuffers.dynamicBuffer(responseValue.length);
        this.responseContent.writeBytes(value);

        VectorClock vc = (VectorClock) responseVersioned.getVersion();
        VectorClockWrapper vcWrapper = new VectorClockWrapper(vc);
        ObjectMapper mapper = new ObjectMapper();
        String eTag = "";
        try {
            eTag = mapper.writeValueAsString(vcWrapper);
        } catch(JsonGenerationException e) {
            e.printStackTrace();
        } catch(JsonMappingException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }

        logger.info("ETAG : " + eTag);

        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        // response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setHeader(CONTENT_TYPE, "application/json");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(ETAG, eTag);

        // 3. Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        logger.info("Response = " + response);

        // Write the response to the Netty Channel
        ChannelFuture future = this.getRequestMessageEvent.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        future.addListener(ChannelFutureListener.CLOSE);

    }

    public void setResponseContent(Versioned<Object> responseVersioned) {}

    @Override
    public void run() {
        Versioned<Object> responseVersioned = storeClient.get(this.key);
        logger.info("Get successful !");
        if(responseVersioned == null) {
            if(this.defaultValue != null) {
                responseVersioned = this.defaultValue;
            } else {
                RESTErrorHandler.handleError(NOT_FOUND,
                                             this.getRequestMessageEvent,
                                             false,
                                             "Requested Key does not exist");
            }
        }
        writeResponse(responseVersioned);
    }

}
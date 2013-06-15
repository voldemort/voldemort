package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ETAG;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.coordinator.CoordinatorUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;

public class GetVersionResponseSender extends RestResponseSender {

    private List<Version> versionedValues;
    private ByteArray key;
    private String storeName;
    private final static Logger logger = Logger.getLogger(GetVersionResponseSender.class);

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
    public void sendResponse() throws Exception {
        System.out.println("Here I am constructing the response");
        MimeMultipart multiPart = new MimeMultipart();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String contentLocationKey = "/" + this.storeName + "/"
                                    + new String(Base64.encodeBase64(key.get()));

        for(Version versionedValue: versionedValues) {

            VectorClock vectorClock = (VectorClock) versionedValue;
            String eTag = CoordinatorUtils.getSerializedVectorClock(vectorClock);

            // Create the individual body part for each concurrent version of
            // the given key
            MimeBodyPart body = new MimeBodyPart();
            try {
                body.addHeader(CONTENT_LOCATION, contentLocationKey);
                body.addHeader(ETAG, eTag);
                multiPart.addBodyPart(body);
            } catch(MessagingException me) {
                logger.error("Exception while constructing body part", me);
                throw me;
            }

        }
        try {
            multiPart.writeTo(outputStream);
        } catch(Exception e) {
            logger.error("Exception while writing multipart to output stream", e);
            throw e;
        }
        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
        responseContent.writeBytes(outputStream.toByteArray());

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // Set the right headers
        response.setHeader(CONTENT_TYPE, "multipart/binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");

        // Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

    }
}

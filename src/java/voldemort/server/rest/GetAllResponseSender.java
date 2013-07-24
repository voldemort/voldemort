package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class GetAllResponseSender extends RestResponseSender {

    private Map<ByteArray, List<Versioned<byte[]>>> versionedResponses;
    private String storeName;
    private static final Logger logger = Logger.getLogger(GetAllResponseSender.class);

    public GetAllResponseSender(MessageEvent messageEvent,
                                Map<ByteArray, List<Versioned<byte[]>>> versionedResponses,
                                String storeName) {
        super(messageEvent);
        this.versionedResponses = versionedResponses;
        this.storeName = storeName;
    }

    /**
     * Sends nested multipart response. Outer multipart wraps all the keys
     * requested. Each key has a separate multipart for the versioned values.
     */

    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) throws Exception {

        // multiPartKeys is the outer multipart
        MimeMultipart multiPartKeys = new MimeMultipart();
        ByteArrayOutputStream keysOutputStream = new ByteArrayOutputStream();

        for(Entry<ByteArray, List<Versioned<byte[]>>> entry: versionedResponses.entrySet()) {
            ByteArray key = entry.getKey();
            String contentLocationKey = "/" + this.storeName + "/"
                                        + new String(Base64.encodeBase64(key.get()));

            // Create the individual body part - for each key requested
            MimeBodyPart keyBody = new MimeBodyPart();
            try {
                // Add the right headers
                keyBody.addHeader(CONTENT_TYPE, "application/octet-stream");
                keyBody.addHeader(CONTENT_TRANSFER_ENCODING, "binary");
                keyBody.addHeader(CONTENT_LOCATION, contentLocationKey);
            } catch(MessagingException me) {
                logger.error("Exception while constructing key body headers", me);
                keysOutputStream.close();
                throw me;
            }
            // multiPartValues is the inner multipart
            MimeMultipart multiPartValues = new MimeMultipart();
            for(Versioned<byte[]> versionedValue: entry.getValue()) {

                byte[] responseValue = versionedValue.getValue();

                VectorClock vectorClock = (VectorClock) versionedValue.getVersion();
                String eTag = CoordinatorUtils.getSerializedVectorClock(vectorClock);

                // Create the individual body part - for each versioned value of
                // a key
                MimeBodyPart valueBody = new MimeBodyPart();
                try {
                    // Add the right headers
                    valueBody.addHeader(CONTENT_TYPE, "application/octet-stream");
                    valueBody.addHeader(CONTENT_TRANSFER_ENCODING, "binary");
                    valueBody.addHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, eTag);
                    valueBody.setContent(responseValue, "application/octet-stream");

                    multiPartValues.addBodyPart(valueBody);
                } catch(MessagingException me) {
                    logger.error("Exception while constructing value body part", me);
                    keysOutputStream.close();
                    throw me;
                }

            }
            try {
                // Add the inner multipart as the content of the outer body part
                keyBody.setContent(multiPartValues);
                multiPartKeys.addBodyPart(keyBody);
            } catch(MessagingException me) {
                logger.error("Exception while constructing key body part", me);
                keysOutputStream.close();
                throw me;
            }

        }
        try {
            multiPartKeys.writeTo(keysOutputStream);
        } catch(Exception e) {
            logger.error("Exception while writing mutipart to output stream", e);
            throw e;
        }

        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
        responseContent.writeBytes(keysOutputStream.toByteArray());

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // Set the right headers
        response.setHeader(CONTENT_TYPE, "multipart/binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");

        // Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        if(performanceStats != null && isFromLocalZone) {
            long duration = System.currentTimeMillis() - startTimeInMs;
            performanceStats.recordTime(Tracked.GET_ALL, duration * toNanoSeconds);
        }

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

        keysOutputStream.close();
    }
}

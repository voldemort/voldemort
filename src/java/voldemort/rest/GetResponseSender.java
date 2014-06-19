package voldemort.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class GetResponseSender extends RestResponseSender {

    private List<Versioned<byte[]>> versionedValues;
    private ByteArray key;
    private String storeName;
    private final static Logger logger = Logger.getLogger(GetResponseSender.class);

    public GetResponseSender(MessageEvent messageEvent,
                             ByteArray key,
                             List<Versioned<byte[]>> versionedValues,
                             String storeName) {
        super(messageEvent);
        this.versionedValues = versionedValues;
        this.key = key;
        this.storeName = storeName;
    }

    /**
     * Sends a multipart response. Each body part represents a versioned value
     * of the given key.
     *
     * @throws IOException
     * @throws MessagingException
     */
    @Override
    public void sendResponse(StoreStats performanceStats,
                             boolean isFromLocalZone,
                             long startTimeInMs) throws Exception {

        /*
         * Pay attention to the code below. Note that in this method we wrap a multiPart object with a mimeMessage.
         * However when writing to the outputStream we only send the multiPart object and not the entire
         * mimeMessage. This is intentional.
         *
         * In the earlier version of this code we used to create a multiPart object and just send that multiPart
         * across the wire.
         *
         * However, we later discovered that upon setting the content of a MimeBodyPart, JavaMail internally creates
         * a DataHandler object wrapping the object you passed in. The part's Content-Type header is not updated
         * immediately. In order to get the headers updated, one needs to to call MimeMessage.saveChanges() on the
         * enclosing message, which cascades down the MIME structure into a call to MimeBodyPart.updateHeaders()
         * on the body part. It's this updateHeaders call that transfers the content type from the
         * DataHandler to the part's MIME Content-Type header.
         *
         * To make sure that the Content-Type headers are being updated (without changing too much code), we decided
         * to wrap the multiPart in a mimeMessage, call mimeMessage.saveChanges() and then just send the multiPart.
         * This is to make sure multiPart's headers are updated accurately.
         */

        MimeMessage message = new MimeMessage(Session.getDefaultInstance(new Properties()));
        MimeMultipart multiPart = new MimeMultipart();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String base64Key = RestUtils.encodeVoldemortKey(key.get());
        String contentLocationKey = "/" + this.storeName + "/" + base64Key;

        for(Versioned<byte[]> versionedValue: versionedValues) {

            byte[] responseValue = versionedValue.getValue();

            VectorClock vectorClock = (VectorClock) versionedValue.getVersion();
            String eTag = RestUtils.getSerializedVectorClock(vectorClock);
            numVectorClockEntries += vectorClock.getVersionMap().size();

            // Create the individual body part for each versioned value of the
            // requested key
            MimeBodyPart body = new MimeBodyPart();
            try {
                // Add the right headers
                body.addHeader(CONTENT_TYPE, "application/octet-stream");
                body.addHeader(CONTENT_TRANSFER_ENCODING, "binary");
                body.addHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, eTag);
                body.setContent(responseValue, "application/octet-stream");
                body.addHeader(RestMessageHeaders.CONTENT_LENGTH,
                               Integer.toString(responseValue.length));

                multiPart.addBodyPart(body);
            } catch(MessagingException me) {
                logger.error("Exception while constructing body part", me);
                outputStream.close();
                throw me;
            }

        }
        message.setContent(multiPart);
        message.saveChanges();
        try {
            multiPart.writeTo(outputStream);
        } catch(Exception e) {
            logger.error("Exception while writing multipart to output stream", e);
            outputStream.close();
            throw e;
        }
        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
        responseContent.writeBytes(outputStream.toByteArray());

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // Set the right headers
        response.setHeader(CONTENT_TYPE, "multipart/binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(CONTENT_LOCATION, contentLocationKey);

        // Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        // Write the response to the Netty Channel
        if(logger.isDebugEnabled()) {
            String keyStr = RestUtils.getKeyHexString(this.key);
            debugLog("GET",
                     this.storeName,
                     keyStr,
                     startTimeInMs,
                     System.currentTimeMillis(),
                     numVectorClockEntries);
        }
        this.messageEvent.getChannel().write(response);

        if(performanceStats != null && isFromLocalZone) {
            recordStats(performanceStats, startTimeInMs, Tracked.GET);
        }

        outputStream.close();

    }
}

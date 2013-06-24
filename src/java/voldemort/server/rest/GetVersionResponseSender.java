package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ETAG;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
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
        String contentLocationKey = "/" + this.storeName + "/"
                                    + new String(Base64.encodeBase64(key.get()));
        // construct the list of versions
        StringBuilder eTags = new StringBuilder();
        boolean firstETag = true;
        for(Version versionedValue: versionedValues) {

            VectorClock vectorClock = (VectorClock) versionedValue;
            String eTag = CoordinatorUtils.getSerializedVectorClock(vectorClock);
            if(firstETag) {
                eTags.append(eTag);
                firstETag = false;
            } else {
                eTags.append(", " + eTag);
            }
        }

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // Set the right headers
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(CONTENT_LENGTH, 0);
        response.setHeader(CONTENT_LOCATION, contentLocationKey);
        response.setHeader(ETAG, eTags.toString());

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);

    }
}

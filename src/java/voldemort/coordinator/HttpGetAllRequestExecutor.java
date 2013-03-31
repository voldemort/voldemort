/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LOCATION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ETAG;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.VoldemortException;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.StoreTimeoutException;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A Runnable class that uses the specified Fat client to perform a Voldemort
 * GET operation. This is invoked by a FatClientWrapper thread to satisfy a
 * corresponding REST GET request.
 * 
 */
public class HttpGetAllRequestExecutor implements Runnable {

    private MessageEvent getRequestMessageEvent;
    DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient;
    private final Logger logger = Logger.getLogger(HttpGetRequestExecutor.class);
    private final CompositeVoldemortRequest<ByteArray, byte[]> getAllRequestObject;
    private final String storeName;
    private final long startTimestampInNs;
    private final StoreStats coordinatorPerfStats;

    /**
     * 
     * @param getAllRequestObject The request object containing key and timeout
     *        values
     * @param requestEvent Reference to the MessageEvent for the response /
     *        error
     * @param storeClient Reference to the fat client for performing this Get
     *        operation
     * @param storeName Name of the store intended to be included in the
     *        response (content-location)
     * @param coordinatorPerfStats Stats object used to measure the turnaround
     *        time
     * @param startTimestampInNs start timestamp of the request
     */
    public HttpGetAllRequestExecutor(CompositeVoldemortRequest<ByteArray, byte[]> getAllRequestObject,
                                     MessageEvent requestMessageEvent,
                                     DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient,
                                     String storeName,
                                     long startTimestampInNs,
                                     StoreStats coordinatorPerfStats) {
        this.getRequestMessageEvent = requestMessageEvent;
        this.storeClient = storeClient;
        this.getAllRequestObject = getAllRequestObject;
        this.storeName = storeName;
        this.startTimestampInNs = startTimestampInNs;
        this.coordinatorPerfStats = coordinatorPerfStats;
    }

    public void writeResponse(Map<ByteArray, Versioned<byte[]>> responseVersioned) {

        // Multipart response
        MimeMultipart mp = new MimeMultipart();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {

            for(Entry<ByteArray, Versioned<byte[]>> entry: responseVersioned.entrySet()) {
                Versioned<byte[]> value = entry.getValue();
                ByteArray keyByteArray = entry.getKey();
                String base64Key = new String(Base64.encodeBase64(keyByteArray.get()));
                String contentLocationKey = "/" + this.storeName + "/" + base64Key;

                byte[] responseValue = value.getValue();

                VectorClock vc = (VectorClock) value.getVersion();
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

                if(logger.isDebugEnabled()) {
                    logger.debug("ETAG : " + eTag);
                }

                // Create the individual body part
                MimeBodyPart body = new MimeBodyPart();
                body.addHeader(CONTENT_TYPE, "application/octet-stream");
                body.addHeader(CONTENT_LOCATION, contentLocationKey);
                body.addHeader(CONTENT_TRANSFER_ENCODING, "binary");
                body.addHeader(CONTENT_LENGTH, "" + responseValue.length);
                body.addHeader(ETAG, eTag);
                body.setContent(responseValue, "application/octet-stream");
                mp.addBodyPart(body);
            }

            // At this point we have a complete multi-part response
            mp.writeTo(outputStream);

        } catch(MessagingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
        responseContent.writeBytes(outputStream.toByteArray());

        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "multipart/binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");

        // 3. Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        // Update the stats
        if(this.coordinatorPerfStats != null) {
            long durationInNs = System.nanoTime() - startTimestampInNs;
            this.coordinatorPerfStats.recordTime(Tracked.GET_ALL, durationInNs);
        }

        // Write the response to the Netty Channel
        this.getRequestMessageEvent.getChannel().write(response);
    }

    @Override
    public void run() {
        try {
            Map<ByteArray, Versioned<byte[]>> responseVersioned = storeClient.getAllWithCustomTimeout(this.getAllRequestObject);
            if(responseVersioned == null) {
                RESTErrorHandler.handleError(NOT_FOUND,
                                             this.getRequestMessageEvent,
                                             "Requested Key does not exist");
            }
            writeResponse(responseVersioned);
        } catch(IllegalArgumentException illegalArgsException) {
            String errorDescription = "GETALL Failed !!! Illegal Arguments : "
                                      + illegalArgsException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(BAD_REQUEST, this.getRequestMessageEvent, errorDescription);
        } catch(StoreTimeoutException timeoutException) {
            String errorDescription = "GET Request timed out: " + timeoutException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                         this.getRequestMessageEvent,
                                         errorDescription);
        } catch(VoldemortException ve) {
            String errorDescription = "Voldemort Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.getRequestMessageEvent,
                                         errorDescription);
        }
    }

}
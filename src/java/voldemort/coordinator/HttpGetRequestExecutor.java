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
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TRANSFER_ENCODING;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.ArrayList;
import java.util.List;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.VoldemortException;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreTimeoutException;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A Runnable class that uses the specified Fat client to perform a Voldemort
 * GET operation. This is invoked by a FatClientWrapper thread to satisfy a
 * corresponding REST GET request.
 * 
 */
public class HttpGetRequestExecutor implements Runnable {

    private MessageEvent getRequestMessageEvent;
    private ChannelBuffer responseContent;
    DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient;
    private final Logger logger = Logger.getLogger(HttpGetRequestExecutor.class);
    private final CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject;
    private final long startTimestampInNs;
    private final StoreStats coordinatorPerfStats;

    /**
     * Dummy constructor invoked during a Noop Get operation
     * 
     * @param requestEvent MessageEvent used to write the response
     */
    public HttpGetRequestExecutor(MessageEvent requestEvent) {
        this.getRequestMessageEvent = requestEvent;
        this.getRequestObject = null;
        this.startTimestampInNs = 0;
        this.coordinatorPerfStats = null;
    }

    /**
     * 
     * @param getRequestObject The request object containing key and timeout
     *        values
     * @param requestEvent Reference to the MessageEvent for the response /
     *        error
     * @param storeClient Reference to the fat client for performing this Get
     *        operation
     * @param coordinatorPerfStats Stats object used to measure the turnaround
     *        time
     * @param startTimestampInNs start timestamp of the request
     */
    public HttpGetRequestExecutor(CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject,
                                  MessageEvent requestEvent,
                                  DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient,
                                  long startTimestampInNs,
                                  StoreStats coordinatorPerfStats) {
        this.getRequestMessageEvent = requestEvent;
        this.storeClient = storeClient;
        this.getRequestObject = getRequestObject;
        this.startTimestampInNs = startTimestampInNs;
        this.coordinatorPerfStats = coordinatorPerfStats;
    }

    public void writeResponse(List<Versioned<byte[]>> versionedValues) throws Exception {

        MimeMultipart multiPart = new MimeMultipart();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for(Versioned<byte[]> versionedValue: versionedValues) {

            byte[] responseValue = versionedValue.getValue();

            VectorClock vectorClock = (VectorClock) versionedValue.getVersion();
            String serializedVectorClock = CoordinatorUtils.getSerializedVectorClock(vectorClock);

            // Create the individual body part for each versioned value of the
            // requested key
            MimeBodyPart body = new MimeBodyPart();
            try {
                // Add the right headers
                body.addHeader(CONTENT_TYPE, "application/octet-stream");
                body.addHeader(CONTENT_TRANSFER_ENCODING, "binary");
                body.addHeader(VoldemortHttpRequestHandler.X_VOLD_VECTOR_CLOCK,
                               serializedVectorClock);
                body.setContent(responseValue, "application/octet-stream");
                body.addHeader(CONTENT_LENGTH, "" + responseValue.length);

                multiPart.addBodyPart(body);
            } catch(MessagingException me) {
                logger.error("Exception while constructing body part", me);
                outputStream.close();
                throw me;
            }
        }
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

        // Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        if(logger.isDebugEnabled()) {
            logger.debug("Response = " + response);
        }

        // Update the stats
        if(this.coordinatorPerfStats != null) {
            long durationInNs = System.nanoTime() - startTimestampInNs;
            this.coordinatorPerfStats.recordTime(Tracked.GET, durationInNs);
        }

        // Write the response to the Netty Channel
        this.getRequestMessageEvent.getChannel().write(response);
    }

    @Override
    public void run() {
        try {
            List<Versioned<byte[]>> versionedValues = storeClient.getWithCustomTimeout(this.getRequestObject);
            if(versionedValues == null || versionedValues.size() == 0) {
                if(this.getRequestObject.getValue() != null) {
                    if(versionedValues == null) {
                        versionedValues = new ArrayList<Versioned<byte[]>>();
                    }
                    versionedValues.add(this.getRequestObject.getValue());

                } else {
                    RESTErrorHandler.handleError(NOT_FOUND,
                                                 this.getRequestMessageEvent,
                                                 "Requested Key does not exist");
                }
                if(logger.isDebugEnabled()) {
                    logger.debug("GET successful !");
                }
            }
            writeResponse(versionedValues);
        } catch(IllegalArgumentException illegalArgsException) {
            String errorDescription = "PUT Failed !!! Illegal Arguments : "
                                      + illegalArgsException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(BAD_REQUEST, this.getRequestMessageEvent, errorDescription);
        } catch(StoreTimeoutException timeoutException) {
            String errorDescription = "GET Request timed out: " + timeoutException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                         this.getRequestMessageEvent,
                                         errorDescription);
        } catch(InsufficientOperationalNodesException exception) {
            long nowInNs = System.nanoTime();
            if(nowInNs - startTimestampInNs > getRequestObject.getRoutingTimeoutInMs()
                                              * Time.NS_PER_MS) {
                String errorDescription = "GET Request timed out: " + exception.getMessage();
                logger.error(errorDescription);
                RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                             this.getRequestMessageEvent,
                                             errorDescription);
            } else {
                String errorDescription = "Voldemort Exception: " + exception.getMessage();
                RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                             this.getRequestMessageEvent,
                                             errorDescription);
            }

        } catch(VoldemortException ve) {
            String errorDescription = "Voldemort Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.getRequestMessageEvent,
                                         errorDescription);
        } catch(Exception ve) {
            String errorDescription = "Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.getRequestMessageEvent,
                                         errorDescription);
        }
    }

}
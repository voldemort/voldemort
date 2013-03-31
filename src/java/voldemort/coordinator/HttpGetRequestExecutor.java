/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ETAG;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;

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
import voldemort.utils.ByteArray;
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

    /**
     * 
     * @param getRequestObject The request object containing key and timeout
     *        values
     * @param requestEvent Reference to the MessageEvent for the response /
     *        error
     * @param storeClient Reference to the fat client for performing this Get
     *        operation
     */
    public HttpGetRequestExecutor(CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject,
                                  MessageEvent requestEvent,
                                  DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient) {
        this.getRequestMessageEvent = requestEvent;
        this.storeClient = storeClient;
        this.getRequestObject = getRequestObject;
    }

    public void writeResponse(Versioned<byte[]> responseVersioned) {

        byte[] value = responseVersioned.getValue();

        // Set the value as the HTTP response payload
        byte[] responseValue = responseVersioned.getValue();
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

        if(logger.isDebugEnabled()) {
            logger.debug("ETAG : " + eTag);
        }

        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(ETAG, eTag);

        // 3. Copy the data into the payload
        response.setContent(responseContent);
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        if(logger.isDebugEnabled()) {
            logger.debug("Response = " + response);
        }

        // Write the response to the Netty Channel
        this.getRequestMessageEvent.getChannel().write(response);
    }

    @Override
    public void run() {
        try {
            Versioned<byte[]> responseVersioned = storeClient.getWithCustomTimeout(this.getRequestObject);
            if(responseVersioned == null) {
                if(this.getRequestObject.getValue() != null) {
                    responseVersioned = this.getRequestObject.getValue();
                } else {
                    RESTErrorHandler.handleError(NOT_FOUND,
                                                 this.getRequestMessageEvent,
                                                 false,
                                                 "Requested Key does not exist");
                }
                if(logger.isDebugEnabled()) {
                    logger.debug("GET successful !");
                }
            }
            writeResponse(responseVersioned);
        } catch(IllegalArgumentException illegalArgsException) {
            String errorDescription = "PUT Failed !!! Illegal Arguments : "
                                      + illegalArgsException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(BAD_REQUEST,
                                         this.getRequestMessageEvent,
                                         false,
                                         errorDescription);
        } catch(StoreTimeoutException timeoutException) {
            String errorDescription = "GET Request timed out: " + timeoutException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                         this.getRequestMessageEvent,
                                         false,
                                         errorDescription);
        } catch(VoldemortException ve) {
            String errorDescription = "Voldemort Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.getRequestMessageEvent,
                                         false,
                                         errorDescription);
        }
    }

}
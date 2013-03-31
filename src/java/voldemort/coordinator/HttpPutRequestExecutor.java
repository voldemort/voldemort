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
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.VoldemortException;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.StoreTimeoutException;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;

/**
 * A Runnable class that uses the specified Fat client to perform a Voldemort
 * PUT operation. This is invoked by a FatClientWrapper thread to satisfy a
 * corresponding REST POST (PUT) request.
 * 
 */
public class HttpPutRequestExecutor implements Runnable {

    private MessageEvent putRequestMessageEvent;
    DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient;
    private final Logger logger = Logger.getLogger(HttpPutRequestExecutor.class);
    private final CompositeVoldemortRequest<ByteArray, byte[]> putRequestObject;

    public HttpPutRequestExecutor(MessageEvent requestEvent) {
        this.putRequestMessageEvent = requestEvent;
        this.putRequestObject = null;
    }

    /**
     * 
     * @param putRequestObject The request object containing key and timeout
     *        values
     * @param requestEvent Reference to the MessageEvent for the response /
     *        error
     * @param storeClient Reference to the fat client for performing this Get
     *        operation
     */
    public HttpPutRequestExecutor(CompositeVoldemortRequest<ByteArray, byte[]> putRequestObject,
                                  MessageEvent requestEvent,
                                  DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient) {
        this.putRequestMessageEvent = requestEvent;
        this.storeClient = storeClient;
        this.putRequestObject = putRequestObject;
    }

    public void writeResponse() {
        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "application/json");

        // 3. Copy the data into the payload
        response.setHeader(CONTENT_LENGTH, 0);

        // Write the response to the Netty Channel
        this.putRequestMessageEvent.getChannel().write(response);
    }

    @Override
    public void run() {

        try {
            this.storeClient.putWithCustomTimeout(putRequestObject);
            if(logger.isDebugEnabled()) {
                logger.debug("PUT successful !");
            }
            writeResponse();

        } catch(IllegalArgumentException illegalArgsException) {
            String errorDescription = "PUT Failed !!! Illegal Arguments : "
                                      + illegalArgsException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(BAD_REQUEST,
                                         this.putRequestMessageEvent,
                                         false,
                                         errorDescription);
        } catch(ObsoleteVersionException oe) {
            String errorDescription = "PUT Failed !!! Obsolete version exception: "
                                      + oe.getMessage();
            RESTErrorHandler.handleError(PRECONDITION_FAILED,
                                         this.putRequestMessageEvent,
                                         false,
                                         errorDescription);

        } catch(StoreTimeoutException timeoutException) {
            String errorDescription = "GET Request timed out: " + timeoutException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                         this.putRequestMessageEvent,
                                         false,
                                         errorDescription);

        } catch(VoldemortException ve) {
            String errorDescription = "Voldemort Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.putRequestMessageEvent,
                                         false,
                                         errorDescription);
        }
    }

}
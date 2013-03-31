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
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

import voldemort.VoldemortException;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.StoreTimeoutException;
import voldemort.utils.ByteArray;

/**
 * A Runnable class that uses the specified Fat client to perform a Voldemort
 * DELETE operation. This is invoked by a FatClientWrapper thread to satisfy a
 * corresponding REST DELETE request.
 * 
 */
public class HttpDeleteRequestExecutor implements Runnable {

    private MessageEvent deleteRequestMessageEvent;
    DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient;
    private final Logger logger = Logger.getLogger(HttpDeleteRequestExecutor.class);
    private final CompositeVoldemortRequest<ByteArray, byte[]> deleteRequestObject;

    /**
     * 
     * @param deleteRequestObject The request object containing key and version
     *        values
     * @param requestEvent Reference to the MessageEvent for the response /
     *        error
     * @param storeClient Reference to the fat client for performing this Delete
     *        operation
     */
    public HttpDeleteRequestExecutor(CompositeVoldemortRequest<ByteArray, byte[]> deleteRequestObject,
                                     MessageEvent requestEvent,
                                     DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient) {
        this.deleteRequestMessageEvent = requestEvent;
        this.storeClient = storeClient;
        this.deleteRequestObject = deleteRequestObject;
    }

    public void writeResponse() {
        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        // 2. Set the right headers
        response.setHeader(CONTENT_TYPE, "binary");
        response.setHeader(CONTENT_TRANSFER_ENCODING, "binary");
        response.setHeader(CONTENT_LENGTH, "0");

        // Write the response to the Netty Channel
        ChannelFuture future = this.deleteRequestMessageEvent.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        future.addListener(ChannelFutureListener.CLOSE);

    }

    @Override
    public void run() {
        try {
            boolean isDeleted = storeClient.deleteWithCustomTimeout(this.deleteRequestObject);
            if(isDeleted) {
                writeResponse();
            } else {
                RESTErrorHandler.handleError(NOT_FOUND,
                                             this.deleteRequestMessageEvent,
                                             false,
                                             "Requested Key with the specified version does not exist");
            }

        } catch(StoreTimeoutException timeoutException) {
            String errorDescription = "DELETE Request timed out: " + timeoutException.getMessage();
            logger.error(errorDescription);
            RESTErrorHandler.handleError(REQUEST_TIMEOUT,
                                         this.deleteRequestMessageEvent,
                                         false,
                                         errorDescription);
        } catch(VoldemortException ve) {
            ve.printStackTrace();
            String errorDescription = "Voldemort Exception: " + ve.getMessage();
            RESTErrorHandler.handleError(INTERNAL_SERVER_ERROR,
                                         this.deleteRequestMessageEvent,
                                         false,
                                         errorDescription);
        }
    }

}

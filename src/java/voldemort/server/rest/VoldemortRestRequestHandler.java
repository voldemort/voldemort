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

package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * Class to handle a REST request and send response back to the client
 * 
 */
public class VoldemortRestRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    private StoreRepository storeRepository;
    private final Logger logger = Logger.getLogger(VoldemortRestRequestHandler.class);

    // Implicit constructor defined for the derived classes
    public VoldemortRestRequestHandler() {}

    public VoldemortRestRequestHandler(StoreRepository storeRepository) {
        this.storeRepository = storeRepository;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent)
            throws Exception {
        RestServerRequestValidator requestValidator;
        if(!readingChunks) {

            // Construct the Request from messageEvent
            HttpRequest request = this.request = (HttpRequest) messageEvent.getMessage();
            String requestURI = this.request.getUri();
            if(logger.isDebugEnabled()) {
                logger.debug("Request URI: " + requestURI);
            }

            if(request.isChunked()) {
                readingChunks = true;
            } else {
                // Instantiate the appropriate error handler
                HttpMethod httpMethod = request.getMethod();
                if(httpMethod.equals(HttpMethod.GET)) {
                    requestValidator = new RestServerGetRequestValidator(request,
                                                                         messageEvent,
                                                                         storeRepository);
                } else if(httpMethod.equals(HttpMethod.POST)) {
                    requestValidator = new RestServerPutRequestValidator(request,
                                                                         messageEvent,
                                                                         storeRepository);
                } else if(httpMethod.equals(HttpMethod.DELETE)) {
                    requestValidator = new RestServerDeleteRequestValidator(request,
                                                                            messageEvent,
                                                                            storeRepository);
                } else {
                    String errorMessage = "Illegal Http request.";
                    logger.error(errorMessage);
                    RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                              BAD_REQUEST,
                                                              errorMessage);
                    return;
                }

                // At this point we know the request is valid and we have a
                // error handler. So we construct the composite Voldemort
                // request object.
                CompositeVoldemortRequest<ByteArray, byte[]> requestObject = requestValidator.constructCompositeVoldemortRequestObject();
                if(requestObject != null) {

                    // Dropping dead requests from going to next handler
                    long now = System.currentTimeMillis();
                    if(requestObject.getRequestOriginTimeInMs()
                       + requestObject.getRoutingTimeoutInMs() <= now) {
                        RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                  HttpResponseStatus.REQUEST_TIMEOUT,
                                                                  "current time: "
                                                                          + now
                                                                          + "\torigin time: "
                                                                          + requestObject.getRequestOriginTimeInMs()
                                                                          + "\ttimeout in ms: "
                                                                          + requestObject.getRoutingTimeoutInMs());
                        return;
                    } else {
                        Store store = getStore(requestValidator.getStoreName(),
                                               requestValidator.getParsedRoutingType());
                        if(store != null) {
                            VoldemortStoreRequest voldemortStoreRequest = new VoldemortStoreRequest(requestObject,
                                                                                                    store,
                                                                                                    parseZoneId());
                            Channels.fireMessageReceived(ctx, voldemortStoreRequest);
                        } else {
                            logger.error("Error when getting store. Non Existing store name.");
                            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                                      HttpResponseStatus.BAD_REQUEST,
                                                                      "Non Existing store name. Critical error.");
                            return;

                        }
                    }
                }
            }
        } else {
            HttpChunk chunk = (HttpChunk) messageEvent.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }

    /**
     * Gets the store for the store name and routing type. At this point we
     * already know that the routing type is valid . So we dont throw Voldemort
     * Exception for unknown routing type.
     * 
     * @param name - store name
     * @param type - routing type from the request
     * @return
     */
    protected Store<ByteArray, byte[], byte[]> getStore(String name, RequestRoutingType type) {

        switch(type) {
            case ROUTED:
                return this.storeRepository.getRoutedStore(name);
            case NORMAL:
                return this.storeRepository.getLocalStore(name);
            case IGNORE_CHECKS:
                return this.storeRepository.getStorageEngine(name);
        }
        return null;
    }

    /**
     * Retrieve and validate the zone id value from the REST request.
     * "X-VOLD-Zone-Id" is the zone id header.
     * 
     * @return valid zone id or -1 if there is no/invalid zone id
     */
    protected int parseZoneId() {
        int result = -1;
        String zoneIdStr = this.request.getHeader(RestMessageHeaders.X_VOLD_ZONE_ID);
        if(zoneIdStr != null) {
            try {
                int zoneId = Integer.parseInt(zoneIdStr);
                if(zoneId < 0) {
                    logger.error("ZoneId cannot be negative. Assuming the default zone id.");
                } else {
                    result = zoneId;
                }
            } catch(NumberFormatException nfe) {
                logger.error("Exception when validating request. Incorrect zone id parameter. Cannot parse this to int: "
                                     + zoneIdStr,
                             nfe);
            }
        }
        return result;
    }
}

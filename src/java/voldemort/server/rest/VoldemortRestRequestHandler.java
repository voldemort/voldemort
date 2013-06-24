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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.common.VoldemortOpCode;
import voldemort.server.StoreRepository;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Class to handle a REST request and send response back to the client
 * 
 * TODO REST-Server 1.Implement Get metadata 2. HAndle store appropriately -
 * currently its a hack 3. Debug GET Version
 */
public class VoldemortRestRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    private StoreRepository storeRepository;
    private final Logger logger = Logger.getLogger(VoldemortRestRequestHandler.class);
    // TODO - REST-Server Just a hack to mock store. Need to change this
    // later
    private final static Store<ByteArray, byte[], byte[]> inMemoryStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");

    // Implicit constructor defined for the derived classes
    public VoldemortRestRequestHandler() {}

    public VoldemortRestRequestHandler(StoreRepository storeRepository) {
        this.storeRepository = storeRepository;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent)
            throws Exception {
        RestServerErrorHandler errorHandler;
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
                    errorHandler = new RestServerGetRequestErrorHandler(request,
                                                                        messageEvent,
                                                                        storeRepository);
                } else if(httpMethod.equals(HttpMethod.POST)) {
                    errorHandler = new RestServerPutRequestErrorHandler(request,
                                                                        messageEvent,
                                                                        storeRepository);
                } else if(httpMethod.equals(HttpMethod.DELETE)) {
                    errorHandler = new RestServerDeleteRequestErrorHandler(request,
                                                                           messageEvent,
                                                                           storeRepository);
                } else if(httpMethod.equals(HttpMethod.HEAD)) {
                    errorHandler = new RestServerGetVersionRequestErrorHandler(request,
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
                CompositeVoldemortRequest<ByteArray, byte[]> requestObject = errorHandler.constructCompositeVoldemortRequestObject();

                if(requestObject != null) {

                    // Issue the requests to store and handle exceptions
                    // accordingly or send back response to the client
                    switch(requestObject.getOperationType()) {
                        case VoldemortOpCode.GET_OP_CODE:
                            if(logger.isDebugEnabled()) {
                                logger.debug("Incoming get request");
                            }
                            try {
                                List<Versioned<byte[]>> versionedValues = inMemoryStore.get(requestObject.getKey(),
                                                                                            null);
                                GetResponseSender responseConstructor = new GetResponseSender(messageEvent,
                                                                                              requestObject.getKey(),
                                                                                              versionedValues,
                                                                                              inMemoryStore.getName());
                                responseConstructor.sendResponse();
                            } catch(Exception e) {
                                errorHandler.handleExceptions(e);
                            }
                            break;

                        case VoldemortOpCode.GET_ALL_OP_CODE:
                            if(logger.isDebugEnabled()) {
                                logger.debug("Incoming get all request");
                            }
                            try {
                                Map<ByteArray, List<Versioned<byte[]>>> keyValuesMap = inMemoryStore.getAll(requestObject.getIterableKeys(),
                                                                                                            null);
                                GetAllResponseSender responseConstructor = new GetAllResponseSender(messageEvent,
                                                                                                    keyValuesMap,
                                                                                                    inMemoryStore.getName());
                                responseConstructor.sendResponse();
                            } catch(Exception e) {
                                errorHandler.handleExceptions(e);
                            }
                            break;

                        case VoldemortOpCode.PUT_OP_CODE:
                            if(logger.isDebugEnabled()) {
                                logger.debug("Incoming put request");
                            }
                            try {
                                inMemoryStore.put(requestObject.getKey(),
                                                  requestObject.getValue(),
                                                  null);
                                PutResponseSender responseConstructor = new PutResponseSender(messageEvent);
                                responseConstructor.sendResponse();
                            } catch(Exception e) {
                                errorHandler.handleExceptions(e);
                            }
                            break;

                        case VoldemortOpCode.DELETE_OP_CODE:
                            if(logger.isDebugEnabled()) {
                                logger.debug("Incoming delete request");
                            }
                            try {
                                boolean result = inMemoryStore.delete(requestObject.getKey(),
                                                                      requestObject.getVersion());
                                DeleteResponseSender responseConstructor = new DeleteResponseSender(messageEvent);
                                responseConstructor.sendResponse();
                            } catch(Exception e) {
                                errorHandler.handleExceptions(e);
                            }
                            break;

                        case VoldemortOpCode.GET_VERSION_OP_CODE:

                            if(logger.isDebugEnabled()) {
                                logger.debug("Incoming get version request");
                            }
                            try {
                                List<Version> versions = inMemoryStore.getVersions(requestObject.getKey());
                                GetVersionResponseSender responseConstructor = new GetVersionResponseSender(messageEvent,
                                                                                                            requestObject.getKey(),
                                                                                                            versions,
                                                                                                            inMemoryStore.getName());
                                responseConstructor.sendResponse();
                            } catch(Exception e) {
                                errorHandler.handleExceptions(e);
                            }
                            break;
                        default:
                            // Since we dont add any other operations than the 5
                            // above, the code stops here.
                            return;
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
}

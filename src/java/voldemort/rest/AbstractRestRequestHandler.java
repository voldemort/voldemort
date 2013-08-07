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

package voldemort.rest;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Class to handle a REST request and send response back to the client
 * 
 */
public abstract class AbstractRestRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    private final Logger logger = Logger.getLogger(AbstractRestRequestHandler.class);
    private final boolean isVectorClockOptional;

    private long maxLatency = 0;

    // Implicit constructor defined for the derived classes
    public AbstractRestRequestHandler() {
        this.isVectorClockOptional = true;
    }

    public AbstractRestRequestHandler(boolean isVectorClockOptional) {
        this.isVectorClockOptional = isVectorClockOptional;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent)
            throws Exception {
        RestRequestValidator requestValidator;
        if(!readingChunks) {

            logger.debug("Received request in Request handler");

            // Construct the Request from messageEvent
            HttpRequest request = this.request = (HttpRequest) messageEvent.getMessage();
            String requestURI = this.request.getUri();
            if(logger.isDebugEnabled()) {
                logger.debug("Request URI: " + requestURI);
            }

            // Figure out the time difference:
            long currentTimeInMs = System.currentTimeMillis();
            long originTimeInMs = Long.parseLong(request.getHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS));
            long diff = currentTimeInMs - originTimeInMs;
            // if(diff > maxLatency) {
            maxLatency = diff;
            System.err.println("Max Time difference in ms = " + maxLatency + " for handler :"
                               + ctx.getPipeline().getChannel().getId());
            // }

            if(request.isChunked()) {
                readingChunks = true;
            } else {
                // Instantiate the appropriate error handler
                HttpMethod httpMethod = request.getMethod();
                if(httpMethod.equals(HttpMethod.GET)) {
                    logger.debug("Received GET request in request handler");
                    requestValidator = new RestGetRequestValidator(request, messageEvent);
                } else if(httpMethod.equals(HttpMethod.POST)) {
                    requestValidator = new RestPutRequestValidator(request,
                                                                   messageEvent,
                                                                   this.isVectorClockOptional);
                } else if(httpMethod.equals(HttpMethod.DELETE)) {
                    requestValidator = new RestDeleteRequestValidator(request,
                                                                      messageEvent,
                                                                      this.isVectorClockOptional);
                } else {
                    String errorMessage = "Illegal Http request.";
                    logger.error(errorMessage);
                    RestErrorHandler.writeErrorResponse(messageEvent, BAD_REQUEST, errorMessage);
                    return;
                }

                registerRequest(requestValidator, ctx, messageEvent);
            }
        } else {
            HttpChunk chunk = (HttpChunk) messageEvent.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
            }
        }
    }

    /**
     * Function used to create a valid request and pass it to the next handler
     * in the Netty pipeline.
     * 
     * @param requestValidator The Validator object used to construct the
     *        request object
     * @param ctx Context of the Netty channel
     * @param messageEvent Message Event used to write the response / exception
     */
    protected abstract void registerRequest(RestRequestValidator requestValidator,
                                            ChannelHandlerContext ctx,
                                            MessageEvent messageEvent);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}

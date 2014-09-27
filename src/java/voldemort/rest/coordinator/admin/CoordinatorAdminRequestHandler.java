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

package voldemort.rest.coordinator.admin;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.rest.coordinator.config.ClientConfigUtil;
import voldemort.rest.coordinator.config.StoreClientConfigService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CoordinatorAdminRequestHandler extends SimpleChannelHandler {

    private static final String STORE_OPS_NAMESPACE = "store-client-config-ops";
    private final StoreClientConfigService storeClientConfigs;
    public HttpRequest request;
    private boolean readingChunks;
    private final Logger logger = Logger.getLogger(CoordinatorAdminRequestHandler.class);

    public CoordinatorAdminRequestHandler(StoreClientConfigService storeClientConfigs) {
        this.storeClientConfigs = storeClientConfigs;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent)
            throws Exception {
        if(!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) messageEvent.getMessage();
            String requestURI = this.request.getUri();
            logger.debug("Admin Request URI: " + requestURI);

            if(request.isChunked()) {
                readingChunks = true;
            } else {
                String[] requestUriSegments = requestURI.split("/");
                HttpResponse response;
                if(requestUriSegments.length > 0
                   && requestUriSegments[1].equals(STORE_OPS_NAMESPACE)) {
                    List<String> storeList = null;
                    if(requestUriSegments.length > 2) {
                        String csvStoreList = requestUriSegments[2];
                        String[] storeArray = csvStoreList.split(",");
                        storeList = Lists.newArrayList(storeArray);
                    }
                    HttpMethod httpMethod = request.getMethod();
                    if(httpMethod.equals(HttpMethod.GET)) {
                        response = handleGet(storeList);
                    } else if(httpMethod.equals(HttpMethod.POST)) {
                        Map<String, Properties> configsToPut = Maps.newHashMap();

                        ChannelBuffer content = this.request.getContent();
                        if(content != null) {
                            byte[] postBody = new byte[content.capacity()];
                            content.readBytes(postBody);
                            configsToPut = ClientConfigUtil.readMultipleClientConfigAvro(new String(postBody));
                        }

                        response = handlePut(configsToPut);
                    } else if(httpMethod.equals(HttpMethod.DELETE)) {
                        response = handleDelete(storeList);
                    } else { // Bad HTTP method
                        response = handleBadRequest("Unsupported HTTP method. Only GET, POST and DELETE are supported.");
                    }
                } else { // Bad namespace
                    response = handleBadRequest("Unsupported namespace. Only /"
                                                + STORE_OPS_NAMESPACE + "/ is supported.");
                }
                messageEvent.getChannel().write(response);
            }
        } else {
            HttpChunk chunk = (HttpChunk) messageEvent.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
            }
        }
    }

    private HttpResponse handleGet(List<String> storeList) {
        logger.info("Received a Http GET Admin request");

        String response;
        if(storeList == null || storeList.isEmpty()) {
            response = storeClientConfigs.getAllConfigs();
        } else {
            response = storeClientConfigs.getSpecificConfigs(storeList);
        }

        HttpResponseStatus responseStatus;

        // FIXME: This sucks. We shouldn't be manually manipulating json...
        if(response.contains(StoreClientConfigService.ERROR_MESSAGE_PARAM_KEY)
           && response.contains(StoreClientConfigService.STORE_NOT_FOUND_ERROR)) {
            responseStatus = NOT_FOUND;
        } else {
            responseStatus = OK;
        }

        return sendResponse(responseStatus, response);
    }

    private HttpResponse handlePut(Map<String, Properties> configsToPut) {
        logger.info("Received a Http POST Admin request");

        String response = storeClientConfigs.putConfigs(configsToPut);

        HttpResponseStatus responseStatus;

        // FIXME: This sucks. We shouldn't be manually manipulating json...
        if(response.contains(StoreClientConfigService.ERROR_MESSAGE_PARAM_KEY)) {
            responseStatus = BAD_REQUEST;
        } else {
            responseStatus = OK;
        }

        return sendResponse(responseStatus, response);
    }

    private HttpResponse handleDelete(List<String> storeList) {
        logger.info("Received a Http DELETE Admin request");

        String response = storeClientConfigs.deleteSpecificConfigs(storeList);

        HttpResponseStatus responseStatus;

        // FIXME: This sucks. We shouldn't be manually manipulating json...
        if(response.contains(StoreClientConfigService.ERROR_MESSAGE_PARAM_KEY)
           && response.contains(StoreClientConfigService.STORE_ALREADY_DOES_NOT_EXIST_WARNING)) {
            responseStatus = NOT_FOUND;
        } else {
            responseStatus = OK;
        }

        return sendResponse(responseStatus, response);
    }

    private HttpResponse handleBadRequest(String errorCause) {
        String errorMessage = "Bad Http Admin request received: " + errorCause;
        logger.error(errorMessage);

        return sendResponse(BAD_REQUEST, errorMessage);
    }

    public HttpResponse sendResponse(HttpResponseStatus responseCode, String responseBody) {
        String actualResponseBody = responseBody + "\n";
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, responseCode);
        response.setHeader(CONTENT_LENGTH, actualResponseBody.length());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(actualResponseBody.getBytes());
        } catch(IOException e) {
            logger.error("IOException while trying to write the outputStream for an admin response",
                         e);
            throw new RuntimeException(e);
        }
        ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
        responseContent.writeBytes(outputStream.toByteArray());
        response.setContent(responseContent);
        logger.debug("Sent " + response);
        return response;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}

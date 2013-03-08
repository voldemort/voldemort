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

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.util.CharsetUtil;

import voldemort.common.VoldemortOpCode;
import voldemort.store.VoldemortRequestWrapper;
import voldemort.utils.ByteArray;

/**
 * A class to handle the HTTP request and execute the same on behalf of the thin
 * client.
 * 
 * Currently, we're using a fat client to handle this request.
 * 
 */
public class VoldemortHttpRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();
    public ChannelBuffer responseContent;
    private Map<String, FatClientWrapper> fatClientMap;
    private final Logger logger = Logger.getLogger(VoldemortHttpRequestHandler.class);
    private String storeName = null;
    private FatClientWrapper fatClientWrapper = null;
    public static final String X_VOLD_REQUEST_TIMEOUT_MS = "X-VOLD-Request-Timeout-ms";
    public static final String X_VOLD_INCONSISTENCY_RESOLVER = "X-VOLD-Inconsistency-Resolver";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";

    // Implicit constructor defined for the derived classes
    public VoldemortHttpRequestHandler() {}

    public VoldemortHttpRequestHandler(Map<String, FatClientWrapper> fatClientMap) {
        this.fatClientMap = fatClientMap;
    }

    /**
     * Function to parse the HTTP headers and build a Voldemort request object
     * 
     * @param requestURI URI of the REST request
     * @param operationType Message Event object used to write the response to
     * @param e The REST (Voldemort) operation type
     * @return true if a valid request was received. False otherwise
     */
    private VoldemortRequestWrapper parseRequest(String requestURI,
                                                 MessageEvent e,
                                                 byte operationType) {
        VoldemortRequestWrapper requestWrapper = null;
        long operationTimeoutInMs = 1500;
        boolean resolveConflicts = true;

        // Retrieve the timeout value from the REST request
        String timeoutValStr = this.request.getHeader(X_VOLD_REQUEST_TIMEOUT_MS);
        if(timeoutValStr != null) {
            try {
                Long.parseLong(timeoutValStr);
            } catch(NumberFormatException nfe) {
                handleBadRequest(e, "Incorrect timeout parameter. Cannot parse this to long: "
                                    + timeoutValStr + ". Details: " + nfe.getMessage());
                return null;
            }
        }

        // Retrieve the inconsistency resolving strategy from the REST request
        String inconsistencyResolverOption = this.request.getHeader(X_VOLD_INCONSISTENCY_RESOLVER);
        if(inconsistencyResolverOption != null) {
            if(inconsistencyResolverOption.equalsIgnoreCase(CUSTOM_RESOLVING_STRATEGY)) {
                resolveConflicts = false;
            } else if(!inconsistencyResolverOption.equalsIgnoreCase(DEFAULT_RESOLVING_STRATEGY)) {
                handleBadRequest(e,
                                 "Invalid Inconsistency Resolving strategy specified in the Request : "
                                         + inconsistencyResolverOption);
                return null;
            }
        }

        // Get the store name from the REST request
        storeName = getStoreName(requestURI);
        this.fatClientWrapper = null;
        if(storeName != null) {
            this.fatClientWrapper = this.fatClientMap.get(storeName);
        }

        if(storeName == null || fatClientWrapper == null) {
            handleBadRequest(e, "Invalid store name. Critical error.");
            return null;
        }

        // Build the request object based on the operation type
        switch(operationType) {
            case VoldemortOpCode.GET_OP_CODE:
                ByteArray getKey = readKey(requestURI);
                requestWrapper = new VoldemortRequestWrapper(getKey,
                                                             operationTimeoutInMs,
                                                             resolveConflicts);
                break;
            case VoldemortOpCode.PUT_OP_CODE:
                ChannelBuffer content = request.getContent();
                if(!content.readable()) {
                    handleBadRequest(e, "Contents not readable");
                    return null;
                }

                ByteArray putKey = readKey(requestURI);
                byte[] putValue = readValue(content);
                requestWrapper = new VoldemortRequestWrapper(putKey, putValue, operationTimeoutInMs);

                break;
            default:
                handleBadRequest(e, "Illegal Operation.");
                return null;
        }

        return requestWrapper;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        if(!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            byte operationType = getOperationType(this.request.getMethod());
            String requestURI = this.request.getUri();
            logger.info(requestURI);

            if(request.isChunked()) {
                readingChunks = true;
            } else {

                VoldemortRequestWrapper requestObject = parseRequest(requestURI, e, operationType);
                if(requestObject == null) {
                    return;
                }

                switch(operationType) {
                    case VoldemortOpCode.GET_OP_CODE:
                        this.fatClientWrapper.submitGetRequest(requestObject, e);
                        break;
                    case VoldemortOpCode.PUT_OP_CODE:
                        this.fatClientWrapper.submitPutRequest(requestObject, e);
                        break;
                    default:
                        String errorMessage = "Illegal operation.";
                        logger.error(errorMessage);
                        RESTErrorHandler.handleError(BAD_REQUEST,
                                                     e,
                                                     isKeepAlive(request),
                                                     errorMessage);
                        return;
                }

            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
                buf.append("END OF CONTENT\r\n");

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                if(!trailer.getHeaderNames().isEmpty()) {
                    buf.append("\r\n");
                    for(String name: trailer.getHeaderNames()) {
                        for(String value: trailer.getHeaders(name)) {
                            buf.append("TRAILING HEADER: " + name + " = " + value + "\r\n");
                        }
                    }
                    buf.append("\r\n");
                }

            } else {
                buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
            }

        }
    }

    /**
     * Send a BAD_REQUEST HTTP error back to the client with the specified
     * message.
     * 
     * @param e Message event to write the error to
     * @param msg Error message
     */
    private void handleBadRequest(MessageEvent e, String msg) {
        String errorMessage = msg;
        logger.error(errorMessage);
        RESTErrorHandler.handleError(BAD_REQUEST, e, false, errorMessage);
    }

    /**
     * Method to determine the operation type
     * 
     * @param httpMethod The HTTP Method object received by the Netty handler
     * @return A voldemortOpCode object representing the operation type
     */
    protected byte getOperationType(HttpMethod httpMethod) {
        if(httpMethod.equals(HttpMethod.POST)) {
            return VoldemortOpCode.PUT_OP_CODE;
        } else if(httpMethod.equals(HttpMethod.GET)) {
            return VoldemortOpCode.GET_OP_CODE;
        }

        return -1;
    }

    /**
     * Method to read a value for a put operation
     * 
     * @param content The ChannelBuffer object containing the value
     * @return The byte[] array representing the value
     */
    private byte[] readValue(ChannelBuffer content) {
        byte[] value = new byte[content.capacity()];
        content.readBytes(value);
        return value;
    }

    /**
     * Method to read a key present in the HTTP request URI
     * 
     * @param requestURI The URI of the HTTP request
     * @return the ByteArray representing the key
     */
    private ByteArray readKey(String requestURI) {
        ByteArray key = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 2) {
            String base64Key = parts[2];
            key = new ByteArray(Base64.decodeBase64(base64Key.getBytes()));
        }
        return key;
    }

    /**
     * Retrieve the store name from the URI
     * 
     * @param requestURI The URI of the HTTP request
     * @return The string representing the store name
     */
    private String getStoreName(String requestURI) {
        String storeName = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 1 && this.fatClientMap.containsKey(parts[1])) {
            storeName = parts[1];
        }

        return storeName;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}

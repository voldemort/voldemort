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

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.common.VoldemortOpCode;
import voldemort.store.CompositeDeleteVoldemortRequest;
import voldemort.store.CompositeGetAllVoldemortRequest;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositePutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;

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
    private Map<String, FatClientWrapper> fatClientMap;
    private final Logger logger = Logger.getLogger(VoldemortHttpRequestHandler.class);
    public static final String X_VOLD_REQUEST_TIMEOUT_MS = "X-VOLD-Request-Timeout-ms";
    public static final String X_VOLD_INCONSISTENCY_RESOLVER = "X-VOLD-Inconsistency-Resolver";
    private static final String X_VOLD_VECTOR_CLOCK = "X-VOLD-Vector-Clock";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";

    private CoordinatorErrorStats errorStats = null;

    // Implicit constructor defined for the derived classes
    public VoldemortHttpRequestHandler() {}

    public VoldemortHttpRequestHandler(Map<String, FatClientWrapper> fatClientMap,
                                       CoordinatorErrorStats errorStats) {
        this.fatClientMap = fatClientMap;
        this.errorStats = errorStats;
    }

    /**
     * Function to parse (and validate) the HTTP headers and build a Voldemort
     * request object
     * 
     * @param requestURI URI of the REST request
     * @param httpMethod Message Event object used to write the response to
     * @param e The REST (Voldemort) operation type
     * @return A composite request object corresponding to the incoming request
     */
    private CompositeVoldemortRequest<ByteArray, byte[]> parseRequest(String requestURI,
                                                                      MessageEvent e,
                                                                      HttpMethod httpMethod) {
        CompositeVoldemortRequest<ByteArray, byte[]> requestWrapper = null;
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

        List<ByteArray> keyList = readKey(requestURI);
        if(keyList == null) {
            handleBadRequest(e, "Error: No key specified !");
            return null;
        }

        byte operationType = getOperationType(httpMethod, keyList);

        // Build the request object based on the operation type
        switch(operationType) {
            case VoldemortOpCode.GET_OP_CODE:
                requestWrapper = new CompositeGetVoldemortRequest<ByteArray, byte[]>(keyList.get(0),
                                                                                     operationTimeoutInMs,
                                                                                     resolveConflicts);
                break;
            case VoldemortOpCode.GET_ALL_OP_CODE:
                requestWrapper = new CompositeGetAllVoldemortRequest<ByteArray, byte[]>(keyList,
                                                                                        operationTimeoutInMs,
                                                                                        resolveConflicts);
                break;

            case VoldemortOpCode.PUT_OP_CODE:
                ChannelBuffer content = request.getContent();
                if(!content.readable()) {
                    handleBadRequest(e, "Contents not readable");
                    return null;
                }

                ByteArray putKey = null;
                if(keyList.size() == 1) {
                    putKey = keyList.get(0);
                } else {
                    handleBadRequest(e, "Cannot have multiple keys in a put operation");
                    return null;
                }
                byte[] putValue = readValue(content);
                requestWrapper = new CompositePutVoldemortRequest<ByteArray, byte[]>(putKey,
                                                                                     putValue,
                                                                                     operationTimeoutInMs);

                break;
            case VoldemortOpCode.DELETE_OP_CODE:
                VectorClock vc = getVectorClock(this.request.getHeader(X_VOLD_VECTOR_CLOCK));
                requestWrapper = new CompositeDeleteVoldemortRequest<ByteArray, byte[]>(keyList.get(0),
                                                                                        vc,
                                                                                        operationTimeoutInMs);

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
            String requestURI = this.request.getUri();
            if(logger.isDebugEnabled()) {
                logger.debug("Request URI: " + requestURI);
            }

            if(request.isChunked()) {
                readingChunks = true;
            } else {

                long startTimeStampInNs = System.nanoTime();

                CompositeVoldemortRequest<ByteArray, byte[]> requestObject = parseRequest(requestURI,
                                                                                          e,
                                                                                          this.request.getMethod());

                // Get the store name from the REST request and the
                // corresponding Fat client
                String storeName = getStoreName(requestURI);
                FatClientWrapper fatClientWrapper = null;
                if(storeName != null) {
                    fatClientWrapper = this.fatClientMap.get(storeName);
                }

                if(storeName == null || fatClientWrapper == null) {
                    this.errorStats.reportException(new IllegalArgumentException());
                    handleBadRequest(e, "Invalid store name. Critical error.");
                    return;
                }

                if(requestObject == null) {
                    this.errorStats.reportException(new IllegalArgumentException());
                    handleBadRequest(e, "Illegal request.");
                    return;
                }

                switch(requestObject.getOperationType()) {
                    case VoldemortOpCode.GET_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming get request");
                        }
                        fatClientWrapper.submitGetRequest(requestObject, e, startTimeStampInNs);
                        break;
                    case VoldemortOpCode.GET_ALL_OP_CODE:
                        fatClientWrapper.submitGetAllRequest(requestObject,
                                                             e,
                                                             storeName,
                                                             startTimeStampInNs);
                        break;

                    case VoldemortOpCode.PUT_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming put request");
                        }
                        fatClientWrapper.submitPutRequest(requestObject, e, startTimeStampInNs);
                        break;
                    case VoldemortOpCode.DELETE_OP_CODE:
                        fatClientWrapper.submitDeleteRequest(requestObject, e, startTimeStampInNs);
                        break;
                    default:
                        String errorMessage = "Illegal operation.";
                        logger.error(errorMessage);
                        RESTErrorHandler.handleError(BAD_REQUEST, e, errorMessage);
                        return;
                }

            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
            }
        }
    }

    /**
     * Parse and return a VectorClock object from the request header
     * 
     * @param vectorClockHeader Header containing the Vector clock in JSON
     *        format
     * @return Equivalent VectorClock object
     */
    private VectorClock getVectorClock(String vectorClockHeader) {
        VectorClock vc = null;
        ObjectMapper mapper = new ObjectMapper();
        if(logger.isDebugEnabled()) {
            logger.debug("Received vector clock : " + vectorClockHeader);
        }
        try {
            VectorClockWrapper vcWrapper = mapper.readValue(vectorClockHeader,
                                                            VectorClockWrapper.class);
            vc = new VectorClock(vcWrapper.getVersions(), vcWrapper.getTimestamp());
        } catch(NullPointerException npe) {
            // npe.printStackTrace();
        } catch(JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return vc;
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
        RESTErrorHandler.handleError(BAD_REQUEST, e, errorMessage);
    }

    /**
     * Method to determine the operation type
     * 
     * @param httpMethod The HTTP Method object received by the Netty handler
     * @param keyList
     * @return A voldemortOpCode object representing the operation type
     */
    protected byte getOperationType(HttpMethod httpMethod, List<ByteArray> keyList) {
        if(httpMethod.equals(HttpMethod.POST)) {
            return VoldemortOpCode.PUT_OP_CODE;
        } else if(httpMethod.equals(HttpMethod.GET)) {
            if(keyList.size() == 1) {
                return VoldemortOpCode.GET_OP_CODE;
            } else if(keyList.size() > 1) {
                return VoldemortOpCode.GET_ALL_OP_CODE;
            }
        } else if(httpMethod.equals(HttpMethod.DELETE)) {
            return VoldemortOpCode.DELETE_OP_CODE;
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
     * Method to read a key (or keys) present in the HTTP request URI. The URI
     * must be of the format /<store_name>/<key>[,<key>,...]
     * 
     * @param requestURI The URI of the HTTP request
     * @return the List<ByteArray> representing the key (or keys)
     */
    private List<ByteArray> readKey(String requestURI) {
        List<ByteArray> keyList = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 2) {
            String base64KeyList = parts[2];
            keyList = new ArrayList<ByteArray>();

            if(!base64KeyList.contains(",")) {
                String rawKey = base64KeyList.trim();
                keyList.add(new ByteArray(Base64.decodeBase64(rawKey.getBytes())));
            } else {
                String[] base64KeyArray = base64KeyList.split(",");
                for(String base64Key: base64KeyArray) {
                    String rawKey = base64Key.trim();
                    keyList.add(new ByteArray(Base64.decodeBase64(rawKey.getBytes())));
                }
            }
        }
        return keyList;
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

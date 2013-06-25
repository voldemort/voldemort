package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import voldemort.VoldemortException;
import voldemort.coordinator.VectorClockWrapper;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;

/**
 * Super class to parse, validate REST requests
 */

public abstract class RestServerErrorHandler {

    protected CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    protected List<ByteArray> parsedKeys;
    protected byte[] parsedValue = null;
    protected VectorClock parsedVectorClock;
    protected long parsedRoutingTimeoutInMs;
    protected byte parsedOperationType;
    protected long parsedRequestOriginTimeInMs;
    protected RequestRoutingType parsedRoutingType = RequestRoutingType.NORMAL;
    protected MessageEvent messageEvent;
    protected HttpRequest request;
    protected StoreRepository storeRepository;
    private final static Logger logger = Logger.getLogger(RestServerErrorHandler.class);

    public RestServerErrorHandler(HttpRequest request,
                                  MessageEvent messageEvent,
                                  StoreRepository storeRepository) {
        this.request = request;
        this.messageEvent = messageEvent;
        this.storeRepository = storeRepository;
    }

    public abstract CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject();

    /**
     * Validations common for all operations are done here
     * 
     * TODO REST-Server 1.Retrieve and validate store
     * 
     * @return true if request is valid else false
     */
    protected boolean parseAndValidateRequest() {
        if(!hasKey() || !hasTimeOutHeader() || !hasRoutingCodeHeader() || !hasTimeStampHeader()
           || hasInconsitencyResolverHeader()) {// || !isStoreValid())
            return false;
        }

        return true;
    }

    /**
     * Exceptions specific to each operation is handled in the corresponding
     * subclass. At this point we don't know the reason behind this exception.
     * 
     * @param exception
     */
    protected void handleExceptions(Exception exception) {
        writeErrorResponse(this.messageEvent,
                           HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Internal Server Error");
    }

    /**
     * Retrieve and validate the timeout value from the REST request.
     * "X_VOLD_REQUEST_TIMEOUT_MS" is the timeout header.
     * 
     * TODO REST-Server 1. check for negative/zero values ? 2. Use timeout
     * header to filter expired requests
     * 
     * @return true if present, false if missing
     */
    protected boolean hasTimeOutHeader() {

        boolean result = false;
        String timeoutValStr = this.request.getHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS);
        if(timeoutValStr != null) {
            try {
                this.parsedRoutingTimeoutInMs = Long.parseLong(timeoutValStr);
                result = true;
            } catch(NumberFormatException nfe) {
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Incorrect timeout parameter. Cannot parse this to long: "
                                           + timeoutValStr);
                result = false;
            }
        } else {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing timeout parameter.");
            result = false;
        }
        return result;
    }

    /**
     * Retrieve and validate the routing type value from the REST request.
     * "X_VOLD_ROUTING_TYPE_CODE" is the routing type header.
     * 
     * TODO REST-Server 1. Change the header name to a better name. 2. Assumes
     * that integer is passed in the header
     * 
     * @return true if present, false if missing
     */
    protected boolean hasRoutingCodeHeader() {

        String rtCode = this.request.getHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE);
        boolean result = false;
        if(rtCode != null) {
            try {
                int routingTypeCode = Integer.parseInt(rtCode);
                this.parsedRoutingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
                result = true;
            } catch(NumberFormatException nfe) {
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Incorrect routing type parameter. Cannot parse this to long: "
                                           + rtCode);
                result = false;
            } catch(VoldemortException ve) {
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Incorrect routing type code: " + rtCode);
                result = false;
            }
        } else {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing routing type parameter.");
            result = false;
        }
        return result;
    }

    /**
     * Retrieve and validate the timestamp value from the REST request.
     * "X_VOLD_REQUEST_ORIGIN_TIME_MS" is timestamp header
     * 
     * TODO REST-Server 1. Change Time stamp header to a better name. 2. Check
     * for zero/negative value?
     * 
     * @return true if present, false if missing
     */
    protected boolean hasTimeStampHeader() {
        String originTime = request.getHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS);
        boolean result = false;
        if(originTime != null) {
            try {
                this.parsedRequestOriginTimeInMs = Long.parseLong(originTime);
                result = true;
            } catch(NumberFormatException nfe) {
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Incorrect origin time parameter. Cannot parse this to long: "
                                           + originTime);
                result = false;
            }
        } else {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing origin time parameter.");
            result = false;
        }
        return result;
    }

    /**
     * Check if inconsistency resolver option is present in the REST request.
     * "X_VOLD_INCONSISTENCY_RESOLVER" is the inconsistency resolver header
     * 
     * TODO REST-Server 1. Should deal with empty string not just null
     * 
     * @return true if present, false if missing
     */
    protected boolean hasInconsitencyResolverHeader() {
        String inconsistencyResolverOption = request.getHeader(RestMessageHeaders.X_VOLD_INCONSISTENCY_RESOLVER);
        boolean result = false;

        if(inconsistencyResolverOption != null) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Cannot have Inconsitence resolver header");
            result = true;
        }
        return result;
    }

    /**
     * Retrieve and validate vector clock value from the REST request.
     * "X_VOLD_VECTOR_CLOCK" is the vector clock header.
     * 
     * @return true if present, false if missing
     */
    protected boolean hasVectorClock() {
        boolean result = false;
        String vectorClockHeader = this.request.getHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK);
        if(vectorClockHeader != null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                VectorClockWrapper vcWrapper = mapper.readValue(vectorClockHeader,
                                                                VectorClockWrapper.class);
                this.parsedVectorClock = new VectorClock(vcWrapper.getVersions(),
                                                         vcWrapper.getTimestamp());
                result = true;
            } catch(Exception e) {
                logger.error("Exception while parsing and constructing vector clock", e);
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Invalid Vector Clock");
            }
        } else {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing Vector Clock");
        }
        return result;
    }

    /**
     * Retrieve and validate the key from the REST request.
     * 
     * TODO REST-Server empty or null check instead of just checking for null?
     * 
     * @return true if present, false if missing
     */
    protected boolean hasKey() {
        boolean result = false;
        String requestURI = this.request.getUri();
        parseKeys(requestURI);

        if(this.parsedKeys == null) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Error: No key specified !");
            result = false;
        } else
            result = true;
        return result;
    }

    /**
     * Method to read a key (or keys) present in the HTTP request URI. The URI
     * must be of the format /<store_name>/<key>[,<key>,...]
     * 
     * @param requestURI The URI of the HTTP request
     */
    protected void parseKeys(String requestURI) {

        this.parsedKeys = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 2) {
            String base64KeyList = parts[2];
            this.parsedKeys = new ArrayList<ByteArray>();

            if(!base64KeyList.contains(",")) {
                String rawKey = base64KeyList.trim();
                this.parsedKeys.add(new ByteArray(Base64.decodeBase64(rawKey.getBytes())));
            } else {
                String[] base64KeyArray = base64KeyList.split(",");
                for(String base64Key: base64KeyArray) {
                    String rawKey = base64Key.trim();
                    this.parsedKeys.add(new ByteArray(Base64.decodeBase64(rawKey.getBytes())));
                }
            }
        }
    }

    /**
     * Retrieve and validate store name from the REST request.
     * 
     * @return true if valid, false otherwise
     */

    protected boolean isStoreValid() {
        boolean result = true;
        String requestURI = this.request.getUri();
        String storeName = getStoreName(requestURI);
        if(storeName == null) {
            result = false;
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing store name. Critical error.");
        } else if(!this.storeRepository.hasLocalStore(storeName)) {
            result = false;
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Invalid store name. Critical error.");
        }
        return result;
    }

    /**
     * Parses the store name HTTP request URI. The URI must be of the format
     * /<store_name>/<key>[,<key>,...]
     * 
     * @param requestURI
     * @return a String representing store name
     */
    protected String getStoreName(String requestURI) {
        String storeName = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 1) {
            storeName = parts[1];
        }
        return storeName;
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
     * Writes all error responses to the client.
     * 
     * TODO REST-Server 1. collect error stats
     * 
     * @param messageEvent - for retrieving the channel details
     * @param status - error code
     * @param message - error message
     */
    public static void writeErrorResponse(MessageEvent messageEvent,
                                          HttpResponseStatus status,
                                          String message) {

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.copiedBuffer("Failure: " + status.toString() + ". "
                                                        + message + "\r\n", CharsetUtil.UTF_8));

        // Write the response to the Netty Channel
        messageEvent.getChannel().write(response);
    }
}

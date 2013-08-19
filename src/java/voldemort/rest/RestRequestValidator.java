package voldemort.rest;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.VoldemortException;
import voldemort.server.RequestRoutingType;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;

/**
 * Super class to parse, validate REST requests
 */

public abstract class RestRequestValidator {

    protected CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    protected String storeName = null;
    protected List<ByteArray> parsedKeys;
    protected byte[] parsedValue = null;
    protected VectorClock parsedVectorClock;
    protected long parsedTimeoutInMs;
    protected byte parsedOperationType;
    protected long parsedRequestOriginTimeInMs;
    protected RequestRoutingType parsedRoutingType = null;
    protected MessageEvent messageEvent;
    protected HttpRequest request;
    protected final Logger logger = Logger.getLogger(getClass());

    public RestRequestValidator(HttpRequest request, MessageEvent messageEvent) {
        this.request = request;
        this.messageEvent = messageEvent;
        this.parsedRoutingType = RequestRoutingType.NORMAL;
    }

    public abstract CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject();

    /**
     * Validations common for all operations are done here
     * 
     * @return true if request is valid else false
     */
    protected boolean parseAndValidateRequest() {
        if(!hasKey() || !hasTimeOutHeader() || !hasTimeStampHeader() || !isStoreValid()) {
            return false;
        }

        // Retrieve the routing code from the header
        parseRoutingCodeHeader();

        return true;
    }

    /**
     * Retrieve and validate the timeout value from the REST request.
     * "X_VOLD_REQUEST_TIMEOUT_MS" is the timeout header.
     * 
     * @return true if present, false if missing
     */
    protected boolean hasTimeOutHeader() {

        boolean result = false;
        String timeoutValStr = this.request.getHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS);
        if(timeoutValStr != null) {
            try {
                this.parsedTimeoutInMs = Long.parseLong(timeoutValStr);
                if(this.parsedTimeoutInMs < 0) {
                    RestErrorHandler.writeErrorResponse(messageEvent,
                                                        HttpResponseStatus.BAD_REQUEST,
                                                        "Time out cannot be negative ");

                } else {
                    result = true;
                }
            } catch(NumberFormatException nfe) {
                logger.error("Exception when validating request. Incorrect timeout parameter. Cannot parse this to long: "
                                     + timeoutValStr,
                             nfe);
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    "Incorrect timeout parameter. Cannot parse this to long: "
                                                            + timeoutValStr);
            }
        } else {
            logger.error("Error when validating request. Missing timeout parameter.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Missing timeout parameter.");
        }
        return result;
    }

    /**
     * Retrieve the routing type value from the REST request.
     * "X_VOLD_ROUTING_TYPE_CODE" is the routing type header.
     * 
     * By default, the routing code is set to NORMAL
     * 
     * TODO REST-Server 1. Change the header name to a better name. 2. Assumes
     * that integer is passed in the header
     * 
     */
    protected void parseRoutingCodeHeader() {

        String rtCode = this.request.getHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE);
        if(rtCode != null) {
            try {
                int routingTypeCode = Integer.parseInt(rtCode);
                this.parsedRoutingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
            } catch(NumberFormatException nfe) {
                logger.error("Exception when validating request. Incorrect routing type parameter. Cannot parse this to long: "
                                     + rtCode,
                             nfe);
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    "Incorrect routing type parameter. Cannot parse this to long: "
                                                            + rtCode);
            } catch(VoldemortException ve) {
                logger.error("Exception when validating request. Incorrect routing type code: "
                             + rtCode, ve);
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    "Incorrect routing type code: " + rtCode);
            }
        }
    }

    /**
     * Retrieve and validate the timestamp value from the REST request.
     * "X_VOLD_REQUEST_ORIGIN_TIME_MS" is timestamp header
     * 
     * TODO REST-Server 1. Change Time stamp header to a better name.
     * 
     * @return true if present, false if missing
     */
    protected boolean hasTimeStampHeader() {
        String originTime = request.getHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS);
        boolean result = false;
        if(originTime != null) {
            try {
                this.parsedRequestOriginTimeInMs = Long.parseLong(originTime);
                if(this.parsedRequestOriginTimeInMs < 0) {
                    RestErrorHandler.writeErrorResponse(messageEvent,
                                                        HttpResponseStatus.BAD_REQUEST,
                                                        "Origin time cannot be negative ");

                } else {
                    result = true;
                }
            } catch(NumberFormatException nfe) {
                logger.error("Exception when validating request. Incorrect origin time parameter. Cannot parse this to long: "
                                     + originTime,
                             nfe);
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    "Incorrect origin time parameter. Cannot parse this to long: "
                                                            + originTime);
            }
        } else {
            logger.error("Error when validating request. Missing origin time parameter.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Missing origin time parameter.");
        }
        return result;
    }

    /**
     * Retrieve and validate vector clock value from the REST request.
     * "X_VOLD_VECTOR_CLOCK" is the vector clock header.
     * 
     * @return true if present, false if missing
     */
    protected boolean hasVectorClock(boolean isVectorClockOptional) {
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
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    "Invalid Vector Clock");
            }
        } else if(!isVectorClockOptional) {
            logger.error("Error when validating request. Missing Vector Clock");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Missing Vector Clock");
        } else {
            result = true;
        }

        return result;
    }

    /**
     * Retrieve and validate the key from the REST request.
     * 
     * @return true if present, false if missing
     */
    protected boolean hasKey() {
        boolean result = false;
        String requestURI = this.request.getUri();
        parseKeys(requestURI);

        if(this.parsedKeys != null) {
            result = true;
        } else {
            logger.error("Error when validating request. No key specified.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Error: No key specified !");
        }
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
                this.parsedKeys.add(new ByteArray(RestUtils.decodeVoldemortKey(rawKey)));
            } else {
                String[] base64KeyArray = base64KeyList.split(",");
                for(String base64Key: base64KeyArray) {
                    String rawKey = base64Key.trim();
                    this.parsedKeys.add(new ByteArray(RestUtils.decodeVoldemortKey(rawKey)));
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
        boolean result = false;
        String requestURI = this.request.getUri();
        this.storeName = parseStoreName(requestURI);
        if(storeName != null) {
            result = true;
        } else {
            logger.error("Error when validatig request. Missing store name.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Missing store name. Critical error.");
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
    protected String parseStoreName(String requestURI) {
        String storeName = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 1) {
            storeName = parts[1];
        }
        return storeName;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public RequestRoutingType getParsedRoutingType() {
        return parsedRoutingType;
    }

}

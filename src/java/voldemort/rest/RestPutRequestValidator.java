package voldemort.rest;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.store.CompositePutVoldemortRequest;
import voldemort.store.CompositeVersionedPutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * This class is used to parse an incoming put request. Parses and validates the
 * REST Request and constructs a CompositeVoldemortRequestObject. Also Handles
 * exceptions specific to put operation.
 */
public class RestPutRequestValidator extends RestRequestValidator {

    private final boolean isVectorClockOptional;

    public RestPutRequestValidator(HttpRequest request,
                                         MessageEvent messageEvent,
                                         boolean isVectorClockOptional) {
        super(request, messageEvent);
        this.isVectorClockOptional = isVectorClockOptional;
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        CompositeVoldemortRequest<ByteArray, byte[]> requestObject = null;
        if(parseAndValidateRequest()) {
            parseValue();
            if(this.parsedValue != null) {

                // Check if we have a valid vector clock
                if(this.parsedVectorClock == null
                   || this.parsedVectorClock.getEntries().size() == 0) {
                    requestObject = new CompositePutVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                        this.parsedValue,
                                                                                        this.parsedTimeoutInMs,
                                                                                        this.parsedRequestOriginTimeInMs,
                                                                                        this.parsedRoutingType);
                } else {

                    requestObject = new CompositeVersionedPutVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                                 new Versioned<byte[]>(this.parsedValue,
                                                                                                                       this.parsedVectorClock),
                                                                                                 this.parsedTimeoutInMs,
                                                                                                 this.parsedRequestOriginTimeInMs,
                                                                                                 this.parsedRoutingType);
                }

                return requestObject;

            } else {
                logger.error("Error when parsing value. Value cannot be null.");
                RestErrorHandler.writeErrorResponse(messageEvent,
                                                          HttpResponseStatus.BAD_REQUEST,
                                                          "Value cannot be null");
            }
        }
        // Return null if request is not valid
        return null;
    }

    /**
     * Validations specific to PUT
     */
    @Override
    public boolean parseAndValidateRequest() {
        boolean result = false;
        if(!super.parseAndValidateRequest() || !hasVectorClock(this.isVectorClockOptional)
           || !hasContentLength() || !hasContentType()) {
            result = false;
        } else {
            result = true;
        }

        return result;
    }

    /**
     * Retrieves and validates the content length from the REST request.
     * 
     * @return true if has content length
     */
    protected boolean hasContentLength() {
        boolean result = false;
        String contentLength = this.request.getHeader(RestMessageHeaders.CONTENT_LENGTH);
        if(contentLength != null) {
            try {
                Long.parseLong(contentLength);
                result = true;
            } catch(NumberFormatException nfe) {
                logger.error("Exception when validating put request. Incorrect content length parameter. Cannot parse this to long: "
                                     + contentLength + ". Details: " + nfe.getMessage(),
                             nfe);
                RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                          HttpResponseStatus.BAD_REQUEST,
                                                          "Incorrect content length parameter. Cannot parse this to long: "
                                                                  + contentLength + ". Details: "
                                                                  + nfe.getMessage());
            }
        } else {
            logger.error("Error when validating put request. Missing Content-Length header.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                      HttpResponseStatus.BAD_REQUEST,
                                                      "Missing Content-Length header");
        }

        return result;
    }

    /**
     * Retrieves and validates the content type from the REST requests
     * 
     * @return true if has content type.
     */
    protected boolean hasContentType() {

        boolean result = false;
        if(this.request.getHeader(RestMessageHeaders.CONTENT_TYPE) != null) {
            result = true;
        } else {
            logger.error("Error when validating put request. Missing Content-Type header.");
            RestErrorHandler.writeErrorResponse(this.messageEvent,
                                                      HttpResponseStatus.BAD_REQUEST,
                                                      "Missing Content-Type header");
        }
        return result;
    }

    /**
     * Retrieve the value from the REST request body.
     * 
     * TODO: REST-Server value cannot be null ( null/empty string ?)
     */
    private void parseValue() {
        ChannelBuffer content = this.request.getContent();
        this.parsedValue = new byte[content.capacity()];
        content.readBytes(parsedValue);
    }
}

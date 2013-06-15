package voldemort.server.rest;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.VoldemortUnsupportedOperationalException;
import voldemort.server.StoreRepository;
import voldemort.store.CompositeVersionedPutVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.rebalancing.ProxyUnreachableException;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * This class is used to parse an incoming put request. Parses and validates the
 * REST Request and constructs a CompositeVoldemortRequestObject. Also Handles
 * exceptions specific to put operation.
 */
public class RestServerPutRequestErrorHandler extends RestServerErrorHandler {

    public RestServerPutRequestErrorHandler(HttpRequest request,
                                            MessageEvent messageEvent,
                                            StoreRepository storeRepository) {
        super(request, messageEvent, storeRepository);
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        CompositeVoldemortRequest<ByteArray, byte[]> requestObject = null;
        if(parseAndValidateRequest()) {
            parseValue();
            if(this.parsedValue != null) {
                requestObject = new CompositeVersionedPutVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                             new Versioned(this.parsedValue,
                                                                                                           this.parsedVectorClock),
                                                                                             this.parsedRoutingTimeoutInMs,
                                                                                             this.parsedRequestOriginTimeInMs,
                                                                                             this.parsedRoutingType);
                return requestObject;
            }
        }
        // Return null if request is not valid
        return null;
    }

    /**
     * Handle exceptions thrown by the storage. Exceptions specific to PUT go
     * here. Pass other exceptions to the parent class
     * 
     * TODO REST-Server Add a new exception for this condition - server busy
     * with pending requests. queue is full
     */
    @Override
    public void handleExceptions(Exception exception) {

        if(exception instanceof InvalidMetadataException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                               "The requested key does not exist in this partition");
        } else if(exception instanceof PersistenceFailureException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               "TOperation failed");
        } else if(exception instanceof ProxyUnreachableException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.SERVICE_UNAVAILABLE,
                               "The proxy is unreachable");
        } else if(exception instanceof VoldemortUnsupportedOperationalException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.METHOD_NOT_ALLOWED,
                               "operation not supported in read-only store");
        } else if(exception instanceof ObsoleteVersionException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.PRECONDITION_FAILED,
                               "A put request resulted in an ObsoleteVersionException");
        } else {
            super.handleExceptions(exception);
        }
    }

    /**
     * Validations specific to PUT
     */
    @Override
    public boolean parseAndValidateRequest() {
        boolean result = false;
        if(!super.parseAndValidateRequest() || !hasVectorClock() || !hasContentLength()
           || !hasContentType()) {
            result = false;
        } else
            result = true;

        return result;
    }

    /**
     * Retrieves and validates the content length from the REST request.
     * 
     * @return
     */
    protected boolean hasContentLength() {
        boolean result = false;
        String contentLength = this.request.getHeader(RestMessageHeaders.CONTENT_LENGTH);
        if(contentLength != null) {
            try {
                Long.parseLong(contentLength);
                result = true;
            } catch(NumberFormatException nfe) {
                writeErrorResponse(this.messageEvent,
                                   HttpResponseStatus.BAD_REQUEST,
                                   "Incorrect content length parameter. Cannot parse this to long: "
                                           + contentLength + ". Details: " + nfe.getMessage());
            }
        } else
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing Content-Length header");

        return result;
    }

    /**
     * Retrieves and validates the content type from the REST request
     * 
     * 
     * TODO REST-Server Should check for valid content type (only binary
     * allowed)
     * 
     * @return
     */
    protected boolean hasContentType() {

        boolean result = false;
        if(this.request.getHeader(RestMessageHeaders.CONTENT_TYPE) != null) {
            result = true;
        } else {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.BAD_REQUEST,
                               "Missing Content-Type header");
        }
        return result;
    }

    /**
     * Retrieve the value from the REST request body.
     * 
     * TODO REST-Server Is null/empty allowed for a value?
     */
    private void parseValue() {
        ChannelBuffer content = this.request.getContent();
        this.parsedValue = new byte[content.capacity()];
        content.readBytes(parsedValue);
    }
}

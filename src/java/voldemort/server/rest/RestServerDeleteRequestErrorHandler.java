package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.server.StoreRepository;
import voldemort.store.CompositeDeleteVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse an incoming delete request. Parses and validates
 * the REST Request and constructs a CompositeVoldemortRequestObject. Also
 * handles exceptions specific to delete operation.
 */
public class RestServerDeleteRequestErrorHandler extends RestServerErrorHandler {

    public RestServerDeleteRequestErrorHandler(HttpRequest request,
                                               MessageEvent messageEvent,
                                               StoreRepository storeRepository) {
        super(request, messageEvent, storeRepository);
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
            this.requestObject = new CompositeDeleteVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                        this.parsedVectorClock,
                                                                                        this.parsedRoutingTimeoutInMs,
                                                                                        this.parsedRequestOriginTimeInMs,
                                                                                        this.parsedRoutingType);
            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

    /**
     * Handle exceptions thrown by the storage. Exceptions specific to DELETE go
     * here. Pass other exceptions to the parent class.
     * 
     * TODO REST-Server Add a new exception for this condition - server busy
     * with pending requests. queue is full
     * 
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
        } else if(exception instanceof UnsupportedOperationException) {
            writeErrorResponse(this.messageEvent,
                               HttpResponseStatus.METHOD_NOT_ALLOWED,
                               "operation not supported in read-only store");
        } else {
            super.handleExceptions(exception);
        }
    }

    /**
     * Validations specific to DELETE
     */
    @Override
    public boolean parseAndValidateRequest() {
        if(!super.parseAndValidateRequest() || !hasVectorClock()) {
            return false;
        }

        return true;
    }
}

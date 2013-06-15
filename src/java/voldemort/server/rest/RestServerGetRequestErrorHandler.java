package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.server.StoreRepository;
import voldemort.store.CompositeGetAllVoldemortRequest;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.rebalancing.ProxyUnreachableException;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse incoming get and get all requests. Parses and
 * validates the REST Request and constructs a CompositeVoldemortRequestObject.
 * Also Handles exceptions specific to get and get all operations.
 */
public class RestServerGetRequestErrorHandler extends RestServerErrorHandler {

    public RestServerGetRequestErrorHandler(HttpRequest request,
                                            MessageEvent messageEvent,
                                            StoreRepository storeRepository) {
        super(request, messageEvent, storeRepository);
    }

    /**
     * Validations specific to GET and GET ALL
     */
    @Override
    public boolean parseAndValidateRequest() {
        if(!super.parseAndValidateRequest()) {
            return false;
        }

        return true;
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
            if(this.parsedKeys.size() > 1) {
                this.requestObject = new CompositeGetAllVoldemortRequest<ByteArray, byte[]>(this.parsedKeys,
                                                                                            this.parsedRoutingTimeoutInMs,
                                                                                            this.parsedRequestOriginTimeInMs,
                                                                                            this.parsedRoutingType);
            } else {
                this.requestObject = new CompositeGetVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                         this.parsedRoutingTimeoutInMs,
                                                                                         this.parsedRequestOriginTimeInMs,
                                                                                         this.parsedRoutingType);
            }
            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

    /**
     * Handle exceptions thrown by the storage. Exceptions specific to
     * GET/GETALL go here. Pass other exceptions to the parent class
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
        } else {
            super.handleExceptions(exception);
        }
    }
}

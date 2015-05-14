package voldemort.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreTimeoutException;

public class RestDeleteErrorHandler extends RestErrorHandler {

    /**
     * Handle exceptions thrown by the storage. Exceptions specific to DELETE go
     * here. Pass other exceptions to the parent class.
     * 
     * TODO REST-Server Add a new exception for this condition - server busy
     * with pending requests. queue is full
     * 
     */

    @Override
    public void handleExceptions(MessageEvent messageEvent, Exception exception) {

        if(exception instanceof InvalidMetadataException) {
            logger.error("Exception when deleting. The requested key does not exist in this partition",
                         exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                               "The requested key does not exist in this partition");
        } else if(exception instanceof PersistenceFailureException) {
            logger.error("Exception when deleting. Operation failed", exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               "Operation failed");
        } else if(exception instanceof UnsupportedOperationException) {
            logger.error("Exception when deleting. Operation not supported in read-only store ",
                         exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.METHOD_NOT_ALLOWED,
                               "Operation not supported in read-only store");
        } else if(exception instanceof StoreTimeoutException) {
            String errorDescription = "DELETE Request timed out: " + exception.getMessage();
            logger.error(errorDescription);
            writeErrorResponse(messageEvent, HttpResponseStatus.REQUEST_TIMEOUT, errorDescription);
        } else if(exception instanceof InsufficientOperationalNodesException) {
            String errorDescription = "DELETE Request failed: " + exception.getMessage();
            logger.error(errorDescription);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               errorDescription);
        } else {
            super.handleExceptions(messageEvent, exception);
        }
    }
}

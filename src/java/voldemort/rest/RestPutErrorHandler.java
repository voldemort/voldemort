package voldemort.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.VoldemortUnsupportedOperationalException;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreTimeoutException;
import voldemort.store.rebalancing.ProxyUnreachableException;
import voldemort.versioning.ObsoleteVersionException;

public class RestPutErrorHandler extends RestErrorHandler {

    /**
     * Handle exceptions thrown by the storage. Exceptions specific to PUT go
     * here. Pass other exceptions to the parent class
     * 
     * TODO REST-Server Add a new exception for this condition - server busy
     * with pending requests. queue is full
     */
    @Override
    public void handleExceptions(MessageEvent messageEvent, Exception exception) {

        if(exception instanceof InvalidMetadataException) {
            logger.error("Exception when doing put. The requested key does not exist in this partition.",
                         exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                               "The requested key does not exist in this partition");
        } else if(exception instanceof PersistenceFailureException) {
            logger.error("Exception when doing put. Operation failed", exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               "Operation failed");
        } else if(exception instanceof ProxyUnreachableException) {
            logger.error("Exception when doing put. The proxy is unreachable.", exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.SERVICE_UNAVAILABLE,
                               "The proxy is unreachable");
        } else if(exception instanceof VoldemortUnsupportedOperationalException) {
            logger.error("Exception when doing put. Operation not supported in read-only store.",
                         exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.METHOD_NOT_ALLOWED,
                               "Operation not supported in read-only store");
        } else if(exception instanceof ObsoleteVersionException) {
            logger.error("Exception when doing put. Request resulted in an ObsoleteVersionException",
                         exception);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.PRECONDITION_FAILED,
                               "A put request resulted in an ObsoleteVersionException");
        } else if(exception instanceof StoreTimeoutException) {
            String errorDescription = "PUT Request timed out: " + exception.getMessage();
            logger.error(errorDescription);
            writeErrorResponse(messageEvent, HttpResponseStatus.REQUEST_TIMEOUT, errorDescription);
        } else if(exception instanceof InsufficientOperationalNodesException) {
            String errorDescription = "PUT Request failed: " + exception.getMessage();
            logger.error(errorDescription);
            writeErrorResponse(messageEvent,
                               HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               errorDescription);
        } else {
            super.handleExceptions(messageEvent, exception);
        }
    }
}

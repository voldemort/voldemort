package voldemort.rest;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.store.CompositeDeleteVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse an incoming delete request. Parses and validates
 * the REST Request and constructs a CompositeVoldemortRequestObject. Also
 * handles exceptions specific to delete operation.
 */
public class RestDeleteRequestValidator extends RestRequestValidator {

    private final boolean isVectorClockOptional;
    private final Logger logger = Logger.getLogger(RestDeleteRequestValidator.class);

    public RestDeleteRequestValidator(HttpRequest request,
                                      MessageEvent messageEvent,
                                      boolean isVectorClockOptional) {
        super(request, messageEvent);
        this.isVectorClockOptional = isVectorClockOptional;
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
            if(logger.isDebugEnabled()) {
                debugLog("DELETE", System.currentTimeMillis());
            }
            this.requestObject = new CompositeDeleteVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                        this.parsedVectorClock,
                                                                                        this.parsedTimeoutInMs,
                                                                                        this.parsedRequestOriginTimeInMs,
                                                                                        this.parsedRoutingType);
            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

    /**
     * Validations specific to DELETE
     */
    @Override
    public boolean parseAndValidateRequest() {
        if(!super.parseAndValidateRequest() || !hasVectorClock(this.isVectorClockOptional)) {
            return false;
        }

        return true;
    }
}

package voldemort.server.rest;

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
public class RestServerDeleteRequestValidator extends RestServerRequestValidator {

    public RestServerDeleteRequestValidator(HttpRequest request, MessageEvent messageEvent) {
        super(request, messageEvent);
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
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
        if(!super.parseAndValidateRequest() || !hasVectorClock()) {
            return false;
        }

        return true;
    }
}

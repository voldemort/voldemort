package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.server.StoreRepository;
import voldemort.store.CompositeGetVersionVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse an incoming Get Version request. Parses and
 * validates the REST Request and constructs a CompositeVoldemortRequestObject.
 * Also Handles exceptions specific to Get Version operation.
 */
public class RestServerGetVersionRequestValidator extends RestServerRequestValidator {

    public RestServerGetVersionRequestValidator(HttpRequest request,
                                                MessageEvent messageEvent,
                                                StoreRepository storeRepository) {
        super(request, messageEvent, storeRepository);
    }

    /**
     * Validations specific to GET VERSION
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
            this.requestObject = new CompositeGetVersionVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                            this.parsedRoutingTimeoutInMs,
                                                                                            this.parsedRequestOriginTimeInMs,
                                                                                            this.parsedRoutingType);

            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

}

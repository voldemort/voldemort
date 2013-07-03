package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.server.StoreRepository;
import voldemort.store.CompositeGetAllVoldemortRequest;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse incoming get and get all requests. Parses and
 * validates the REST Request and constructs a CompositeVoldemortRequestObject.
 * Also Handles exceptions specific to get and get all operations.
 */
public class RestServerGetRequestValidator extends RestServerRequestValidator {

    public RestServerGetRequestValidator(HttpRequest request,
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

}

package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.server.StoreRepository;
import voldemort.store.CompositeGetAllVoldemortRequest;
import voldemort.store.CompositeGetVersionVoldemortRequest;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

/**
 * This class is used to parse incoming get and get all requests. Parses and
 * validates the REST Request and constructs a CompositeVoldemortRequestObject.
 * Also Handles exceptions specific to get and get all operations.
 */
public class RestServerGetRequestValidator extends RestServerRequestValidator {

    protected boolean isGetVersionRequest = false;

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
        isGetVersionRequest = hasGetVersionRequestHeader();
        if(isGetVersionRequest && this.parsedKeys.size() > 1) {
            RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                      HttpResponseStatus.BAD_REQUEST,
                                                      "Get version request cannot have multiple keys");
            return false;
        }
        return true;
    }

    public boolean hasGetVersionRequestHeader() {
        boolean result = false;
        String headerValue = this.request.getHeader(RestMessageHeaders.X_VOLD_GET_VERSION);
        if(headerValue != null) {
            result = true;
        }
        return result;
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
            if(this.isGetVersionRequest) {
                this.requestObject = new CompositeGetVersionVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                                this.parsedTimeoutInMs,
                                                                                                this.parsedRequestOriginTimeInMs,
                                                                                                this.parsedRoutingType);

            } else if(this.parsedKeys.size() > 1) {
                this.requestObject = new CompositeGetAllVoldemortRequest<ByteArray, byte[]>(this.parsedKeys,
                                                                                            this.parsedTimeoutInMs,
                                                                                            this.parsedRequestOriginTimeInMs,
                                                                                            this.parsedRoutingType);
            } else {
                this.requestObject = new CompositeGetVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                         this.parsedTimeoutInMs,
                                                                                         this.parsedRequestOriginTimeInMs,
                                                                                         this.parsedRoutingType);
            }
            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

}

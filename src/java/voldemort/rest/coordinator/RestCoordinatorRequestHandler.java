package voldemort.rest.coordinator;

import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.common.VoldemortOpCode;
import voldemort.rest.AbstractRestRequestHandler;
import voldemort.rest.RestErrorHandler;
import voldemort.rest.RestMessageHeaders;
import voldemort.rest.RestRequestValidator;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

public class RestCoordinatorRequestHandler extends AbstractRestRequestHandler {

    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap = null;
    private final Logger logger = Logger.getLogger(RestCoordinatorRequestHandler.class);

    public RestCoordinatorRequestHandler(Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap) {
        super(true);
        this.fatClientMap = fatClientMap;
    }

    /**
     * Constructs a valid request and passes it on to the next handler. It also
     * creates the 'StoreClient' object corresponding to the store name
     * specified in the REST request.
     * 
     * @param requestValidator The Validator object used to construct the
     *        request object
     * @param ctx Context of the Netty channel
     * @param messageEvent Message Event used to write the response / exception
     */
    @Override
    protected void registerRequest(RestRequestValidator requestValidator,
                                   ChannelHandlerContext ctx,
                                   MessageEvent messageEvent) {

        // At this point we know the request is valid and we have a
        // error handler. So we construct the composite Voldemort
        // request object.
        CompositeVoldemortRequest<ByteArray, byte[]> requestObject = requestValidator.constructCompositeVoldemortRequestObject();

        if(requestObject != null) {

            DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient = null;

            if(!requestValidator.getStoreName().equalsIgnoreCase(RestMessageHeaders.SCHEMATA_STORE)) {

                storeClient = this.fatClientMap.get(requestValidator.getStoreName());
                if(storeClient == null) {
                    logger.error("Error when getting store. Non Existing store client.");
                    RestErrorHandler.writeErrorResponse(messageEvent,
                                                        HttpResponseStatus.BAD_REQUEST,
                                                        "Non Existing store client. Critical error.");
                    return;
                }
            } else {
                requestObject.setOperationType(VoldemortOpCode.GET_METADATA_OP_CODE);
            }

            CoordinatorStoreClientRequest coordinatorRequest = new CoordinatorStoreClientRequest(requestObject,
                                                                                                 storeClient);
            Channels.fireMessageReceived(ctx, coordinatorRequest);

        }
    }
}

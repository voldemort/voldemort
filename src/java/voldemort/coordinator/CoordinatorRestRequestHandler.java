package voldemort.coordinator;

import java.util.Map;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.server.rest.RestServerErrorHandler;
import voldemort.server.rest.RestServerRequestValidator;
import voldemort.server.rest.VoldemortRestRequestHandler;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

public class CoordinatorRestRequestHandler extends VoldemortRestRequestHandler implements
        ChannelHandler {

    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap = null;
    private final Logger logger = Logger.getLogger(CoordinatorRestRequestHandler.class);

    public CoordinatorRestRequestHandler(Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap) {
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
    protected void registerRequest(RestServerRequestValidator requestValidator,
                                   ChannelHandlerContext ctx,
                                   MessageEvent messageEvent) {

        // At this point we know the request is valid and we have a
        // error handler. So we construct the composite Voldemort
        // request object.
        CompositeVoldemortRequest<ByteArray, byte[]> requestObject = requestValidator.constructCompositeVoldemortRequestObject();

        if(requestObject != null) {
            // TODO: Change this after getting rid of FatClientWrapper
            DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient = this.fatClientMap.get(requestValidator.getStoreName());
            if(storeClient != null) {
                CoordinatorStoreClientRequest coordinatorRequest = new CoordinatorStoreClientRequest(requestObject,
                                                                                                     storeClient);
                Channels.fireMessageReceived(ctx, coordinatorRequest);
            } else {
                logger.error("Error when getting store. Non Existing store client.");
                RestServerErrorHandler.writeErrorResponse(messageEvent,
                                                          HttpResponseStatus.BAD_REQUEST,
                                                          "Non Existing store client. Critical error.");
                return;

            }
        }
    }
}

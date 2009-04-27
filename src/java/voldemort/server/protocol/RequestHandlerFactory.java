package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.store.ErrorCodeMapper;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.RequestFormatType}.
 * 
 * @author jay
 * 
 */
public class RequestHandlerFactory {

    private final StoreRepository repository;

    public RequestHandlerFactory(StoreRepository repository) {
        this.repository = repository;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), repository);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}

package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.store.ErrorCodeMapper;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.protocol.RequestFormatType}.
 * 
 * 
 */
public class ClientRequestHandlerFactory implements RequestHandlerFactory {

    private final StoreRepository repository;

    public ClientRequestHandlerFactory(StoreRepository repository) {
        this.repository = repository;
    }

    @Override
    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT_V0:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 0);
            case VOLDEMORT_V1:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 1);
            case VOLDEMORT_V2:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 2);
            case VOLDEMORT_V3:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 3);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), repository);
            default:
                throw new VoldemortException("Unknown wire format in " + this.getClass().getName()
                                             + " Type : " + type);
        }
    }

    @Override
    public boolean shareReadWriteBuffer() {
        return true;
    }
}

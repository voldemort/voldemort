package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortMetadata;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.server.socket.AdminServiceRequestHandler;
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
    private final VoldemortMetadata metadata;
    private final VoldemortConfig voldemortConfig;

    public RequestHandlerFactory(StoreRepository repository,
                                 VoldemortMetadata metadata,
                                 VoldemortConfig voldemortConfig) {
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), repository);
            case ADMIN_HANDLER:
                return new AdminServiceRequestHandler(new ErrorCodeMapper(),
                                                      repository,
                                                      metadata,
                                                      voldemortConfig.getMetadataDirectory(),
                                                      voldemortConfig.getStreamMaxReadBytesPerSec(),
                                                      voldemortConfig.getStreamMaxWriteBytesPerSec());
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }
}

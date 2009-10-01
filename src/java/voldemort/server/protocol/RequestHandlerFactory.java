package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.NativeAdminServiceRequestHandler;
import voldemort.server.protocol.pb.ProtoBuffAdminServiceRequestHandler;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.RequestFormatType}.
 * 
 * @author jay
 * 
 */
public class RequestHandlerFactory {

    private final StoreRepository repository;
    private final MetadataStore metadata;
    private final VoldemortConfig voldemortConfig;

    public RequestHandlerFactory(StoreRepository repository,
                                 MetadataStore metadata,
                                 VoldemortConfig voldemortConfig) {
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT_V0:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 0);
            case VOLDEMORT_V1:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 1);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), repository);
            case ADMIN:
                return new NativeAdminServiceRequestHandler(new ErrorCodeMapper(),
                                                      repository,
                                                      metadata,
                                                      voldemortConfig.getStreamMaxReadBytesPerSec(),
                                                      voldemortConfig.getStreamMaxWriteBytesPerSec());
            case ADMIN_PROTOCOL_BUFFERS:
                return new ProtoBuffAdminServiceRequestHandler(new ErrorCodeMapper(),
                        repository,
                        metadata);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }
}

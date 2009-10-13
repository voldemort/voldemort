package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.NativeAdminServiceRequestHandler;
import voldemort.server.protocol.admin.ProtoBuffAdminServiceRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;

/**
 * A factory that gets the appropriate request handler for Admin request Types
 * {@link voldemort.client.RequestFormatType}.
 * 
 * Need to separate it out from {@link RequestHandlerFactory} as any attempt to
 * use AdminClient with normal socket should throw an error.
 * 
 * @author bbansal
 * 
 */
public class AdminRequestHandlerFactory implements RequestHandlerFactory {

    private final StoreRepository repository;
    private final MetadataStore metadata;
    private final VoldemortConfig voldemortConfig;

    public AdminRequestHandlerFactory(StoreRepository repository,
                                      MetadataStore metadata,
                                      VoldemortConfig voldemortConfig) {
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case ADMIN:
                return new NativeAdminServiceRequestHandler(new ErrorCodeMapper(),
                                                            repository,
                                                            metadata,
                                                            voldemortConfig.getStreamMaxReadBytesPerSec(),
                                                            voldemortConfig.getStreamMaxWriteBytesPerSec());
            case ADMIN_PROTOCOL_BUFFERS:
                return new ProtoBuffAdminServiceRequestHandler(new ErrorCodeMapper(),
                                                               repository,
                                                               metadata,
                                                               voldemortConfig.getStreamMaxReadBytesPerSec(),
                                                               voldemortConfig.getStreamMaxWriteBytesPerSec());
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }
}

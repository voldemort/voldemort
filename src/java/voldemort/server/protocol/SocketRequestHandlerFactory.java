package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.storage.StorageService;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.protocol.RequestFormatType}.
 * 
 * 
 */
public class SocketRequestHandlerFactory implements RequestHandlerFactory {

    private final StorageService storage;
    private final StoreRepository repository;
    private final MetadataStore metadata;
    private final VoldemortConfig voldemortConfig;
    private final AsyncOperationRunner asyncRunner;
    private final Rebalancer rebalancer;

    public SocketRequestHandlerFactory(StorageService storageService,
                                       StoreRepository repository,
                                       MetadataStore metadata,
                                       VoldemortConfig voldemortConfig,
                                       AsyncOperationRunner asyncRunner,
                                       Rebalancer rebalancer) {
        this.storage = storageService;
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
        this.asyncRunner = asyncRunner;
        this.rebalancer = rebalancer;
    }

    public RequestHandler getRequestHandler(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT_V0:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 0);
            case VOLDEMORT_V1:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 1);
            case VOLDEMORT_V2:
                return new VoldemortNativeRequestHandler(new ErrorCodeMapper(), repository, 2);
            case PROTOCOL_BUFFERS:
                return new ProtoBuffRequestHandler(new ErrorCodeMapper(), repository);
            case ADMIN_PROTOCOL_BUFFERS:
                return new AdminServiceRequestHandler(new ErrorCodeMapper(),
                                                               storage,
                                                               repository,
                                                               metadata,
                                                               voldemortConfig,
                                                               asyncRunner,
                                                               rebalancer);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }
}

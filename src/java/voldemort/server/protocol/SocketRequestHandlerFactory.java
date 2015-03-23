package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationService;
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

    private final ClientRequestHandlerFactory clientFactory;

    private final StorageService storage;
    private final StoreRepository repository;
    private final MetadataStore metadata;
    private final VoldemortConfig voldemortConfig;
    private final AsyncOperationService asyncService;
    private final Rebalancer rebalancer;
    private final VoldemortServer server;

    public SocketRequestHandlerFactory(StorageService storageService,
                                       StoreRepository repository,
                                       MetadataStore metadata,
                                       VoldemortConfig voldemortConfig,
                                       AsyncOperationService asyncService,
                                       Rebalancer rebalancer,
                                       VoldemortServer server) {

        this.clientFactory = new ClientRequestHandlerFactory(repository);

        this.storage = storageService;
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
        this.asyncService = asyncService;
        this.rebalancer = rebalancer;
        this.server = server;
    }

    @Override
    public RequestHandler getRequestHandler(RequestFormatType type) {
        if(type == RequestFormatType.ADMIN_PROTOCOL_BUFFERS) {
                return new AdminServiceRequestHandler(new ErrorCodeMapper(),
                                                  storage,
                                                  repository,
                                                  metadata,
                                                  voldemortConfig,
                                                  asyncService,
                                                  rebalancer,
                                                  server);
        }

        try {
            return clientFactory.getRequestHandler(type);
        } catch(VoldemortException e) {

        }

        throw new VoldemortException("Unknown wire format in " + this.getClass().getName()
                                     + " Type : " + type);
    }
}

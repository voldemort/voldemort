package voldemort.server.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AdminServiceRequestHandler;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.server.protocol.pb.ProtoBuffRequestHandler;
import voldemort.server.protocol.vold.VoldemortNativeRequestHandler;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.storage.StorageService;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamStats;
import voldemort.store.stats.StreamStatsJmx;
import voldemort.utils.JmxUtils;

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
    private final AsyncOperationService asyncService;
    private final Rebalancer rebalancer;
    private final StreamStats stats;

    public SocketRequestHandlerFactory(StorageService storageService,
                                       StoreRepository repository,
                                       MetadataStore metadata,
                                       VoldemortConfig voldemortConfig,
                                       AsyncOperationService asyncService,
                                       Rebalancer rebalancer) {
        this.storage = storageService;
        this.repository = repository;
        this.metadata = metadata;
        this.voldemortConfig = voldemortConfig;
        this.asyncService = asyncService;
        this.rebalancer = rebalancer;
        this.stats = new StreamStats();
        if(null != voldemortConfig && voldemortConfig.isJmxEnabled())
            if(this.voldemortConfig.isEnableJmxClusterName())
                JmxUtils.registerMbean(new StreamStatsJmx(stats),
                                       JmxUtils.createObjectName(metadata.getCluster().getName()
                                                                         + ".voldemort.store.stats",
                                                                 "admin-streaming"));
            else
                JmxUtils.registerMbean(new StreamStatsJmx(stats),
                                       JmxUtils.createObjectName("voldemort.store.stats",
                                                                 "admin-streaming"));
    }

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
            case ADMIN_PROTOCOL_BUFFERS:
                return new AdminServiceRequestHandler(new ErrorCodeMapper(),
                                                      storage,
                                                      repository,
                                                      metadata,
                                                      voldemortConfig,
                                                      asyncService,
                                                      rebalancer,
                                                      stats);
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }
}

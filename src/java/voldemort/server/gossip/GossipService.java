package voldemort.server.gossip;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.store.metadata.MetadataStore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author afeinberg
 */
public class GossipService extends AbstractService {
    private final ExecutorService executor;
    private final Gossiper gossiper;

    private final static int GOSSIP_INTERVAL = 30 * 1000;

    public GossipService(MetadataStore metadataStore, VoldemortConfig voldemortConfig) {
        super(ServiceType.GOSSIP);
        executor = Executors.newSingleThreadExecutor();

        ClientConfig clientConfig = new ClientConfig()
                .setMaxConnectionsPerNode(3)
                .setMaxThreads(3)
                .setConnectionTimeout(voldemortConfig.getAdminConnectionTimeout(), TimeUnit.MILLISECONDS)
                .setSocketTimeout(voldemortConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .setSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());
        AdminClient adminClient = new ProtoBuffAdminClientRequestFormat(metadataStore.getCluster(),
                clientConfig);
        gossiper = new Gossiper(metadataStore, adminClient, GOSSIP_INTERVAL);
    }
    
    @Override
    protected void startInner() {
        gossiper.start();
        executor.submit(gossiper);
    }

    @Override
    protected void stopInner() {
        gossiper.stop();
    }
}

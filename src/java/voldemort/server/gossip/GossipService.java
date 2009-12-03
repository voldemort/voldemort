package voldemort.server.gossip;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.metadata.MetadataStore;

/**
 * @author afeinberg
 */
@JmxManaged(description = "Epidemic (gossip) protocol for propagating state/configuration to the cluster.")
public class GossipService extends AbstractService {
    private final SchedulerService schedulerService;
    private final Gossiper gossiper;

    public GossipService(MetadataStore metadataStore, AdminClient adminClient, SchedulerService service, VoldemortConfig voldemortConfig) {
        super(ServiceType.GOSSIP);
        schedulerService = service;
        gossiper = new Gossiper(metadataStore, adminClient, voldemortConfig.getGossipInterval());
    }
    
    @Override
    protected void startInner() {
        gossiper.start();
        schedulerService.scheduleNow(gossiper);
    }

    @Override
    protected void stopInner() {
        gossiper.stop();
    }
}

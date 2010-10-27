package voldemort.server.rebalance;

import java.util.Date;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.metadata.MetadataStore;

/**
 */
@JmxManaged(description = "Rebalancer service to check if server is in rebalancing state and attempt rebalancing periodically.")
public class RebalancerService extends AbstractService {

    private final int periodMs;
    private final SchedulerService schedulerService;
    private final Rebalancer rebalancer;

    public RebalancerService(MetadataStore metadataStore,
                             VoldemortConfig voldemortConfig,
                             AsyncOperationService asyncService,
                             SchedulerService service) {
        super(ServiceType.REBALANCE);
        schedulerService = service;
        rebalancer = new Rebalancer(metadataStore, voldemortConfig, asyncService);
        periodMs = voldemortConfig.getRebalancingServicePeriod();
    }

    @Override
    protected void startInner() {
        rebalancer.start();
        schedulerService.schedule("rebalancer", rebalancer, new Date(), periodMs);
    }

    @Override
    protected void stopInner() {
        rebalancer.stop();
    }

    public Rebalancer getRebalancer() {
        return rebalancer;
    }
}

package voldemort.server.rebalance;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;

/**
 */
@JmxManaged(description = "Rebalancer service to help with rebalancing")
public class RebalancerService extends AbstractService {

    private final SchedulerService schedulerService;
    private final Rebalancer rebalancer;

    public RebalancerService(StoreRepository storeRepository,
                             MetadataStore metadataStore,
                             VoldemortConfig voldemortConfig,
                             AsyncOperationService asyncService,
                             SchedulerService service) {
        super(ServiceType.REBALANCE);
        schedulerService = service;
        rebalancer = new Rebalancer(storeRepository, metadataStore, voldemortConfig, asyncService);
    }

    @Override
    protected void startInner() {
        rebalancer.start();
        schedulerService.scheduleNow(rebalancer);
    }

    @Override
    protected void stopInner() {
        rebalancer.stop();
    }

    public Rebalancer getRebalancer() {
        return rebalancer;
    }
}

package voldemort.server.rebalance;

import java.util.Calendar;
import java.util.GregorianCalendar;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.Time;

/**
 */
@JmxManaged(description = "Rebalancer service to check if server is in rebalancing state only during startup")
public class RebalancerService extends AbstractService {

    private final long periodMs;
    private final SchedulerService schedulerService;
    private final Rebalancer rebalancer;

    public RebalancerService(MetadataStore metadataStore,
                             VoldemortConfig voldemortConfig,
                             AsyncOperationService asyncService,
                             SchedulerService service) {
        super(ServiceType.REBALANCE);
        schedulerService = service;
        rebalancer = new Rebalancer(metadataStore, voldemortConfig, asyncService);
        periodMs = voldemortConfig.getRebalancingStartMs();
    }

    @Override
    protected void startInner() {
        rebalancer.start();
        GregorianCalendar cal = new GregorianCalendar();
        cal.add(Calendar.SECOND, (int) (periodMs / Time.MS_PER_SECOND));
        schedulerService.schedule("rebalancer", rebalancer, cal.getTime());
    }

    @Override
    protected void stopInner() {
        rebalancer.stop();
    }

    public Rebalancer getRebalancer() {
        return rebalancer;
    }
}

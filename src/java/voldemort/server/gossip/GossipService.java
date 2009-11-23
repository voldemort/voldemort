package voldemort.server.gossip;

import voldemort.server.AbstractService;
import voldemort.server.ServiceType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author afeinberg
 */
public class GossipService extends AbstractService {
    private final ExecutorService executor;
    private final Gossiper gossiper;

    public GossipService() {
        super(ServiceType.GOSSIP);
        executor = Executors.newSingleThreadExecutor();
        gossiper = new Gossiper();
    }
    
    @Override
    protected void startInner() {
        executor.submit(gossiper);
    }

    @Override
    protected void stopInner() {
        gossiper.stop();
    }
}

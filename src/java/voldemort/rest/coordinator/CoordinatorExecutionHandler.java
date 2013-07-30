package voldemort.rest.coordinator;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.execution.ExecutionHandler;

import voldemort.store.stats.StoreStats;

public class CoordinatorExecutionHandler extends ExecutionHandler {

    private final CoordinatorMetadata coordinatorMetadata;
    private final StoreStats coordinatorPerfStats;

    public CoordinatorExecutionHandler(Executor executor,
                                       CoordinatorMetadata coordinatorMetadata,
                                       StoreStats coordinatorPerfStats) {
        super(executor);
        this.coordinatorMetadata = coordinatorMetadata;
        this.coordinatorPerfStats = coordinatorPerfStats;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext context, ChannelEvent channelEvent)
            throws Exception {
        if(channelEvent instanceof MessageEvent) {
            getExecutor().execute(new CoordinatorWorkerThread((MessageEvent) channelEvent,
                                                              this.coordinatorMetadata,
                                                              this.coordinatorPerfStats));
        }
    }

}

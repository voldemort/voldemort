package voldemort.coordinator;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.execution.ExecutionHandler;

public class CoordinatorExecutionHandler extends ExecutionHandler {

    public CoordinatorExecutionHandler(Executor executor) {
        super(executor);
    }

    @Override
    public void handleUpstream(ChannelHandlerContext context, ChannelEvent channelEvent)
            throws Exception {
        if(channelEvent instanceof MessageEvent) {
            getExecutor().execute(new CoordinatorWorkerThread((MessageEvent) channelEvent));
        }
    }

}

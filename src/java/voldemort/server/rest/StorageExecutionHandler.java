package voldemort.server.rest;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.execution.ExecutionHandler;

public class StorageExecutionHandler extends ExecutionHandler {

    public StorageExecutionHandler(Executor executor) {
        super(executor);
    }

    @Override
    public void handleUpstream(ChannelHandlerContext context, ChannelEvent channelEvent)
            throws Exception {
        if(channelEvent instanceof MessageEvent) {
            getExecutor().execute(new StorageWorkerThread((MessageEvent) channelEvent));
        }
    }

}

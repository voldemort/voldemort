package voldemort.rest.server;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.execution.ExecutionHandler;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.stats.StoreStats;

public class StorageExecutionHandler extends ExecutionHandler {

    private final ThreadPoolExecutor threadPoolExecutor;
    private final StoreStats performanceStats;
    private final int localZoneId;

    public StorageExecutionHandler(Executor executor, StoreStats performanceStats, int localZoneId) {
        super(executor);
        if(executor instanceof ThreadPoolExecutor) {
            threadPoolExecutor = (ThreadPoolExecutor) executor;
        } else {
            threadPoolExecutor = null;
        }
        this.performanceStats = performanceStats;
        this.localZoneId = localZoneId;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext context, ChannelEvent channelEvent)
            throws Exception {
        if(channelEvent instanceof MessageEvent) {
            getExecutor().execute(new StorageWorkerThread((MessageEvent) channelEvent,
                                                          performanceStats,
                                                          localZoneId));
        }
    }

    @JmxGetter(name = "StorageThreadPoolQueueSize", description = "The number of storage requests queued to be executed")
    public int getQueuSize() {
        if(this.threadPoolExecutor != null) {
            return threadPoolExecutor.getQueue().size();
        } else {
            return -1;
        }
    }

    @JmxGetter(name = "ActiveStorageThreads", description = "The number of active Storage worker threads.")
    public int getActiveThreadsInWorkerPool() {
        if(this.threadPoolExecutor != null) {
            return this.threadPoolExecutor.getActiveCount();
        } else {
            return -1;
        }
    }

    @JmxGetter(name = "TotalStorageWorkerThreads", description = "The total number of Storage worker threads, active and idle.")
    public int getAllThreadInWorkerPool() {
        if(this.threadPoolExecutor != null) {
            return this.threadPoolExecutor.getPoolSize();
        } else {
            return -1;
        }
    }

}

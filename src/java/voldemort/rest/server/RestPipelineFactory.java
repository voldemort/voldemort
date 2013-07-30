package voldemort.rest.server;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import voldemort.rest.NettyConnectionStats;
import voldemort.rest.NettyConnectionStatsHandler;
import voldemort.server.StoreRepository;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.JmxUtils;

public class RestPipelineFactory implements ChannelPipelineFactory {

    private StoreRepository storeRepository;

    /**
     * TODO REST-Server 1. Using a Bounded blocking queue with configurable
     * capacity
     * 
     */
    ThreadFactory threadFactory = new DaemonThreadFactory("Voldemort-REST-Server-Storage-Thread");
    private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;
    private final NettyConnectionStats connectionStats;
    private final NettyConnectionStatsHandler connectionStatsHandler;
    private final StoreStats performanceStats;

    public RestPipelineFactory(StoreRepository storeRepository,
                               int numStorageThreads,
                               int threadPoolQueueSize,
                               boolean jmxEnabled,
                               int localZoneId) {
        this.storeRepository = storeRepository;
        performanceStats = new StoreStats();
        this.threadPoolExecutor = new ThreadPoolExecutor(numStorageThreads,
                                                         numStorageThreads,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(threadPoolQueueSize),
                                                         threadFactory);
        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor,
                                                              performanceStats,
                                                              localZoneId);
        connectionStats = new NettyConnectionStats();
        connectionStatsHandler = new NettyConnectionStatsHandler(connectionStats);

        if(jmxEnabled) {
            // Register MBeans for Storage pool stats
            JmxUtils.registerMbean(this.storageExecutionHandler,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.storageExecutionHandler.getClass()),
                                                             JmxUtils.getClassName(this.storageExecutionHandler.getClass())));
            // Register MBeans for connection stats
            JmxUtils.registerMbean(this.connectionStats,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.connectionStats.getClass()),
                                                             JmxUtils.getClassName(this.connectionStats.getClass())));

            // Register the Rest server performance stats
            JmxUtils.registerMbean(new StoreStatsJmx(this.performanceStats),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.performanceStats.getClass()),
                                                             JmxUtils.getClassName(this.performanceStats.getClass())));
        }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("connectionStats", connectionStatsHandler);
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new RestServerRequestHandler(storeRepository));
        pipeline.addLast("storageExecutionHandler", storageExecutionHandler);
        return pipeline;
    }

}

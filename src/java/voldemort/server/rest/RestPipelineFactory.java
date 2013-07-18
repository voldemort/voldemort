package voldemort.server.rest;

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

import voldemort.server.StoreRepository;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.JmxUtils;

public class RestPipelineFactory implements ChannelPipelineFactory {

    private StoreRepository storeRepository;

    /**
     * TODO REST-Server 1. Using a normal thread pool 2. using core pool size =
     * max pool size = 40 arbitrarily 3. Bounded blocking queue with capacity
     * 40. queue capacity is also arbitrary
     */
    ThreadFactory threadFactory = new DaemonThreadFactory("Voldemort-REST-Server-Storage-Thread");
    private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;
    private final ConnectionStats connectionStats;
    private final ConnectionStatsHandler connectionStatsHandler;

    public RestPipelineFactory(StoreRepository storeRepository,
                               int numStorageThreads,
                               int threadPoolQueueSize) {
        this.storeRepository = storeRepository;
        this.threadPoolExecutor = new ThreadPoolExecutor(numStorageThreads,
                                                         numStorageThreads,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(threadPoolQueueSize),
                                                         threadFactory);
        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor);
        connectionStats = new ConnectionStats();
        connectionStatsHandler = new ConnectionStatsHandler(connectionStats);

        // Register MBeans for Storage pool stats
        JmxUtils.registerMbean(this.storageExecutionHandler,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.storageExecutionHandler.getClass()),
                                                         JmxUtils.getClassName(this.storageExecutionHandler.getClass())));
        // Register MBeans for connection stats
        JmxUtils.registerMbean(this.connectionStats,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.connectionStats.getClass()),
                                                         JmxUtils.getClassName(this.connectionStats.getClass())));
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
        pipeline.addLast("handler", new VoldemortRestRequestHandler(storeRepository));
        pipeline.addLast("storageExecutionHandler", storageExecutionHandler);
        return pipeline;
    }

}

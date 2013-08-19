package voldemort.rest.server;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import voldemort.rest.NettyConnectionStats;
import voldemort.rest.NettyConnectionStatsHandler;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.JmxUtils;
import azkaban.common.utils.Utils;

public class RestPipelineFactory implements ChannelPipelineFactory {

    private StoreRepository storeRepository;
    ThreadFactory threadFactory = new DaemonThreadFactory("Voldemort-REST-Server-Storage-Thread");
    private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;
    private final NettyConnectionStats connectionStats;
    private final NettyConnectionStatsHandler connectionStatsHandler;
    private final StoreStats aggregatedStoreStats;
    private final int maxHttpContentLength;
    private ConcurrentHashMap<String, StoreStats> storeStatsMap;

    public RestPipelineFactory(StoreRepository storeRepository,
                               VoldemortConfig config,
                               int localZoneId,
                               List<StoreDefinition> storeDefinitions,
                               ChannelGroup allchannels) {
        this.storeRepository = storeRepository;
        this.threadPoolExecutor = new ThreadPoolExecutor(config.getNumRestServiceStorageThreads(),
                                                         config.getNumRestServiceStorageThreads(),
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(config.getRestServiceStorageThreadPoolQueueSize()),
                                                         threadFactory);
        this.aggregatedStoreStats = new StoreStats();
        createAndRegisterMBeansForAllStoreStats(config, storeDefinitions);

        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor,
                                                              storeStatsMap,
                                                              aggregatedStoreStats,
                                                              config.isJmxEnabled(),
                                                              localZoneId);
        connectionStats = new NettyConnectionStats();
        connectionStatsHandler = new NettyConnectionStatsHandler(connectionStats, allchannels);

        maxHttpContentLength = config.getMaxHttpAggregatedContentLength();
        if(config.isJmxEnabled()) {
            // Register MBeans for Storage pool stats
            JmxUtils.registerMbean(this.storageExecutionHandler,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.storageExecutionHandler.getClass()),
                                                             JmxUtils.getClassName(this.storageExecutionHandler.getClass())));
            // Register MBeans for connection stats
            JmxUtils.registerMbean(this.connectionStats,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.connectionStats.getClass()),
                                                             JmxUtils.getClassName(this.connectionStats.getClass())));
        }
    }

    public void createAndRegisterMBeansForAllStoreStats(VoldemortConfig config,
                                                        List<StoreDefinition> storeDefinitions) {
        storeStatsMap = new ConcurrentHashMap<String, StoreStats>();
        boolean isJmxEnabled = config.isJmxEnabled();
        for(StoreDefinition storeDefinition: Utils.nonNull(storeDefinitions)) {
            String storeName = storeDefinition.getName();

            // Add to concurrentHashMap
            storeStatsMap.put(storeName, new StoreStats(aggregatedStoreStats));

            // Register storeStats MBeans for every store
            if(isJmxEnabled) {
                JmxUtils.registerMbean(new StoreStatsJmx(storeStatsMap.get(storeName)),
                                       JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass())
                                                                         + ".store.stats",
                                                                 storeName));
            }
        }

        // Register Mbean for aggregated server store stats
        if(isJmxEnabled) {
            JmxUtils.registerMbean(new StoreStatsJmx(aggregatedStoreStats),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass())
                                                                     + ".store.stats",
                                                             JmxUtils.getClassName(this.aggregatedStoreStats.getClass())));
        }
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("connectionStats", connectionStatsHandler);
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(maxHttpContentLength));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new RestServerRequestHandler(storeRepository));
        pipeline.addLast("storageExecutionHandler", storageExecutionHandler);
        return pipeline;
    }

}

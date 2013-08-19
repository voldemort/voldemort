package voldemort.rest.server;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.JmxUtils;

@JmxManaged(description = "Rest Service to serve Http requests")
public class RestService extends AbstractService {

    private final Logger logger = Logger.getLogger(RestService.class);
    private final int port;
    protected ThreadPoolExecutor workerPool;
    private ServerBootstrap bootstrap = null;
    private Channel nettyServerChannel;
    static final ChannelGroup allChannels = new DefaultChannelGroup();
    private final StoreRepository storeRepository;
    private final List<StoreDefinition> storeDefinitions;
    private final VoldemortConfig config;
    private int localZoneId;

    public RestService(VoldemortConfig config,
                       int restPort,
                       StoreRepository storeRepository,
                       int zoneId,
                       List<StoreDefinition> storeDefinitions) {
        super(ServiceType.RESTSERVICE);
        this.config = config;
        this.port = restPort;
        this.storeRepository = storeRepository;
        this.localZoneId = zoneId;
        this.storeDefinitions = storeDefinitions;
    }

    @Override
    protected void startInner() {

        // Configure the server.
        this.workerPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getNumRestServiceNettyWorkerThreads());
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(config.getNumRestServiceNettyBossThreads()),
                                                                               workerPool));
        this.bootstrap.setOption("backlog", config.getRestServiceNettyServerBacklog());
        this.bootstrap.setOption("child.tcpNoDelay", true);
        this.bootstrap.setOption("child.keepAlive", true);
        this.bootstrap.setOption("child.reuseAddress", true);
        this.bootstrap.setPipelineFactory(new RestPipelineFactory(storeRepository,
                                                                  config,
                                                                  localZoneId,
                                                                  storeDefinitions,
                                                                  allChannels));

        // Bind and start to accept incoming connections.
        this.nettyServerChannel = this.bootstrap.bind(new InetSocketAddress(this.port));
        allChannels.add(nettyServerChannel);
        logger.info("REST service started on port " + this.port);

        // Register MBeans for Netty worker pool stats
        if(config.isJmxEnabled()) {
            JmxUtils.registerMbean(this,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                             JmxUtils.getClassName(this.getClass())));
        }
    }

    /**
     * Closes the Netty Channel and releases all resources
     */
    @Override
    protected void stopInner() {
        /*
         * TODO REST-Server Need to handle infligh operations. What happens to
         * the existing async operations when a channel.close() is issued in
         * Netty?
         */
        if(this.nettyServerChannel != null) {
            this.nettyServerChannel.close();
        }

        if(allChannels != null) {
            allChannels.close().awaitUninterruptibly();
        }
        this.bootstrap.releaseExternalResources();
    }

    @JmxGetter(name = "ActiveNettyWorkerThreads", description = "The number of active Netty worker threads.")
    public int getActiveThreadsInWorkerPool() {
        return this.workerPool.getActiveCount();
    }

    @JmxGetter(name = "TotalNettyWorkerThreads", description = "The total number of Netty worker threads, active and idle.")
    public int getAllThreadInWorkerPool() {
        return this.workerPool.getPoolSize();
    }

    @JmxGetter(name = "QueuedNetworkRequests", description = "Number of requests in the Netty worker queue waiting to execute.")
    public int getQueuedRequests() {
        return this.workerPool.getQueue().size();
    }

}

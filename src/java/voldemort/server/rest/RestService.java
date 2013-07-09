package voldemort.server.rest;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;

public class RestService extends AbstractService {

    // TODO REST-Server
    // 1. use a different http port for REST service. Do not use the existing
    // http service port
    // 2. Bring up both Rest service and http service from Voldemort server

    private final Logger logger = Logger.getLogger(RestService.class);
    private final int port;
    protected ThreadPoolExecutor workerPool;
    private ServerBootstrap bootstrap = null;
    private Channel nettyServerChannel;
    private final StoreRepository storeRepository;
    private final VoldemortConfig config;

    public RestService(VoldemortConfig config, int httpPort, StoreRepository storeRepository) {
        super(ServiceType.RESTSERVICE);
        this.config = config;
        this.port = httpPort;
        this.storeRepository = storeRepository;
    }

    @Override
    protected void startInner() {

        // Configure the server.
        this.workerPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getNumRestServiceNettyWorkerThreads());
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(config.getNumRestServiceNettyBossThreads()),
                                                                               workerPool));
        /*
         * TODO REST-Server Need to add a server parameter for netty backlog
         */
        // this.bootstrap.setOption("backlog",
        // this.coordinatorConfig.getNettyServerBacklog());
        this.bootstrap.setOption("child.tcpNoDelay", true);
        this.bootstrap.setOption("child.keepAlive", true);
        this.bootstrap.setOption("child.reuseAddress", true);
        this.bootstrap.setPipelineFactory(new RestPipelineFactory(storeRepository,
                                                                  config.getNumRestServiceStorageThreads(),
                                                                  config.getRestServiceStorageThreadPoolQueueSize()));

        // Bind and start to accept incoming connections.
        this.nettyServerChannel = this.bootstrap.bind(new InetSocketAddress(this.port));
        logger.info("REST service started on port " + this.port);
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
        this.bootstrap.releaseExternalResources();
    }

}

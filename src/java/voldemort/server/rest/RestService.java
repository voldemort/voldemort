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

public class RestService extends AbstractService {

    // TODO REST-Server
    // 1. use a different http port for REST service. Do not use the existing
    // http service port
    // 2. Temporarily use in memory storage. LAter design for Bdb storage
    // 3. Bring up both Rest service and http service from Voldemort server

    private final Logger logger = Logger.getLogger(RestService.class);
    private final int port;
    private final int numOfThreads;
    protected ThreadPoolExecutor workerPool;
    private ServerBootstrap bootstrap = null;
    private Channel nettyServerChannel;
    private final StoreRepository storeRepository;

    public RestService(int numberOfThreads, int httpPort, StoreRepository storeRepository) {
        super(ServiceType.RESTSERVICE);
        this.port = httpPort;
        this.numOfThreads = numberOfThreads;
        this.storeRepository = storeRepository;
    }

    @Override
    protected void startInner() {

        // Configure the server.
        this.workerPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfThreads);
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                               workerPool));
        /*
         * TODO REST-Server Need to add a server parameter for netty backlog
         */
        // this.bootstrap.setOption("backlog",
        // this.coordinatorConfig.getNettyServerBacklog());
        this.bootstrap.setOption("child.tcpNoDelay", true);
        this.bootstrap.setOption("child.keepAlive", true);
        this.bootstrap.setOption("child.reuseAddress", true);
        this.bootstrap.setPipelineFactory(new RestPipelineFactory(storeRepository));

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

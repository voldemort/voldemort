package voldemort.rest;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;
import voldemort.rest.coordinator.CoordinatorConfig;
import voldemort.rest.coordinator.CoordinatorProxyService;
import voldemort.utils.JmxUtils;

public abstract class AbstractRestService extends AbstractService {

    private CoordinatorConfig coordinatorConfig = null;
    protected ThreadPoolExecutor workerPool = null;
    protected final NettyConnectionStats connectionStats;
    private ServerBootstrap bootstrap = null;
    private Channel channel = null;

    private static final Logger logger = Logger.getLogger(AbstractRestService.class);

    public AbstractRestService(ServiceType type, CoordinatorConfig config) {
        super(type);
        this.coordinatorConfig = config;
        this.connectionStats = new NettyConnectionStats();
    }

    abstract protected void initialize();

    abstract protected Logger getLogger();

    abstract protected ChannelPipelineFactory getPipelineFactory();

    abstract protected int getServicePort();

    abstract protected String getServiceName();

    @Override
    protected void startInner() {
        initialize();
        // Configure the service
        this.workerPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), workerPool));
        this.bootstrap.setOption("backlog", this.coordinatorConfig.getNettyServerBacklog());
        this.bootstrap.setOption("child.tcpNoDelay", true);
        this.bootstrap.setOption("child.keepAlive", true);
        this.bootstrap.setOption("child.reuseAddress", true);

        // Set up the event pipeline factory.
        this.bootstrap.setPipelineFactory(getPipelineFactory());

        // Assuming JMX is always enabled for service
        JmxUtils.registerMbean(this, JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                               JmxUtils.getClassName(this.getClass())));

        // Register MBeans for connection stats
        JmxUtils.registerMbean(this.connectionStats,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.connectionStats.getClass())));

        // Bind and start to accept incoming connections.
        this.channel = this.bootstrap.bind(new InetSocketAddress(getServicePort()));
        logger.info(getServiceName() + " service started on port " + getServicePort());
    }

    @Override
    protected void stopInner() {
        if(this.channel != null) {
            this.channel.close();
        }
        JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                           JmxUtils.getClassName(this.getClass())));
    }

}

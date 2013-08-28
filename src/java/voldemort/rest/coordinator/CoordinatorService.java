/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.rest.coordinator;

import static voldemort.utils.Utils.croak;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.BootstrapFailureException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.rest.NettyConnectionStats;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

/**
 * A Netty based HTTP service that accepts REST requests from the Voldemort thin
 * clients and invokes the corresponding Fat client API.
 * 
 */
@JmxManaged(description = "A Coordinator Service for proxying Voldemort HTTP requests")
public class CoordinatorService extends AbstractService {

    private CoordinatorConfig coordinatorConfig = null;

    private SocketStoreClientFactory storeClientFactory = null;
    private AsyncMetadataVersionManager asyncMetadataManager = null;
    private final CoordinatorMetadata coordinatorMetadata;
    private SchedulerService schedulerService = null;
    private static final Logger logger = Logger.getLogger(CoordinatorService.class);
    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap = null;
    public final static Schema CLIENT_CONFIGS_AVRO_SCHEMA = Schema.parse("{ \"name\": \"clientConfigs\",  \"type\":\"array\","
                                                                         + "\"items\": { \"name\": \"clientConfig\", \"type\": \"map\", \"values\":\"string\" }}}");
    private static final String STORE_NAME_KEY = "store_name";
    protected ThreadPoolExecutor workerPool = null;
    private final StoreStats coordinatorPerfStats;
    private final NettyConnectionStats connectionStats;
    private ServerBootstrap bootstrap = null;
    private Channel nettyServerChannel = null;

    public CoordinatorService(CoordinatorConfig config) {
        super(ServiceType.COORDINATOR);
        this.coordinatorConfig = config;
        this.coordinatorPerfStats = new StoreStats();
        this.coordinatorMetadata = new CoordinatorMetadata();
        this.connectionStats = new NettyConnectionStats();
    }

    /**
     * Initializes all the Fat clients (1 per store) for the cluster that this
     * Coordinator talks to. This is invoked once during startup and then every
     * time the Metadata manager detects changes to the cluster and stores
     * metadata.
     */
    private void initializeFatClients() {
        StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

        // Fetch the state once and use this to initialize all the Fat clients
        String storesXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
        String clusterXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);
        List<StoreDefinition> storeDefList = storeMapper.readStoreList(new StringReader(storesXml),
                                                                       false);

        // Update the Coordinator Metadata
        this.coordinatorMetadata.setMetadata(clusterXml, storeDefList);

        Map<String, SocketStoreClientFactory> fatClientFactoryMap = readClientConfig(this.coordinatorConfig.getFatClientConfigPath(),
                                                                                     this.coordinatorConfig.getBootstrapURLs());

        // Do not recreate map if it already exists.
        // This function might be called by the AsyncMetadataVersionManager if
        // there is a metadata update on the server side
        if(this.fatClientMap == null) {
            this.fatClientMap = new HashMap<String, DynamicTimeoutStoreClient<ByteArray, byte[]>>();
        }

        for(StoreDefinition storeDef: storeDefList) {
            String storeName = storeDef.getName();

            // Initialize only those stores defined in the client configs file
            if(fatClientFactoryMap.get(storeName) != null) {
                DynamicTimeoutStoreClient<ByteArray, byte[]> storeClient = new DynamicTimeoutStoreClient<ByteArray, byte[]>(storeName,
                                                                                                                            fatClientFactoryMap.get(storeName),
                                                                                                                            1,
                                                                                                                            storesXml,
                                                                                                                            clusterXml);

                fatClientMap.put(storeName, storeClient);
            }
        }
    }

    @Override
    protected void startInner() {

        // Initialize the Voldemort Metadata
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(this.coordinatorConfig.getBootstrapURLs());
        storeClientFactory = new SocketStoreClientFactory(clientConfig);
        try {
            initializeFatClients();
            // Setup the Async Metadata checker
            SystemStoreRepository sysRepository = new SystemStoreRepository(clientConfig);
            String clusterXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);

            sysRepository.createSystemStores(clientConfig,
                                             clusterXml,
                                             storeClientFactory.getFailureDetector());

            // Create a callback for re-bootstrapping the client
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    initializeFatClients();
                    return null;
                }

            };

            // For now track changes in cluster.xml only
            // TODO: Modify this to track stores.xml in the future
            asyncMetadataManager = new AsyncMetadataVersionManager(sysRepository,
                                                                   rebootstrapCallback,
                                                                   null);

            schedulerService = new SchedulerService(1, SystemTime.INSTANCE, true);
            schedulerService.schedule(asyncMetadataManager.getClass().getName(),
                                      asyncMetadataManager,
                                      new Date(),
                                      this.coordinatorConfig.getMetadataCheckIntervalInMs());
        } catch(BootstrapFailureException be) {
            /*
             * While testing, the cluster may not be up, but we may still need
             * to verify if the service deploys. Hence, catch a
             * BootstrapFailureException if any, but continue to register the
             * Netty service (and listener).
             * 
             * TODO: Modify the coordinator service to be more lazy. If it
             * cannot initialize the fat clients during initialization, do this
             * when we get an actual request.
             */
        }

        // Configure the server.
        this.workerPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                               workerPool));
        this.bootstrap.setOption("backlog", this.coordinatorConfig.getNettyServerBacklog());
        this.bootstrap.setOption("child.tcpNoDelay", true);
        this.bootstrap.setOption("child.keepAlive", true);
        this.bootstrap.setOption("child.reuseAddress", true);

        // Set up the event pipeline factory.
        this.bootstrap.setPipelineFactory(new CoordinatorPipelineFactory(this.fatClientMap,
                                                                         this.coordinatorMetadata,
                                                                         this.coordinatorConfig,
                                                                         this.coordinatorPerfStats,
                                                                         this.connectionStats));

        // Assuming JMX is always enabled for Coordinator Service
        JmxUtils.registerMbean(this,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.getClass())));

        // Register MBeans for connection stats
        JmxUtils.registerMbean(this.connectionStats,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.connectionStats.getClass())));

        // Bind and start to accept incoming connections.
        this.nettyServerChannel = this.bootstrap.bind(new InetSocketAddress(this.coordinatorConfig.getServerPort()));

        logger.info("Coordinator service started on port " + this.coordinatorConfig.getServerPort());
    }

    /**
     * A function to parse the specified Avro file in order to obtain the config
     * for each fat client managed by this coordinator.
     * 
     * @param configFilePath Path of the Avro file containing fat client configs
     * @param bootstrapURLs The server URLs used during bootstrap
     * @return Map of store name to the corresponding fat client config
     */
    @SuppressWarnings("unchecked")
    private static Map<String, SocketStoreClientFactory> readClientConfig(String configFilePath,
                                                                          String[] bootstrapURLs) {
        String line;
        Map<String, SocketStoreClientFactory> storeFactoryMap = new HashMap<String, SocketStoreClientFactory>();
        try {
            line = Joiner.on(" ")
                         .join(IOUtils.readLines(new FileReader(new File(configFilePath))))
                         .trim();

            JsonDecoder decoder = new JsonDecoder(CLIENT_CONFIGS_AVRO_SCHEMA, line);
            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(CLIENT_CONFIGS_AVRO_SCHEMA);
            GenericData.Array<Map<Utf8, Utf8>> flowMaps = (GenericData.Array<Map<Utf8, Utf8>>) datumReader.read(null,
                                                                                                                decoder);

            // Flows to return back
            if(flowMaps != null && flowMaps.size() > 0) {
                for(Map<Utf8, Utf8> flowMap: flowMaps) {
                    Properties props = new Properties();
                    for(Utf8 key: flowMap.keySet()) {
                        props.put(key.toString(), flowMap.get(key).toString());
                    }

                    String storeName = flowMap.get(new Utf8(STORE_NAME_KEY)).toString();

                    storeName = props.getProperty(STORE_NAME_KEY);
                    if(storeName == null || storeName.length() == 0) {
                        throw new Exception("Illegal Store Name !!!");
                    }

                    ClientConfig fatClientConfig = new ClientConfig(props);
                    fatClientConfig.setBootstrapUrls(bootstrapURLs)
                                   .setEnableCompressionLayer(false)
                                   .setEnableSerializationLayer(false)
                                   .enableDefaultClient(true)
                                   .setEnableLazy(false);

                    logger.info("Creating a Fat client for store: " + storeName);
                    logger.info("Using config: " + fatClientConfig);

                    storeFactoryMap.put(storeName, new SocketStoreClientFactory(fatClientConfig));

                }
            }
        } catch(FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return storeFactoryMap;
    }

    @Override
    protected void stopInner() {
        if(this.nettyServerChannel != null) {
            this.nettyServerChannel.close();
        }

        JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                           JmxUtils.getClassName(this.getClass())));
    }

    public static void main(String[] args) throws Exception {
        CoordinatorConfig config = null;
        try {
            if(args.length != 1) {
                croak("USAGE: java " + CoordinatorService.class.getName()
                      + " <coordinator_config_file>");

                System.exit(-1);
            }

            config = new CoordinatorConfig(new File(args[0]));
        } catch(Exception e) {
            logger.error(e);
            Utils.croak("Error while loading configuration: " + e.getMessage());
        }

        final CoordinatorService coordinator = new CoordinatorService(config);
        if(!coordinator.isStarted()) {
            coordinator.start();
        }

        // add a shutdown hook to stop the coordinator
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                if(coordinator.isStarted())
                    coordinator.stop();
            }
        });
    }

    @JmxGetter(name = "numberOfActiveThreads", description = "The number of active Netty worker threads.")
    public int getNumberOfActiveThreads() {
        return this.workerPool.getActiveCount();
    }

    @JmxGetter(name = "numberOfThreads", description = "The total number of Netty worker threads, active and idle.")
    public int getNumberOfThreads() {
        return this.workerPool.getPoolSize();
    }

    @JmxGetter(name = "queuedRequests", description = "Number of requests in the Netty worker queue waiting to execute.")
    public int getQueuedRequests() {
        return this.workerPool.getQueue().size();
    }

    @JmxGetter(name = "averageGetCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetCompletionTimeInMs() {
        return this.coordinatorPerfStats.getAvgTimeInMs(Tracked.GET);
    }

    @JmxGetter(name = "averagePutCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAveragePutCompletionTimeInMs() {
        return this.coordinatorPerfStats.getAvgTimeInMs(Tracked.PUT);
    }

    @JmxGetter(name = "averageGetAllCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageGetAllCompletionTimeInMs() {
        return this.coordinatorPerfStats.getAvgTimeInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "averageDeleteCompletionTimeInMs", description = "The avg. time in ms for GET calls to complete.")
    public double getAverageDeleteCompletionTimeInMs() {
        return this.coordinatorPerfStats.getAvgTimeInMs(Tracked.DELETE);
    }

    @JmxGetter(name = "q99GetLatencyInMs", description = "")
    public double getQ99GetLatency() {
        return this.coordinatorPerfStats.getQ99LatencyInMs(Tracked.GET);
    }

    @JmxGetter(name = "q99PutLatencyInMs", description = "")
    public double getQ99PutLatency() {
        return this.coordinatorPerfStats.getQ99LatencyInMs(Tracked.PUT);
    }

    @JmxGetter(name = "q99GetAllLatencyInMs", description = "")
    public double getQ99GetAllLatency() {
        return this.coordinatorPerfStats.getQ99LatencyInMs(Tracked.GET_ALL);
    }

    @JmxGetter(name = "q99DeleteLatencyInMs", description = "")
    public double getQ99DeleteLatency() {
        return this.coordinatorPerfStats.getQ99LatencyInMs(Tracked.DELETE);
    }
}

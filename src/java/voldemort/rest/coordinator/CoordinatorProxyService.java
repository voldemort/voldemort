/*
 * Copyright 2014 LinkedIn, Inc
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipelineFactory;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.BootstrapFailureException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.rest.AbstractRestService;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.rest.coordinator.config.StoreClientConfigService;
import voldemort.rest.coordinator.config.StoreClientConfigServiceListener;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;

/*
 * A Netty based service that accepts REST requests from the Voldemort thin
 * clients and invokes the corresponding fat client API.
 * 
 * In other words, this is what proxies client requests to the Voldemort nodes.
 */

@JmxManaged(description = "Coordinator Service for proxying Voldemort requests from the thin client")
public class CoordinatorProxyService extends AbstractRestService implements
        StoreClientConfigServiceListener {

    private CoordinatorConfig coordinatorConfig = null;
    private SocketStoreClientFactory storeClientFactory = null;
    private AsyncMetadataVersionManager asyncMetadataManager = null;
    private final CoordinatorMetadata coordinatorMetadata;
    private SchedulerService schedulerService = null;
    private static final Logger logger = Logger.getLogger(CoordinatorProxyService.class);
    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap = null;
    private final StoreStats coordinatorPerfStats;
    private final StoreClientConfigService storeClientConfigs;

    public CoordinatorProxyService(CoordinatorConfig config,
                                   StoreClientConfigService storeClientConfigs) {
        super(ServiceType.COORDINATOR_PROXY, config);
        this.storeClientConfigs = storeClientConfigs;
        this.coordinatorConfig = config;
        this.coordinatorPerfStats = new StoreStats("aggregate.proxy-service");
        this.coordinatorMetadata = new CoordinatorMetadata();
        storeClientConfigs.registerListener(ServiceType.COORDINATOR_PROXY, this);
    }

    /**
     * Updates the CoordinatorMetdada with latest cluster state
     */
    private void updateCoordinatorMetadataWithLatestState() {
        // Fetch the state once and use this to initialize the fat clients
        String storesXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
        String clusterXml = storeClientFactory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);

        // Update the Coordinator Metadata
        this.coordinatorMetadata.setMetadata(clusterXml, storesXml);
    }

    /**
     * Initialize the fat client for the given store.
     * 
     * 1. Updates the coordinatorMetadata 2.Gets the new store configs from the
     * config file 3.Creates a new @SocketStoreClientFactory 4. Subsequently
     * caches the @StoreClient obtained from the factory.
     * 
     * 
     * This is synchronized because if Coordinator Admin is already doing some
     * change we want the AsyncMetadataVersionManager to wait.
     * 
     * @param storeName
     */
    private synchronized void initializeFatClient(String storeName, Properties storeClientProps) {
        // updates the coordinator metadata with recent stores and cluster xml
        updateCoordinatorMetadataWithLatestState();

        logger.info("Creating a Fat client for store: " + storeName);
        SocketStoreClientFactory fatClientFactory = getFatClientFactory(this.coordinatorConfig.getBootstrapURLs(),
                                                                        storeClientProps);

        if(this.fatClientMap == null) {
            this.fatClientMap = new HashMap<String, DynamicTimeoutStoreClient<ByteArray, byte[]>>();
        }
        DynamicTimeoutStoreClient<ByteArray, byte[]> fatClient = new DynamicTimeoutStoreClient<ByteArray, byte[]>(storeName,
                                                                                                                  fatClientFactory,
                                                                                                                  1,
                                                                                                                  this.coordinatorMetadata.getStoreDefs(),
                                                                                                                  this.coordinatorMetadata.getClusterXmlStr());
        this.fatClientMap.put(storeName, fatClient);

    }

    /**
     * Create a @SocketStoreClientFactory from the given configPops
     * 
     * @param bootstrapURLs
     * @param configProps
     * @return
     */
    private SocketStoreClientFactory getFatClientFactory(String[] bootstrapURLs,
                                                         Properties configProps) {

        ClientConfig fatClientConfig = new ClientConfig(configProps);
        logger.info("Using config: " + fatClientConfig);
        fatClientConfig.setBootstrapUrls(bootstrapURLs)
                       .setEnableCompressionLayer(false)
                       .setEnableSerializationLayer(false)
                       .enableDefaultClient(true)
                       .setEnableLazy(false);

        return new SocketStoreClientFactory(fatClientConfig);
    }

    /**
     * Initializes all the Fat clients (1 per store) for the cluster that this
     * Coordinator talks to. This is invoked once during startup and then every
     * time the Metadata manager detects changes to the cluster and stores
     * metadata.
     * 
     * This method is synchronized because we do not want Coordinator Admin
     * changes to interfere with Async metadata version manager
     * 
     */
    private synchronized void initializeAllFatClients() {
        updateCoordinatorMetadataWithLatestState();

        // get All stores defined in the config file
        Map<String, Properties> storeClientConfigsMap = storeClientConfigs.getAllConfigsMap();

        for(StoreDefinition storeDef: this.coordinatorMetadata.getStoresDefs()) {
            String storeName = storeDef.getName();
            // Initialize only those stores defined in the client configs file
            if(storeClientConfigsMap.get(storeName) != null) {
                initializeFatClient(storeName, storeClientConfigsMap.get(storeName));
            }
        }
    }

    private synchronized void deleteFatClient(String storeName) {
        /*
         * We depend on GarbageCollector here to get rid of the previous
         * instantiation of SocketStoreClientFactory for this store <storeName>
         * FIXME Currently the SocketStoreClientFactories do not have a shutdown
         * method. If connections leak or selector threads are not Garbage
         * Collected in future, we may need to implement the Shutdown method for
         * SocketStoreClientFactory to gracefully shutdown and call the factory
         * shutdown method here
         */
        if(this.fatClientMap.containsKey(storeName)) {
            fatClientMap.remove(storeName);
        }
    }

    @Override
    protected void initialize() {
        // Initialize the Voldemort Metadata
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(this.coordinatorConfig.getBootstrapURLs());
        storeClientFactory = new SocketStoreClientFactory(clientConfig);
        try {
            initializeAllFatClients();
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
                    initializeAllFatClients();
                    return null;
                }

            };
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
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected ChannelPipelineFactory getPipelineFactory() {
        return (new CoordinatorPipelineFactory(this.fatClientMap,
                                               this.coordinatorMetadata,
                                               this.coordinatorConfig,
                                               this.coordinatorPerfStats,
                                               this.connectionStats));
    }

    @Override
    protected int getServicePort() {
        return this.coordinatorConfig.getServerPort();
    }

    @Override
    protected String getServiceName() {
        return ServiceType.COORDINATOR_PROXY.getDisplayName();
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

    @Override
    public void onStoreConfigAddOrUpdate(String storeName, Properties storeClientProps) {
        initializeFatClient(storeName, storeClientProps);
    }

    @Override
    public void onStoreConfigDelete(String storeName) {
        deleteFatClient(storeName);
    }

}

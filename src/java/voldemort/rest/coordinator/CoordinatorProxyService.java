/*
 * Copyright 2014 LinkedIn, Inc <<<<<<<
 * HEAD:src/java/voldemort/rest/coordinator/CoordinatorProxyService.java
 * 
 * =======
 * 
 * >>>>>>>
 * c5995a789325ff959482e0eb39c54eebb911dd70:src/java/voldemort/rest/coordinator
 * /CoordinatorProxyService.java Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
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
import voldemort.rest.coordinator.config.ClientConfigUtil;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.rest.coordinator.config.StoreClientConfigService;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

/*
 * A Netty based service that accepts REST requests from the Voldemort thin
 * clients and invokes the corresponding fat client API.
 *
 * In other words, this is what proxies client requests to the Voldemort nodes.
 */

@JmxManaged(description = "Coordinator Service for proxying Voldemort requests from the thin client")
public class CoordinatorProxyService extends AbstractRestService {

    private CoordinatorConfig coordinatorConfig = null;
    private SocketStoreClientFactory storeClientFactory = null;
    private AsyncMetadataVersionManager asyncMetadataManager = null;
    private final CoordinatorMetadata coordinatorMetadata;
    private SchedulerService schedulerService = null;
    private static final Logger logger = Logger.getLogger(CoordinatorProxyService.class);
    private Map<String, DynamicTimeoutStoreClient<ByteArray, byte[]>> fatClientMap = null;
    private final StoreStats coordinatorPerfStats;

    public CoordinatorProxyService(CoordinatorConfig config) {
        super(ServiceType.COORDINATOR_PROXY, config);
        StoreClientConfigService.initialize(config);
        this.coordinatorConfig = config;
        this.coordinatorPerfStats = new StoreStats("aggregate.proxy-service");
        this.coordinatorMetadata = new CoordinatorMetadata();
    }

    /**
     * Initializes all the Fat clients (1 per store) for the cluster that this
     * Coordinator talks to. This is invoked once during startup and then every
     * time the Metadata manager detects changes to the cluster and stores
     * metadata.
     * 
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

        // Do not recreate map if it already exists. This function might be
        // called by the AsyncMetadataVersionManager
        // if there is a metadata update on the server side

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
    protected void initialize() {
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

    /**
     * A function to parse the specified Avro file in order to obtain the config
     * for each fat client managed by this coordinator.
     * 
     * @param configFilePath Path of the Avro file containing fat client configs
     * @param bootstrapURLs The server URLs used during bootstrap
     * @return Map of store name to the corresponding fat client config
     */
    private static Map<String, SocketStoreClientFactory> readClientConfig(String configFilePath,
                                                                          String[] bootstrapURLs) {
        String line;
        Map<String, SocketStoreClientFactory> storeFactoryMap = new HashMap<String, SocketStoreClientFactory>();
        try {
            line = Joiner.on(" ")
                         .join(IOUtils.readLines(new FileReader(new File(configFilePath))))
                         .trim();

            Map<String, Properties> mapStoreToProps = ClientConfigUtil.readMultipleClientConfigAvro(line);

            for(String storeName: mapStoreToProps.keySet()) {
                Properties props = mapStoreToProps.get(storeName);

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

        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return storeFactoryMap;
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

}

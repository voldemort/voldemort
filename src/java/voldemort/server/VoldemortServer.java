/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.server;

import static voldemort.utils.Utils.croak;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.gossip.GossipService;
import voldemort.server.http.HttpService;
import voldemort.server.jmx.JmxService;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.protocol.admin.AsyncOperationRunner;
import voldemort.server.rebalance.Rebalancer;
import voldemort.server.rebalance.RebalancerService;
import voldemort.server.scheduler.SchedulerService;
import voldemort.server.socket.AdminService;
import voldemort.server.socket.SocketService;
import voldemort.server.storage.StorageService;
import voldemort.store.configuration.ConfigurationStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.ImmutableList;

/**
 * This is the main server, it bootstraps all the services.
 * 
 * It can be embedded or run directly via it's main method.
 * 
 * @author jay
 * 
 */
public class VoldemortServer extends AbstractService {

    private static final Logger logger = Logger.getLogger(VoldemortServer.class.getName());
    public static final long DEFAULT_PUSHER_POLL_MS = 60 * 1000;

    private final static int ASYNC_REQUEST_CACHE_SIZE = 64;

    private final Node identityNode;
    private final List<VoldemortService> services;
    private final StoreRepository storeRepository;
    private final VoldemortConfig voldemortConfig;
    private final MetadataStore metadata;
    private AsyncOperationRunner asyncRunner;

    public VoldemortServer(VoldemortConfig config) {
        super(ServiceType.VOLDEMORT);
        this.voldemortConfig = config;
        this.storeRepository = new StoreRepository();
        this.metadata = MetadataStore.readFromDirectory(new File(this.voldemortConfig.getMetadataDirectory()),
                                                        voldemortConfig.getNodeId());
        this.identityNode = metadata.getCluster().getNodeById(voldemortConfig.getNodeId());
        this.services = createServices();
    }

    public VoldemortServer(VoldemortConfig config, Cluster cluster) {
        super(ServiceType.VOLDEMORT);
        this.voldemortConfig = config;
        this.identityNode = cluster.getNodeById(voldemortConfig.getNodeId());
        this.storeRepository = new StoreRepository();
        // update cluster details in metaDataStore
        ConfigurationStorageEngine metadataInnerEngine = new ConfigurationStorageEngine("metadata-config-store",
                                                                                        voldemortConfig.getMetadataDirectory());
        metadataInnerEngine.put(MetadataStore.CLUSTER_KEY,
                                new Versioned<String>(new ClusterMapper().writeCluster(cluster)));
        this.metadata = new MetadataStore(metadataInnerEngine, voldemortConfig.getNodeId());

        this.services = createServices();
    }

    public AsyncOperationRunner getAsyncRunner() {
        return asyncRunner;
    }

    private List<VoldemortService> createServices() {

        /* Services are given in the order they must be started */
        List<VoldemortService> services = new ArrayList<VoldemortService>();
        SchedulerService scheduler = new SchedulerService(voldemortConfig.getSchedulerThreads(),
                                                          SystemTime.INSTANCE);
        StorageService storageService = new StorageService(storeRepository,
                                                           metadata,
                                                           scheduler,
                                                           voldemortConfig);

        asyncRunner = new AsyncOperationRunner(scheduler, ASYNC_REQUEST_CACHE_SIZE);

        services.add(storageService);
        services.add(scheduler);
        services.add(asyncRunner);

        if(voldemortConfig.isHttpServerEnabled())
            services.add(new HttpService(this,
                                         storeRepository,
                                         RequestFormatType.VOLDEMORT_V1,
                                         voldemortConfig.getMaxThreads(),
                                         identityNode.getHttpPort()));
        if(voldemortConfig.isSocketServerEnabled()) {
            RequestHandlerFactory socketRequestHandlerFactory = new SocketRequestHandlerFactory(this.storeRepository,
                                                                                                this.metadata,
                                                                                                this.voldemortConfig,
                                                                                                this.asyncRunner,
                                                                                                null);

            if(voldemortConfig.getUseNioConnector()) {
                logger.info("Using NIO Connector.");
                services.add(new NioSocketService(socketRequestHandlerFactory,
                                                  identityNode.getSocketPort(),
                                                  voldemortConfig.getSocketBufferSize(),
                                                  voldemortConfig.getNioConnectorSelectors(),
                                                  "nio-socket-server",
                                                  voldemortConfig.isJmxEnabled()));
            } else {
                logger.info("Using BIO Connector.");
                services.add(new SocketService(socketRequestHandlerFactory,
                                               identityNode.getSocketPort(),
                                               voldemortConfig.getCoreThreads(),
                                               voldemortConfig.getMaxThreads(),
                                               voldemortConfig.getSocketBufferSize(),
                                               "socket-server",
                                               voldemortConfig.isJmxEnabled()));
            }
        }

        if(voldemortConfig.isAdminServerEnabled()) {
            Rebalancer rebalancer = null;
            if(voldemortConfig.isEnableRebalanceService()) {
                RebalancerService rebalancerService = new RebalancerService(metadata,
                                                                            voldemortConfig,
                                                                            asyncRunner,
                                                                            scheduler);
                services.add(rebalancerService);
                rebalancer = rebalancerService.getRebalancer();
            }

            SocketRequestHandlerFactory adminRequestHandlerFactory = new SocketRequestHandlerFactory(this.storeRepository,
                                                                                                     this.metadata,
                                                                                                     this.voldemortConfig,
                                                                                                     this.asyncRunner,
                                                                                                     rebalancer);
            services.add(new AdminService(adminRequestHandlerFactory,
                                          identityNode.getAdminPort(),
                                          voldemortConfig.getAdminCoreThreads(),
                                          voldemortConfig.getAdminMaxThreads(),
                                          voldemortConfig.getAdminSocketBufferSize(),
                                          "admin-server",
                                          voldemortConfig.isJmxEnabled()));
        }

        if(voldemortConfig.isGossipEnabled()) {
            services.add(new GossipService(this.metadata, scheduler, voldemortConfig));
        }

        if(voldemortConfig.isJmxEnabled())
            services.add(new JmxService(this, this.metadata.getCluster(), storeRepository, services));

        return ImmutableList.copyOf(services);
    }

    @Override
    protected void startInner() throws VoldemortException {
        logger.info("Starting " + services.size() + " services.");
        long start = System.currentTimeMillis();
        for(VoldemortService service: services)
            service.start();
        long end = System.currentTimeMillis();
        logger.info("Startup completed in " + (end - start) + " ms.");
    }

    /**
     * Attempt to shutdown the server. As much shutdown as possible will be
     * completed, even if intermediate errors are encountered.
     * 
     * @throws VoldemortException
     */
    @Override
    protected void stopInner() throws VoldemortException {
        List<VoldemortException> exceptions = new ArrayList<VoldemortException>();

        logger.info("Stopping services:" + getIdentityNode().getId());
        /* Stop in reverse order */
        for(VoldemortService service: Utils.reversed(services)) {
            try {
                service.stop();
            } catch(VoldemortException e) {
                exceptions.add(e);
                logger.error(e);
            }
        }
        logger.info("All services stopped for Node:" + getIdentityNode().getId());

        if(exceptions.size() > 0)
            throw exceptions.get(0);
    }

    public static void main(String[] args) throws Exception {
        VoldemortConfig config = null;
        try {
            if(args.length == 0)
                config = VoldemortConfig.loadFromEnvironmentVariable();
            else if(args.length == 1)
                config = VoldemortConfig.loadFromVoldemortHome(args[0]);
            else
                croak("USAGE: java " + VoldemortServer.class.getName() + " [voldemort_home_dir]");
        } catch(Exception e) {
            logger.error(e);
            Utils.croak("Error while loading configuration: " + e.getMessage());
        }

        final VoldemortServer server = new VoldemortServer(config);
        if(!server.isStarted())
            server.start();

        // add a shutdown hook to stop the server
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                if(server.isStarted())
                    server.stop();
            }
        });
    }

    public Node getIdentityNode() {
        return this.identityNode;
    }

    public List<VoldemortService> getServices() {
        return services;
    }

    public VoldemortService getService(ServiceType type) {
        for(VoldemortService service: services)
            if(service.getType().equals(type))
                return service;
        throw new IllegalStateException(type.getDisplayName() + " has not been initialized.");
    }

    public VoldemortConfig getVoldemortConfig() {
        return this.voldemortConfig;
    }

    public StoreRepository getStoreRepository() {
        return this.storeRepository;
    }

    public MetadataStore getMetadataStore() {
        return metadata;
    }

    @JmxOperation(description = "force restore data from replication")
    public void restoreDataFromReplication(int numberOfParallelTransfers) {

        AdminClient adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                                       metadata.getCluster(),
                                                                       numberOfParallelTransfers * 2,
                                                                       numberOfParallelTransfers * 2);
        try {
            adminClient.restoreDataFromReplications(metadata.getNodeId(), numberOfParallelTransfers);
        } finally {
            adminClient.stop();
        }
    }
}

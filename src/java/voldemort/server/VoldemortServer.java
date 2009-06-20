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
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.http.HttpService;
import voldemort.server.jmx.JmxService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.scheduler.SchedulerService;
import voldemort.server.socket.SocketService;
import voldemort.server.storage.StorageService;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
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

    private final Node identityNode;
    private final List<VoldemortService> services;
    private final StoreRepository storeRepository;
    private final VoldemortConfig voldemortConfig;
    private VoldemortMetadata metadata;

    public VoldemortServer(VoldemortConfig config) {
        super(ServiceType.VOLDEMORT);
        this.voldemortConfig = config;
        this.storeRepository = new StoreRepository();
        this.metadata = new VoldemortMetadata(this.voldemortConfig.getMetadataDirectory(),
                                              voldemortConfig.getNodeId());
        this.identityNode = metadata.getCurrentCluster().getNodeById(voldemortConfig.getNodeId());
        this.services = createServices();
    }

    public VoldemortServer(VoldemortConfig config, Cluster cluster) {
        super(ServiceType.VOLDEMORT);
        this.voldemortConfig = config;
        this.identityNode = cluster.getNodeById(voldemortConfig.getNodeId());
        this.storeRepository = new StoreRepository();
        // update cluster details in metaDataStore
        MetadataStore metadataStore = MetadataStore.readFromDirectory(new File(this.voldemortConfig.getMetadataDirectory()));
        metadataStore.put(new ByteArray(ByteUtils.getBytes(MetadataStore.CLUSTER_KEY, "UTF-8")),
                          new Versioned<byte[]>(ByteUtils.getBytes(new ClusterMapper().writeCluster(cluster),
                                                                   "UTF-8"),
                                                new VectorClock()));
        this.metadata = new VoldemortMetadata(this.voldemortConfig.getMetadataDirectory(),
                                              this.voldemortConfig.getNodeId());
        metadata.setCurrentCluster(cluster);
        this.services = createServices();
    }

    private List<VoldemortService> createServices() {
        RequestHandlerFactory requestHandlerFactory = new RequestHandlerFactory(this.storeRepository,
                                                                                this.metadata,
                                                                                voldemortConfig);

        /* Services are given in the order they must be started */
        List<VoldemortService> services = new ArrayList<VoldemortService>();
        SchedulerService scheduler = new SchedulerService(voldemortConfig.getSchedulerThreads(),
                                                          SystemTime.INSTANCE);
        services.add(new StorageService(storeRepository, metadata, scheduler, voldemortConfig));
        services.add(scheduler);
        if(voldemortConfig.isHttpServerEnabled())
            services.add(new HttpService(this,
                                         storeRepository,
                                         RequestFormatType.VOLDEMORT,
                                         voldemortConfig.getMaxThreads(),
                                         identityNode.getHttpPort()));
        if(voldemortConfig.isSocketServerEnabled())
            services.add(new SocketService(requestHandlerFactory.getRequestHandler(voldemortConfig.getRequestFormatType()),
                                           identityNode.getSocketPort(),
                                           voldemortConfig.getCoreThreads(),
                                           voldemortConfig.getMaxThreads(),
                                           voldemortConfig.getSocketBufferSize(),
                                           "client-request-server",
                                           voldemortConfig.isJmxEnabled()));
        if(voldemortConfig.isAdminServerEnabled())
            services.add(new SocketService(requestHandlerFactory.getRequestHandler(RequestFormatType.ADMIN_HANDLER),
                                           identityNode.getAdminPort(),
                                           voldemortConfig.getAdminCoreThreads(),
                                           voldemortConfig.getAdminMaxThreads(),
                                           voldemortConfig.getAdminSocketBufferSize(),
                                           "admin-server",
                                           voldemortConfig.isJmxEnabled()));

        if(voldemortConfig.isJmxEnabled())
            services.add(new JmxService(this,
                                        this.metadata.getCurrentCluster(),
                                        storeRepository,
                                        services));

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

        logger.info("Stopping services:");
        /* Stop in reverse order */
        for(VoldemortService service: Utils.reversed(services)) {
            try {
                service.stop();
            } catch(VoldemortException e) {
                exceptions.add(e);
                logger.error(e);
            }
        }
        logger.info("All services stopped.");

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

    public VoldemortMetadata getVoldemortMetadata() {
        return metadata;
    }
}

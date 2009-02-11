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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.http.HttpService;
import voldemort.server.jmx.JmxService;
import voldemort.server.scheduler.SchedulerService;
import voldemort.server.socket.SocketService;
import voldemort.server.storage.StorageService;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.Props;
import voldemort.utils.SystemTime;
import voldemort.utils.Utils;

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
    private final Cluster cluster;
    private final MetadataStore metadataStore;
    private final List<VoldemortService> services;
    private final ConcurrentMap<String, Store<byte[], byte[]>> storeMap;
    private final VoldemortConfig voldemortConfig;

    public VoldemortServer(VoldemortConfig config) {
        super("voldemort-server");
        this.voldemortConfig = config;
        this.storeMap = new ConcurrentHashMap<String, Store<byte[], byte[]>>();
        this.metadataStore = new MetadataStore(new File(voldemortConfig.getMetadataDirectory()),
                                               storeMap);
        this.cluster = this.metadataStore.getCluster();
        this.identityNode = this.cluster.getNodeById(voldemortConfig.getNodeId());
        this.services = createServices();
    }

    public VoldemortServer(Props props, Cluster cluster) {
        super("voldemort-server");
        this.voldemortConfig = new VoldemortConfig(props);
        this.cluster = cluster;
        this.identityNode = cluster.getNodeById(voldemortConfig.getNodeId());
        this.storeMap = new ConcurrentHashMap<String, Store<byte[], byte[]>>();
        this.services = createServices();
        this.metadataStore = new MetadataStore(new File(voldemortConfig.getMetadataDirectory()),
                                               storeMap);
    }

    private List<VoldemortService> createServices() {
        List<VoldemortService> services = Collections.synchronizedList(new ArrayList<VoldemortService>());
        SchedulerService scheduler = new SchedulerService("scheduler-service",
                                                          voldemortConfig.getSchedulerThreads(),
                                                          SystemTime.INSTANCE);
        services.add(scheduler);
        services.add(new StorageService("storage-service",
                                        this.storeMap,
                                        scheduler,
                                        voldemortConfig));
        if(voldemortConfig.isHttpServerEnabled())
            services.add(new HttpService("http-service",
                                         this,
                                         voldemortConfig.getMaxThreads(),
                                         identityNode.getHttpPort()));
        if(voldemortConfig.isSocketServerEnabled())
            services.add(new SocketService("socket-service",
                                           storeMap,
                                           identityNode.getSocketPort(),
                                           voldemortConfig.getCoreThreads(),
                                           voldemortConfig.getMaxThreads()));
        if(voldemortConfig.isJmxEnabled())
            services.add(new JmxService("jmx-service", this, cluster, storeMap, services));

        return services;
    }

    protected void startInner() throws VoldemortException {
        logger.info("Starting all services: ");
        long start = System.currentTimeMillis();
        for(VoldemortService service: services)
            service.start();
        long end = System.currentTimeMillis();

        // add a shutdown hook to stop the server
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                if(VoldemortServer.this.isStarted())
                    VoldemortServer.this.stop();
            }
        });

        logger.info("Startup completed in " + (end - start) + " ms.");
    }

    /**
     * Attempt to shutdown the server. As much shutdown as possible will be
     * completed, even if intermediate errors are encountered.
     * 
     * @throws VoldemortException
     */
    protected void stopInner() throws VoldemortException {
        List<VoldemortException> exceptions = new ArrayList<VoldemortException>();
        logger.info("Stoping services:");
        for(VoldemortService service: services) {
            try {
                logger.info("Stoping " + service.getName() + ".");
                service.stop();
            } catch(VoldemortException e) {
                exceptions.add(e);
                logger.error(e);
            }
        }
        logger.info("All services stoped.");

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

        VoldemortServer server = new VoldemortServer(config);
        if(!server.isStarted())
            server.start();
    }

    public Node getIdentityNode() {
        return this.identityNode;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<VoldemortService> getServices() {
        return services;
    }

    public VoldemortService getService(String name) {
        for(VoldemortService service: services)
            if(service.getName().equals(name))
                return service;
        return null;
    }

    public ConcurrentMap<String, Store<byte[], byte[]>> getStoreMap() {
        return storeMap;
    }

    public VoldemortConfig getVoldemortConfig() {
        return this.voldemortConfig;
    }

}

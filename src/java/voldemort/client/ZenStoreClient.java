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

package voldemort.client;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.client.scheduler.ClientRegistryRefresher;
import voldemort.common.service.SchedulerService;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.JmxUtils;
import voldemort.utils.ManifestFileReader;
import voldemort.utils.SystemTime;
import voldemort.utils.Utils;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The enhanced {@link voldemort.client.StoreClient StoreClient} implementation
 * you get back from a {@link voldemort.client.StoreClientFactory
 * StoreClientFactory}
 * 
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
@Threadsafe
@JmxManaged(description = "A voldemort client")
public class ZenStoreClient<K, V> extends DefaultStoreClient<K, V> {

    private static final int ASYNC_THREADS_COUNT = 2;
    private static final boolean ALLOW_INTERRUPT_ASYNC = true;

    private final Logger logger = Logger.getLogger(ZenStoreClient.class);

    private final AbstractStoreClientFactory abstractStoreFactory;
    private final ClientConfig config;
    private final SystemStoreRepository sysRepository;
    private final UUID clientId;
    private final SchedulerService scheduler;
    private ClientInfo clientInfo;
    private String clusterXml;
    private AsyncMetadataVersionManager asyncCheckMetadata = null;

    public ZenStoreClient(String storeName,
                               InconsistencyResolver<Versioned<V>> resolver,
                               AbstractStoreClientFactory storeFactory,
                               int maxMetadataRefreshAttempts) {
        this(storeName, resolver, storeFactory, maxMetadataRefreshAttempts, null, 0, null);
    }

    public ZenStoreClient(String storeName,
                               InconsistencyResolver<Versioned<V>> resolver,
                               AbstractStoreClientFactory storeFactory,
                               int maxMetadataRefreshAttempts,
                               String clientContext,
                               int clientSequence,
                               ClientConfig config) {

        super();
        this.storeName = Utils.notNull(storeName);
        this.resolver = resolver;
        this.abstractStoreFactory = Utils.notNull(storeFactory);
        this.storeFactory = this.abstractStoreFactory;
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;
        this.clientInfo = new ClientInfo(storeName,
                                         clientContext,
                                         clientSequence,
                                         System.currentTimeMillis(),
                                         ManifestFileReader.getReleaseVersion());
        this.clientId = AbstractStoreClientFactory.generateClientId(clientInfo);
        this.config = config;
        this.sysRepository = new SystemStoreRepository();
        this.scheduler = new SchedulerService(ASYNC_THREADS_COUNT,
                                              SystemTime.INSTANCE,
                                              ALLOW_INTERRUPT_ASYNC);
        // Registering self to be able to bootstrap client dynamically via JMX
        JmxUtils.registerMbean(this,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.getClass())
                                                                 + "." + clientContext + "."
                                                                 + storeName + "."
                                                                 + clientId.toString()));

        // Bootstrap this client
        bootStrap();

        // Initialize the background thread for checking metadata version
        if(config != null) {
            asyncCheckMetadata = scheduleMetadataChecker(clientId.toString(),
                                                         config.getAsyncMetadataRefreshInMs());
        }

        registerClient(clientId.toString(), config.getClientRegistryUpdateInSecs());
        logger.info("Voldemort client created: " + clientId.toString() + "\n" + clientInfo);
    }

    private void registerClient(String jobId, int interval) {
        SystemStore<String, String> clientRegistry = this.sysRepository.getClientRegistryStore();
        if(null != clientRegistry) {
            try {
                Version version = clientRegistry.putSysStore(clientId.toString(),
                                                             clientInfo.toString());
                ClientRegistryRefresher refresher = new ClientRegistryRefresher(clientRegistry,
                                                                                clientId.toString(),
                                                                                clientInfo,
                                                                                version);
                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.SECOND, interval);
                scheduler.schedule(jobId + refresher.getClass().getName(),
                                   refresher,
                                   cal.getTime(),
                                   interval * 1000);
                logger.info("Client registry refresher thread started, refresh frequency: "
                            + interval + " seconds");
            } catch(Exception e) {
                logger.warn("Unable to register with the cluster due to the following error:", e);
            }
        } else {
            logger.warn(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name()
                        + "not found. Unable to registry with voldemort cluster.");
        }
    }

    private AsyncMetadataVersionManager scheduleMetadataChecker(String jobId, long interval) {
        AsyncMetadataVersionManager asyncCheckMetadata = null;
        SystemStore<String, Long> versionStore = this.sysRepository.getVersionStore();
        if(versionStore == null)
            logger.warn("Metadata version system store not found. Cannot run Metadata version check thread.");
        else {

            // Create a callback for re-bootstrapping the client
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                public Void call() throws Exception {
                    bootStrap();
                    return null;
                }
            };

            asyncCheckMetadata = new AsyncMetadataVersionManager(this.sysRepository,
                                                                 rebootstrapCallback,
                                                                 null);

            // schedule the job to run every 'checkInterval' period, starting
            // now
            scheduler.schedule(jobId + asyncCheckMetadata.getClass().getName(),
                               asyncCheckMetadata,
                               new Date(),
                               interval);
            logger.info("Metadata version check thread started. Frequency = Every " + interval
                        + " ms");

        }
        return asyncCheckMetadata;
    }

    @Override
    @JmxOperation(description = "bootstrap metadata from the cluster.")
    public void bootStrap() {
        logger.info("Bootstrapping metadata for store " + this.storeName);

        /*
         * Since we need cluster.xml for bootstrapping this client as well as
         * all the System stores, just fetch it once and pass it around.
         */
        clusterXml = abstractStoreFactory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY);

        this.store = abstractStoreFactory.getRawStore(storeName,
                                                      resolver,
                                                      clientId,
                                                      null,
                                                      clusterXml);

        // Create system stores
        logger.info("Creating system stores for store " + this.storeName);
        this.sysRepository.createSystemStores(this.config, this.clusterXml);

        /*
         * Update to the new metadata versions (in case we got here from Invalid
         * Metadata exception). This will prevent another bootstrap via the
         * Async metadata checker
         */
        if(asyncCheckMetadata != null) {
            asyncCheckMetadata.updateMetadataVersions();
        }

        if(this.clientInfo != null) {
            this.clientInfo.setBootstrapTime(System.currentTimeMillis());
        }
    }

    public void close() {
        scheduler.stopInner();
    }

    public UUID getClientId() {
        return clientId;
    }

    @JmxGetter(name = "getStoreMetadataVersion")
    public String getStoreMetadataVersion() {
        String result = "Current Store Metadata Version : "
                        + this.asyncCheckMetadata.getStoreMetadataVersion();
        return result;
    }

    @JmxGetter(name = "getClusterMetadataVersion")
    public String getClusterMetadataVersion() {
        String result = "Current Cluster Metadata Version : "
                        + this.asyncCheckMetadata.getClusterMetadataVersion();
        return result;
    }
}

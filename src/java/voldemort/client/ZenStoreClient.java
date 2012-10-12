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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

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

    private final Logger logger = Logger.getLogger(ZenStoreClient.class);

    private final AbstractStoreClientFactory abstractStoreFactory;
    private final ClientConfig config;
    private final SystemStoreRepository sysRepository;
    private final String clientId;
    private final SchedulerService scheduler;
    private ClientInfo clientInfo;
    private String clusterXml;
    private AsyncMetadataVersionManager asyncMetadataManager = null;
    private ClientRegistryRefresher clientRegistryRefresher = null;

    public ZenStoreClient(String storeName,
                          InconsistencyResolver<Versioned<V>> resolver,
                          AbstractStoreClientFactory storeFactory,
                          int maxMetadataRefreshAttempts) {
        this(storeName, resolver, storeFactory, maxMetadataRefreshAttempts, null, 0, null, null);
    }

    public ZenStoreClient(String storeName,
                          InconsistencyResolver<Versioned<V>> resolver,
                          AbstractStoreClientFactory storeFactory,
                          int maxMetadataRefreshAttempts,
                          String clientContext,
                          int clientSequence,
                          ClientConfig config,
                          SchedulerService scheduler) {

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
                                         ManifestFileReader.getReleaseVersion(),
                                         config);
        this.clientId = generateClientId(clientInfo);
        this.config = config;
        this.sysRepository = new SystemStoreRepository();
        this.scheduler = scheduler;

        // Registering self to be able to bootstrap client dynamically via JMX
        JmxUtils.registerMbean(this,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.getClass())
                                                                 + "." + storeName));

        // Bootstrap this client
        bootStrap();

        // Initialize the background thread for checking metadata version
        if(config != null) {
            asyncMetadataManager = scheduleAsyncMetadataVersionManager(clientId.toString(),
                                                                       config.getAsyncMetadataRefreshInMs());
        }

        clientRegistryRefresher = registerClient(clientId,
                                                 config.getClientRegistryUpdateIntervalInSecs());
        logger.info("Voldemort client created: " + clientId + "\n" + clientInfo);
    }

    private ClientRegistryRefresher registerClient(String jobId, int intervalInSecs) {
        ClientRegistryRefresher refresher = null;
        if(this.sysRepository.getClientRegistryStore() != null) {
            try {
                Version version = this.sysRepository.getClientRegistryStore()
                                                    .putSysStore(clientId, clientInfo.toString());
                refresher = new ClientRegistryRefresher(this.sysRepository,
                                                        clientId,
                                                        clientInfo,
                                                        version);
                GregorianCalendar cal = new GregorianCalendar();
                cal.add(Calendar.SECOND, intervalInSecs);

                if(scheduler != null) {
                    scheduler.schedule(jobId + refresher.getClass().getName(),
                                       refresher,
                                       cal.getTime(),
                                       TimeUnit.MILLISECONDS.convert(intervalInSecs,
                                                                     TimeUnit.SECONDS));
                    logger.info("Client registry refresher thread started, refresh interval: "
                                + intervalInSecs + " seconds");
                } else {
                    logger.warn("Client registry won't run because scheduler service is not configured");
                }
            } catch(Exception e) {
                logger.warn("Unable to register with the cluster due to the following error:", e);
            }
        } else {
            logger.warn(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name()
                        + "not found. Unable to registry with voldemort cluster.");
        }
        return refresher;
    }

    private AsyncMetadataVersionManager scheduleAsyncMetadataVersionManager(String jobId,
                                                                            long interval) {
        AsyncMetadataVersionManager asyncMetadataManager = null;
        SystemStore<String, String> versionStore = this.sysRepository.getMetadataVersionStore();
        if(versionStore == null) {
            logger.warn("Metadata version system store not found. Cannot run Metadata version check thread.");
        } else {

            // Create a callback for re-bootstrapping the client
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                public Void call() throws Exception {
                    bootStrap();
                    return null;
                }
            };

            asyncMetadataManager = new AsyncMetadataVersionManager(this.sysRepository,
                                                                   rebootstrapCallback,
                                                                   this.storeName);

            // schedule the job to run every 'checkInterval' period, starting
            // now
            if(scheduler != null) {
                scheduler.schedule(jobId + asyncMetadataManager.getClass().getName(),
                                   asyncMetadataManager,
                                   new Date(),
                                   interval);
                logger.info("Metadata version check thread started. Frequency = Every " + interval
                            + " ms");
            } else {
                logger.warn("Metadata version check thread won't start because the scheduler service is not configured.");
            }
        }
        return asyncMetadataManager;
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

        // Get client store
        this.store = abstractStoreFactory.getRawStore(storeName, resolver, null, clusterXml, null);

        // Create system stores
        logger.info("Creating system stores for store " + this.storeName);
        this.sysRepository.createSystemStores(this.config,
                                              this.clusterXml,
                                              abstractStoreFactory.getFailureDetector());

        /*
         * Update to the new metadata versions (in case we got here from Invalid
         * Metadata exception). This will prevent another bootstrap via the
         * Async metadata checker
         */
        if(asyncMetadataManager != null) {
            asyncMetadataManager.updateMetadataVersions();
        }

        /*
         * Every time we bootstrap, update the bootstrap time
         */
        if(this.clientInfo != null) {
            if(this.asyncMetadataManager != null) {
                this.clientInfo.setClusterMetadataVersion(this.asyncMetadataManager.getClusterMetadataVersion());
            }
            this.clientInfo.setBootstrapTime(System.currentTimeMillis());
        }

        if(this.clientRegistryRefresher == null) {
            logger.error("Unable to publish the client registry after bootstrap. Client Registry Refresher is NULL.");
        } else {
            logger.info("Publishing client registry after Bootstrap.");
            this.clientRegistryRefresher.publishRegistry();
        }
    }

    public String getClientId() {
        return clientId;
    }

    @JmxGetter(name = "getClusterMetadataVersion")
    public String getClusterMetadataVersion() {
        String result = "Current Cluster Metadata Version : "
                        + this.asyncMetadataManager.getClusterMetadataVersion();
        return result;
    }

    /**
     * Generate a unique client ID based on: 0. clientContext, if specified; 1.
     * storeName; 2. deployment path; 3. client sequence
     * 
     * @param storeName the name of the store the client is created for
     * @param contextName the name of the client context
     * @param clientSequence the client sequence number
     * @return unique client ID
     */
    public String generateClientId(ClientInfo clientInfo) {
        String contextName = clientInfo.getContext();
        int clientSequence = clientInfo.getClientSequence();

        String newLine = System.getProperty("line.separator");
        StringBuilder context = new StringBuilder(contextName == null ? "" : contextName);
        context.append(0 == clientSequence ? "" : ("." + clientSequence));
        context.append(".").append(clientInfo.getStoreName());
        context.append("@").append(clientInfo.getLocalHostName()).append(":");
        context.append(clientInfo.getDeploymentPath()).append(newLine);

        if(logger.isDebugEnabled()) {
            logger.debug(context.toString());
        }

        return context.toString();
    }
}

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

import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.StringSerializer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressingStore;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.utils.SystemTime;
import voldemort.versioning.ChainedResolver;
import voldemort.versioning.InconsistencyResolver;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Maps;

/**
 * A base class for various {@link voldemort.client.StoreClientFactory
 * StoreClientFactory} implementations
 * 
 * @author jay
 * 
 */
public abstract class AbstractStoreClientFactory implements StoreClientFactory {

    private static AtomicInteger jmxIdCounter = new AtomicInteger(0);

    public static final int DEFAULT_ROUTING_TIMEOUT_MS = 5000;
    public static final int DEFAULT_NODE_BANNAGE_MS = 10000;

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private static final Logger logger = Logger.getLogger(AbstractStoreClientFactory.class);

    private final URI[] bootstrapUrls;
    private final int routingTimeoutMs;
    private final int nodeBannageMs;
    private final ExecutorService threadPool;
    private final SerializerFactory serializerFactory;
    private final boolean isJmxEnabled;
    private final RequestFormatType requestFormatType;
    private final MBeanServer mbeanServer;
    private final int jmxId;
    private final int maxBootstrapRetries;

    public AbstractStoreClientFactory(ClientConfig config) {
        this.threadPool = new ClientThreadPool(config.getMaxThreads(),
                                               config.getThreadIdleTime(TimeUnit.MILLISECONDS),
                                               config.getMaxQueuedRequests());
        this.serializerFactory = config.getSerializerFactory();
        this.bootstrapUrls = validateUrls(config.getBootstrapUrls());
        this.routingTimeoutMs = config.getRoutingTimeout(TimeUnit.MILLISECONDS);
        this.nodeBannageMs = config.getNodeBannagePeriod(TimeUnit.MILLISECONDS);
        this.isJmxEnabled = config.isJmxEnabled();
        this.requestFormatType = config.getRequestFormatType();
        if(isJmxEnabled)
            this.mbeanServer = ManagementFactory.getPlatformMBeanServer();
        else
            this.mbeanServer = null;
        this.jmxId = jmxIdCounter.getAndIncrement();
        this.maxBootstrapRetries = config.getMaxBootstrapRetries();
        registerThreadPoolJmx(threadPool);
    }

    private void registerThreadPoolJmx(ExecutorService threadPool) {
        try {
            registerJmx(JmxUtils.createObjectName(JmxUtils.getPackageName(threadPool.getClass()),
                                                  JmxUtils.getClassName(threadPool.getClass())
                                                          + jmxId), threadPool);
        } catch(Exception e) {
            logger.error("Error registering threadpool jmx: ", e);
        }
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return getStoreClient(storeName, null);
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {

        return new DefaultStoreClient<K, V>(storeName, resolver, this, 3);
    }

    @SuppressWarnings("unchecked")
    public <K, V> Store<K, V> getRawStore(String storeName,
                                          InconsistencyResolver<Versioned<V>> resolver) {
        // Get cluster and store metadata
        String clusterXml = bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY, bootstrapUrls);
        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml));
        String storesXml = bootstrapMetadataWithRetries(MetadataStore.STORES_KEY, bootstrapUrls);
        List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesXml));
        StoreDefinition storeDef = null;
        for(StoreDefinition d: storeDefs)
            if(d.getName().equals(storeName))
                storeDef = d;
        if(storeDef == null)
            throw new BootstrapFailureException("Unknown store '" + storeName + "'.");

        // construct mapping
        Map<Integer, Store<ByteArray, byte[]>> clientMapping = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[]> store = getStore(storeDef.getName(),
                                                      node.getHost(),
                                                      getPort(node),
                                                      this.requestFormatType);
            store = new LoggingStore(store);
            clientMapping.put(node.getId(), store);
        }

        boolean repairReads = !storeDef.isView();
        Store<ByteArray, byte[]> store = new RoutedStore(storeName,
                                                         clientMapping,
                                                         cluster,
                                                         storeDef,
                                                         repairReads,
                                                         threadPool,
                                                         routingTimeoutMs,
                                                         nodeBannageMs,
                                                         SystemTime.INSTANCE);

        if(isJmxEnabled) {
            store = new StatTrackingStore(store);
            try {
                registerJmx(JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                      store.getName() + jmxId), store);
            } catch(Exception e) {
                logger.error("Error in JMX registration: ", e);
            }
        }

        if(storeDef.getKeySerializer().hasCompression()
           || storeDef.getValueSerializer().hasCompression()) {
            store = new CompressingStore(store,
                                         getCompressionStrategy(storeDef.getKeySerializer()),
                                         getCompressionStrategy(storeDef.getValueSerializer()));
        }

        Serializer<K> keySerializer = (Serializer<K>) serializerFactory.getSerializer(storeDef.getKeySerializer());
        Serializer<V> valueSerializer = (Serializer<V>) serializerFactory.getSerializer(storeDef.getValueSerializer());
        Store<K, V> serializedStore = SerializingStore.wrap(store, keySerializer, valueSerializer);

        // Add inconsistency resolving decorator, using their inconsistency
        // resolver (if they gave us one)
        InconsistencyResolver<Versioned<V>> secondaryResolver = resolver == null ? new TimeBasedInconsistencyResolver()
                                                                                : resolver;
        serializedStore = new InconsistencyResolvingStore<K, V>(serializedStore,
                                                                new ChainedResolver<Versioned<V>>(new VectorClockInconsistencyResolver(),
                                                                                                  secondaryResolver));
        return serializedStore;
    }

    private CompressionStrategy getCompressionStrategy(SerializerDefinition serializerDef) {
        return new CompressionStrategyFactory().get(serializerDef.getCompression());
    }

    public String bootstrapMetadataWithRetries(String key, URI[] urls) {
        int nTries = 0;
        while(nTries++ < this.maxBootstrapRetries) {
            try {
                return bootstrapMetadata(key, urls);
            } catch(BootstrapFailureException e) {
                if(nTries < this.maxBootstrapRetries) {
                    int backOffTime = 5 * nTries;
                    logger.warn("Failed to bootstrap will try again after " + backOffTime
                                + " seconds.");
                    try {
                        Thread.sleep(backOffTime * 1000);
                    } catch(InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            }
        }

        throw new BootstrapFailureException("No available boostrap servers found!");
    }

    private String bootstrapMetadata(String key, URI[] urls) {
        for(URI url: urls) {
            try {
                Store<ByteArray, byte[]> remoteStore = getStore(MetadataStore.METADATA_STORE_NAME,
                                                                url.getHost(),
                                                                url.getPort(),
                                                                this.requestFormatType);
                Store<String, String> store = SerializingStore.wrap(remoteStore,
                                                                    new StringSerializer("UTF-8"),
                                                                    new StringSerializer("UTF-8"));
                List<Versioned<String>> found = store.get(key);
                if(found.size() == 1)
                    return found.get(0).getValue();
            } catch(Exception e) {
                logger.warn("Failed to bootstrap from " + url);
                logger.debug(e);
            }
        }
        throw new BootstrapFailureException("No available boostrap servers found!");
    }

    public URI[] validateUrls(String[] urls) {
        if(urls == null || urls.length == 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL!");

        URI[] uris = new URI[urls.length];
        for(int i = 0; i < urls.length; i++) {
            if(urls[i] == null)
                throw new IllegalArgumentException("Null URL not allowed for bootstrapping!");
            URI uri = null;
            try {
                uri = new URI(urls[i]);
            } catch(URISyntaxException e) {
                throw new BootstrapFailureException(e);
            }

            if(uri.getHost() == null || uri.getHost().length() == 0)
                throw new IllegalArgumentException("Illegal scheme in bootstrap URL, must specify a host, URL: "
                                                   + uri);
            else if(uri.getPort() < 0)
                throw new IllegalArgumentException("Must specify a port in bootstrap URL, URL: "
                                                   + uri);
            else
                validateUrl(uri);

            uris[i] = uri;
        }

        return uris;
    }

    protected abstract Store<ByteArray, byte[]> getStore(String storeName,
                                                         String host,
                                                         int port,
                                                         RequestFormatType type);

    protected abstract int getPort(Node node);

    protected abstract void validateUrl(URI url);

    protected ExecutorService getThreadPool() {
        return this.threadPool;
    }

    public long getRoutingTimeoutMs() {
        return routingTimeoutMs;
    }

    public long getNodeBannageMs() {
        return nodeBannageMs;
    }

    public SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    protected void registerJmx(ObjectName name, Object object) {
        if(this.isJmxEnabled) {
            synchronized(mbeanServer) {
                try {

                    if(mbeanServer.isRegistered(name))
                        JmxUtils.unregisterMbean(mbeanServer, name);
                    JmxUtils.registerMbean(mbeanServer, JmxUtils.createModelMBean(object), name);
                } catch(Exception e) {
                    logger.error("Error while registering mbean: ", e);
                }
            }
        }
    }

}

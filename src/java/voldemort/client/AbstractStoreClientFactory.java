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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.SlopSerializer;
import voldemort.serialization.StringSerializer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressingStore;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.RoutedStoreFactory;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.store.versioned.InconsistencyResolvingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
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
 * 
 */
public abstract class AbstractStoreClientFactory implements StoreClientFactory {

    private static AtomicInteger jmxIdCounter = new AtomicInteger(0);

    public static final int DEFAULT_ROUTING_TIMEOUT_MS = 5000;

    protected static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    protected static final Logger logger = Logger.getLogger(AbstractStoreClientFactory.class);

    private static final Serializer<ByteArray> slopKeySerializer = new ByteArraySerializer();
    private static final Serializer<Slop> slopValueSerializer = new SlopSerializer();

    private final URI[] bootstrapUrls;
    private final ExecutorService threadPool;
    private final SerializerFactory serializerFactory;
    private final boolean isJmxEnabled;
    private final RequestFormatType requestFormatType;
    private final int jmxId;
    protected volatile FailureDetector failureDetector;
    private final int maxBootstrapRetries;
    private final StoreStats stats;
    private final ClientConfig config;
    private final RoutedStoreFactory routedStoreFactory;
    private final int clientZoneId;
    private final String clientContextName;
    private final AtomicInteger sequencer;

    public AbstractStoreClientFactory(ClientConfig config) {
        this.config = config;
        this.threadPool = new ClientThreadPool(config.getMaxThreads(),
                                               config.getThreadIdleTime(TimeUnit.MILLISECONDS),
                                               config.getMaxQueuedRequests());
        this.serializerFactory = config.getSerializerFactory();
        this.bootstrapUrls = validateUrls(config.getBootstrapUrls());
        this.isJmxEnabled = config.isJmxEnabled();
        this.requestFormatType = config.getRequestFormatType();
        this.jmxId = jmxIdCounter.getAndIncrement();
        this.maxBootstrapRetries = config.getMaxBootstrapRetries();
        this.stats = new StoreStats();
        this.clientZoneId = config.getClientZoneId();
        this.clientContextName = (null == config.getClientContextName() ? ""
                                                                       : config.getClientContextName());
        this.routedStoreFactory = new RoutedStoreFactory(config.isPipelineRoutedStoreEnabled(),
                                                         threadPool,
                                                         config.getRoutingTimeout(TimeUnit.MILLISECONDS));
        this.sequencer = new AtomicInteger(0);

        if(this.isJmxEnabled) {
            JmxUtils.registerMbean(threadPool,
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(threadPool.getClass()),
                                                             JmxUtils.getClassName(threadPool.getClass())
                                                                     + "."
                                                                     + clientContextName
                                                                     + jmxId()));
            JmxUtils.registerMbean(new StoreStatsJmx(stats),
                                   JmxUtils.createObjectName("voldemort.store.stats.aggregate",
                                                             clientContextName + ".aggregate-perf"
                                                                     + jmxId()));
        }
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return getStoreClient(storeName, null);
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        return new DefaultStoreClient<K, V>(storeName,
                                            resolver,
                                            this,
                                            3,
                                            clientContextName,
                                            sequencer.getAndIncrement());
    }

    @SuppressWarnings("unchecked")
    public <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                                InconsistencyResolver<Versioned<V>> resolver,
                                                UUID clientId) {

        logger.info("Client zone-id [" + clientZoneId
                    + "] Attempting to obtain metadata for store [" + storeName + "] ");
        if(logger.isDebugEnabled()) {
            for(URI uri: bootstrapUrls) {
                logger.debug("Client Bootstrap url [" + uri + "]");
            }
        }
        // Get cluster and store metadata
        String clusterXml = bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY, bootstrapUrls);
        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml), false);
        String storesXml = bootstrapMetadataWithRetries(MetadataStore.STORES_KEY, bootstrapUrls);

        if(logger.isDebugEnabled()) {
            logger.debug("Obtained cluster metadata xml" + clusterXml);
            logger.debug("Obtained stores  metadata xml" + storesXml);
        }

        List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesXml),
                                                                    false);
        StoreDefinition storeDef = null;
        for(StoreDefinition d: storeDefs)
            if(d.getName().equals(storeName))
                storeDef = d;
        if(storeDef == null)
            throw new BootstrapFailureException("Unknown store '" + storeName + "'.");

        if(logger.isDebugEnabled()) {
            logger.debug(cluster.toString(true));
            logger.debug(storeDef.toString());
        }
        boolean repairReads = !storeDef.isView();

        // construct mapping
        Map<Integer, Store<ByteArray, byte[], byte[]>> clientMapping = Maps.newHashMap();
        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();
        Map<Integer, NonblockingStore> nonblockingSlopStores = Maps.newHashMap();

        Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = null;
        if(storeDef.hasHintedHandoffStrategyType())
            slopStores = Maps.newHashMap();

        for(Node node: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> store = getStore(storeDef.getName(),
                                                              node.getHost(),
                                                              getPort(node),
                                                              this.requestFormatType);
            clientMapping.put(node.getId(), store);

            NonblockingStore nonblockingStore = routedStoreFactory.toNonblockingStore(store);
            nonblockingStores.put(node.getId(), nonblockingStore);

            if(slopStores != null) {
                Store<ByteArray, byte[], byte[]> rawSlopStore = getStore("slop",
                                                                         node.getHost(),
                                                                         getPort(node),
                                                                         this.requestFormatType);
                Store<ByteArray, Slop, byte[]> slopStore = SerializingStore.wrap(rawSlopStore,
                                                                                 slopKeySerializer,
                                                                                 slopValueSerializer,
                                                                                 new IdentitySerializer());
                slopStores.put(node.getId(), slopStore);
                nonblockingSlopStores.put(node.getId(),
                                          routedStoreFactory.toNonblockingStore(rawSlopStore));
            }
        }

        Store<ByteArray, byte[], byte[]> store = routedStoreFactory.create(cluster,
                                                                           storeDef,
                                                                           clientMapping,
                                                                           nonblockingStores,
                                                                           slopStores,
                                                                           nonblockingSlopStores,
                                                                           repairReads,
                                                                           clientZoneId,
                                                                           getFailureDetector(),
                                                                           isJmxEnabled);
        store = new LoggingStore(store);

        if(isJmxEnabled) {
            StatTrackingStore statStore = new StatTrackingStore(store, this.stats);
            store = statStore;
            JmxUtils.registerMbean(new StoreStatsJmx(statStore.getStats()),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                             clientContextName
                                                                     + "."
                                                                     + store.getName()
                                                                     + jmxId()
                                                                     + (null == clientId ? ""
                                                                                        : "."
                                                                                          + clientId.toString())));
        }

        if(storeDef.getKeySerializer().hasCompression()
           || storeDef.getValueSerializer().hasCompression()) {
            store = new CompressingStore(store,
                                         getCompressionStrategy(storeDef.getKeySerializer()),
                                         getCompressionStrategy(storeDef.getValueSerializer()));
        }

        Serializer<K> keySerializer = (Serializer<K>) serializerFactory.getSerializer(storeDef.getKeySerializer());
        Serializer<V> valueSerializer = (Serializer<V>) serializerFactory.getSerializer(storeDef.getValueSerializer());

        if(storeDef.isView() && (storeDef.getTransformsSerializer() == null))
            throw new SerializationException("Transforms serializer must be specified with a view ");

        Serializer<T> transformsSerializer = (Serializer<T>) serializerFactory.getSerializer(storeDef.getTransformsSerializer() != null ? storeDef.getTransformsSerializer()
                                                                                                                                       : new SerializerDefinition("identity"));

        Store<K, V, T> serializedStore = SerializingStore.wrap(store,
                                                               keySerializer,
                                                               valueSerializer,
                                                               transformsSerializer);

        // Add inconsistency resolving decorator, using their inconsistency
        // resolver (if they gave us one)
        InconsistencyResolver<Versioned<V>> secondaryResolver = resolver == null ? new TimeBasedInconsistencyResolver()
                                                                                : resolver;
        serializedStore = new InconsistencyResolvingStore<K, V, T>(serializedStore,
                                                                   new ChainedResolver<Versioned<V>>(new VectorClockInconsistencyResolver(),
                                                                                                     secondaryResolver));
        return serializedStore;
    }

    public <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                                InconsistencyResolver<Versioned<V>> resolver) {
        return getRawStore(storeName, resolver, null);
    }

    protected ClientConfig getConfig() {
        return config;
    }

    protected abstract FailureDetector initFailureDetector(final ClientConfig config,
                                                           final Collection<Node> nodes);

    public FailureDetector getFailureDetector() {
        // first check: avoids locking as the field is volatile
        FailureDetector result = failureDetector;
        if(result == null) {
            String clusterXml = bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY,
                                                             bootstrapUrls);
            Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml), false);
            synchronized(this) {
                // second check: avoids double initialization
                result = failureDetector;
                if(result == null)
                    failureDetector = result = initFailureDetector(config, cluster.getNodes());
            }
        }

        return result;
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

    public String bootstrapMetadataWithRetries(String key) {
        return bootstrapMetadataWithRetries(key, bootstrapUrls);
    }

    private String bootstrapMetadata(String key, URI[] urls) {
        for(URI url: urls) {
            try {
                List<Versioned<String>> found = getRemoteMetadata(key, url);
                if(found.size() == 1)
                    return found.get(0).getValue();
            } catch(Exception e) {
                logger.warn("Failed to bootstrap from " + url, e);
            }
        }
        throw new BootstrapFailureException("No available bootstrap servers found!");
    }

    protected List<Versioned<String>> getRemoteMetadata(String key, URI url) {
        Store<ByteArray, byte[], byte[]> remoteStore = getStore(MetadataStore.METADATA_STORE_NAME,
                                                                url.getHost(),
                                                                url.getPort(),
                                                                this.requestFormatType);
        Store<String, String, byte[]> store = SerializingStore.wrap(remoteStore,
                                                                    new StringSerializer("UTF-8"),
                                                                    new StringSerializer("UTF-8"),
                                                                    new IdentitySerializer());
        return store.get(key, null);
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

    protected abstract Store<ByteArray, byte[], byte[]> getStore(String storeName,
                                                                 String host,
                                                                 int port,
                                                                 RequestFormatType type);

    protected abstract int getPort(Node node);

    protected abstract void validateUrl(URI url);

    public SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    public RequestFormatType getRequestFormatType() {
        return requestFormatType;
    }

    public void close() {
        this.threadPool.shutdown();

        try {
            if(!this.threadPool.awaitTermination(10, TimeUnit.SECONDS))
                this.threadPool.shutdownNow();
        } catch(InterruptedException e) {
            // okay, fine, playing nice didn't work
            this.threadPool.shutdownNow();
        }

        if(failureDetector != null)
            failureDetector.destroy();
    }

    /* Give a unique id to avoid jmx clashes */
    private String jmxId() {
        return jmxId == 0 ? "" : "." + Integer.toString(jmxId);
    }

    /**
     * Generate a unique client ID based on: 0. clientContext, if specified; 1.
     * storeName 2. run path 3. client sequence
     * 
     * @param storeName the name of the store the client is created for
     * @param contextName the name of the client context
     * @param clientSequence the client sequence number
     * @return unique client ID
     */
    public static UUID generateClientId(String storeName, String contextName, int clientSequence) {
        String newLine = System.getProperty("line.separator");
        StringBuilder context = new StringBuilder(contextName == null ? "" : contextName);
        context.append(0 == clientSequence ? "" : ("." + clientSequence));
        context.append(".").append(storeName);

        try {
            InetAddress host = InetAddress.getLocalHost();
            context.append("@").append(host.getHostName()).append(":");
        } catch(UnknownHostException e) {
            logger.info("Unable to obtain client hostname.");
            logger.info(e.getMessage());
        }

        try {
            String currentPath = new File(".").getCanonicalPath();
            context.append(currentPath).append(newLine);
        } catch(IOException e) {
            logger.info("Unable to obtain client run path.");
            logger.info(e.getMessage());
        }

        if(logger.isDebugEnabled()) {
            logger.debug(context.toString());
        }

        return UUID.nameUUIDFromBytes(context.toString().getBytes());
    }
}

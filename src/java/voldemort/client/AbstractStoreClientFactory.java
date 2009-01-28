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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.StringSerializer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.logging.LoggingStore;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.routed.RoutedStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.versioned.InconsistencyResolvingStore;
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

    public static final int DEFAULT_ROUTING_TIMEOUT_MS = 5000;
    public static final int DEFAULT_NODE_BANNAGE_MS = 10000;

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
    private static final Logger logger = Logger.getLogger(AbstractStoreClientFactory.class);

    private final long routingTimeoutMs;
    private final long nodeBannageMs;
    private final ExecutorService threadPool;
    private final SerializerFactory serializerFactory;
    private final URI[] bootstrapUrls;
    private final boolean enableVerboseLogging = true;

    public AbstractStoreClientFactory(ExecutorService threadPool,
                                      SerializerFactory serializerFactory,
                                      int routingTimeoutMs,
                                      int nodeBannageMs,
                                      String... bootstrapUrls) {
        this.threadPool = threadPool;
        this.serializerFactory = serializerFactory;
        this.bootstrapUrls = validateUrls(bootstrapUrls);
        this.routingTimeoutMs = routingTimeoutMs;
        this.nodeBannageMs = nodeBannageMs;
    }

    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return getStoreClient(storeName, null);
    }

    @SuppressWarnings("unchecked")
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> inconsistencyResolver) {
        // Get cluster and store metadata
        String clusterXml = bootstrapMetadata(MetadataStore.CLUSTER_KEY, bootstrapUrls);
        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml));
        String storesXml = bootstrapMetadata(MetadataStore.STORES_KEY, bootstrapUrls);
        List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesXml));
        StoreDefinition storeDef = null;
        for(StoreDefinition d: storeDefs)
            if(d.getName().equals(storeName))
                storeDef = d;
        if(storeDef == null)
            throw new BootstrapFailureException("Unknown store '" + storeName + "'.");

        // create routing strategy
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                                        storeDef.getReplicationFactor());

        // construct mapping
        Map<Integer, Store<byte[], byte[]>> clientMapping = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            Store<byte[], byte[]> store = getStore(storeDef.getName(),
                                                   node.getHost(),
                                                   getPort(node));
            if(enableVerboseLogging)
                store = new LoggingStore(store);
            clientMapping.put(node.getId(), store);
        }

        Store<byte[], byte[]> store = new RoutedStore(storeName,
                                                      clientMapping,
                                                      routingStrategy,
                                                      storeDef.getPreferredReads() == null ? storeDef.getRequiredReads()
                                                                                          : storeDef.getPreferredReads(),
                                                      storeDef.getRequiredReads(),
                                                      storeDef.getPreferredWrites() == null ? storeDef.getRequiredWrites()
                                                                                           : storeDef.getPreferredWrites(),
                                                      storeDef.getRequiredWrites(),
                                                      true,
                                                      threadPool,
                                                      routingTimeoutMs,
                                                      nodeBannageMs,
                                                      SystemTime.INSTANCE);

        Serializer<K> keySerializer = (Serializer<K>) serializerFactory.getSerializer(storeDef.getKeySerializer());
        Serializer<V> valueSerializer = (Serializer<V>) serializerFactory.getSerializer(storeDef.getValueSerializer());
        Store<K, V> serializingStore = new SerializingStore<K, V>(store,
                                                                  keySerializer,
                                                                  valueSerializer);

        // Add inconsistency resolving decorator, using their inconsistency
        // resolver (if they gave us one)
        InconsistencyResolver<Versioned<V>> secondaryResolver = inconsistencyResolver == null ? new TimeBasedInconsistencyResolver()
                                                                                             : inconsistencyResolver;
        Store<K, V> resolvingStore = new InconsistencyResolvingStore<K, V>(serializingStore,
                                                                           new ChainedResolver<Versioned<V>>(new VectorClockInconsistencyResolver(),
                                                                                                             secondaryResolver));

        return new DefaultStoreClient<K, V>(resolvingStore,
                                            keySerializer,
                                            valueSerializer,
                                            routingStrategy);
    }

    private String bootstrapMetadata(String key, URI[] urls) {
        for(URI url: urls) {
            try {
                Store<byte[], byte[]> remoteStore = getStore(MetadataStore.METADATA_STORE_NAME,
                                                             url.getHost(),
                                                             url.getPort());
                Store<String, String> store = new SerializingStore<String, String>(remoteStore,
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

    private URI[] validateUrls(String[] urls) {
        URI[] uris = new URI[urls.length];
        if(urls == null || urls.length == 0)
            throw new IllegalArgumentException("Must provide at least one bootstrap URL!");

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
                throw new IllegalArgumentException("Illegal scheme in bootstrap URL, must specify a host.");
            else if(uri.getPort() < 0)
                throw new IllegalArgumentException("Must specify a port in bootstrap URL!");
            else
                validateUrl(uri);

            uris[i] = uri;
        }

        return uris;
    }

    protected abstract Store<byte[], byte[]> getStore(String storeName, String host, int port);

    protected abstract int getPort(Node node);

    protected abstract void validateUrl(URI url);

    protected ExecutorService getThreadPool() {
        return this.threadPool;
    }

}

package voldemort.restclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.rest.RestUtils;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.Store;
import voldemort.store.compress.CompressingStore;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.StoreClientFactoryStats;
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

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

/**
 * Factory used to create a REST client for performing Voldemort operations
 * 
 */
public class RESTClientFactory implements StoreClientFactory {

    private RESTClientConfig config = null;
    private final StoreStats stats;
    private Logger logger = Logger.getLogger(RESTClientFactory.class);
    private SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private HttpClientFactory _clientFactory;
    private final TransportClient transportClient;
    private final StoreClientFactoryStats RESTClientFactoryStats;
    
    /**
     * This list holds a reference to all the raw stores created by this
     * factory. When the application invokes 'close' on this factory, it invokes
     * close on all these stores in turn.
     */
    private List<R2Store> rawStoreList = null;

    public RESTClientFactory(RESTClientConfig config) {
        this.config = config;
        this.stats = new StoreStats();
        this.rawStoreList = new ArrayList<R2Store>();

        // Create the R2 (Netty) Factory object
        // TODO: Add monitoring for R2 factory
        this._clientFactory = new HttpClientFactory();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HttpClientFactory.POOL_SIZE_KEY,
                       Integer.toString(this.config.getMaxR2ConnectionPoolSize()));
        transportClient = _clientFactory.getClient(properties);
        this.RESTClientFactoryStats = new StoreClientFactoryStats();

    }

    /**
     * Creates a REST client used to perform Voldemort operations against the
     * Coordinator
     * 
     * @param storeName Name of the store to perform the operations on
     * @return
     */
    @Override
    public <K, V> StoreClient<K, V> getStoreClient(String storeName) {
        return getStoreClient(storeName, null);
    }

    /**
     * Creates a REST client used to perform Voldemort operations against the
     * Coordinator
     * 
     * @param storeName Name of the store to perform the operations on
     * @param resolver Custom resolver as specified by the application
     * @return
     */
    @Override
    public <K, V> StoreClient<K, V> getStoreClient(String storeName,
                                                   InconsistencyResolver<Versioned<V>> resolver) {
        Store<K, V, Object> clientStore = getRawStore(storeName, resolver);
        return new RESTClient<K, V>(storeName, clientStore);

    }

    @Override
    public <K, V, T> Store<K, V, T> getRawStore(String storeName,
                                                InconsistencyResolver<Versioned<V>> resolver) {

        Store<K, V, T> clientStore = null;

        // The lowest layer : Transporting request to coordinator
        R2Store r2store = new R2Store(storeName,
                                      this.config.getHttpBootstrapURL(),
                                      this.transportClient,
                                      this.config);
        this.rawStoreList.add(r2store);

        // bootstrap from the coordinator and obtain all the serialization
        // information.
        String serializerInfoXml = r2store.getSerializerInfoXml();
        SerializerDefinition keySerializerDefinition = RestUtils.parseKeySerializerDefinition(serializerInfoXml);
        SerializerDefinition valueSerializerDefinition = RestUtils.parseValueSerializerDefinition(serializerInfoXml);

        if(logger.isDebugEnabled()) {
            logger.debug("Bootstrapping for " + storeName + ": Key serializer "
                         + keySerializerDefinition);
            logger.debug("Bootstrapping for " + storeName + ": Value serializer "
                         + valueSerializerDefinition);
        }

        // Start building the stack..
        // First, the transport layer
        Store<ByteArray, byte[], byte[]> store = r2store;

        // TODO: Add jmxId / some unique identifier to the Mbean name
        if(this.config.isEnableJmx()) {
            StatTrackingStore statStore = new StatTrackingStore(store, this.stats);
            store = statStore;
            JmxUtils.registerMbean(new StoreStatsJmx(statStore.getStats()),
                                   JmxUtils.createObjectName(JmxUtils.getPackageName(store.getClass()),
                                                             store.getName()));
        }

        // Add compression layer
        if(keySerializerDefinition.hasCompression() || valueSerializerDefinition.hasCompression()) {
            store = new CompressingStore(store,
                                         new CompressionStrategyFactory().get(keySerializerDefinition.getCompression()),
                                         new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression()));
        }

        // Add Serialization layer
        Serializer<K> keySerializer = (Serializer<K>) serializerFactory.getSerializer(keySerializerDefinition);
        Serializer<V> valueSerializer = (Serializer<V>) serializerFactory.getSerializer(valueSerializerDefinition);
        clientStore = SerializingStore.wrap(store, keySerializer, valueSerializer, null);

        // Add inconsistency Resolving layer
        InconsistencyResolver<Versioned<V>> secondaryResolver = resolver == null ? new TimeBasedInconsistencyResolver<V>()
                                                                                : resolver;
        clientStore = new InconsistencyResolvingStore<K, V, T>(clientStore,
                                                               new ChainedResolver<Versioned<V>>(new VectorClockInconsistencyResolver<V>(),
                                                                                                 secondaryResolver));
        return clientStore;
    }

    @Override
    public void close() {
        for(R2Store store: this.rawStoreList) {
            store.close();
        }

        final FutureCallback<None> factoryShutdownCallback = new FutureCallback<None>();
        this._clientFactory.shutdown(factoryShutdownCallback);
        try {
            factoryShutdownCallback.get();
        } catch(InterruptedException e) {
            logger.error("Interrupted while shutting down the HttpClientFactory: " + e.getMessage(),
                         e);
        } catch(ExecutionException e) {
            logger.error("Execution exception occurred while shutting down the HttpClientFactory: "
                         + e.getMessage(), e);
        }
    }

    @Override
    public FailureDetector getFailureDetector() {
        return null;
    }

    @Override
    public StoreClientFactoryStats getStoreClientFactoryStats() {
        return RESTClientFactoryStats;
    }

}

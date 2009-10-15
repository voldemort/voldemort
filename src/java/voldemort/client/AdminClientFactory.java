package voldemort.client;

import org.apache.log4j.Logger;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClientRequestFormat;
import voldemort.client.protocol.admin.NativeAdminClientRequestFormat;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.serialization.StringSerializer;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: afeinber
 * Date: Oct 14, 2009
 * Time: 1:12:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class AdminClientFactory {
    private final SocketPool socketPool;
    private final RoutingTier routingTier;
    private final Logger logger = Logger.getLogger(AdminClientFactory.class);
    private final ClientConfig config;
    private final URI bootstrapUris[];

    public AdminClientFactory(ClientConfig config) {
        this.config = config;
        this.routingTier = config.getRoutingTier();
        this.socketPool = new SocketPool(config.getMaxConnectionsPerNode(),
            config.getConnectionTimeout(TimeUnit.MILLISECONDS),
            config.getSocketTimeout(TimeUnit.MILLISECONDS),
            config.getSocketBufferSize());
        this.bootstrapUris = validateUrls(config.getBootstrapUrls());
    }

    private static URI[] validateUrls(String[] urls) {
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

    private static void validateUrl(URI uri) {
        if (!"tcp".equals(uri.getScheme()))
            throw new IllegalArgumentException("Illegal scheme in bootstrap URL for SocketStoreClientFactory:"
                                               + " expected 'tcp' "
                                               + "but found '"
                                               + uri.getScheme() + "'.");


    }

    private String bootstrapMetadata(String key, URI[] urls) {
        for(URI url: urls) {
            try {
                Store<ByteArray, byte[]> remoteStore = getStore(MetadataStore.METADATA_STORE_NAME,
                                                                url.getHost(),
                                                                url.getPort(),
                                                                config.getRequestFormatType());
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

    protected Store<ByteArray, byte[]> getStore(String storeName,
                                                String host,
                                                int port,
                                                RequestFormatType type) {
        return new SocketStore(Utils.notNull(storeName),
                               new SocketDestination(Utils.notNull(host), port, type),
                               socketPool,
                               RoutingTier.SERVER.equals(routingTier));
    }

    public AdminClientRequestFormat getAdminClient() {
        return getAdminClient(false);
    }

    public AdminClientRequestFormat getAdminClient(boolean useNative) {
        String clusterXml = bootstrapMetadata(MetadataStore.CLUSTER_KEY, bootstrapUris);
        String storesXml = bootstrapMetadata(MetadataStore.STORES_KEY, bootstrapUris);
        StorageEngine<String,String> backingStore = new InMemoryStorageEngine<String, String>
            ("metadata");
        backingStore.put(MetadataStore.CLUSTER_KEY, new Versioned<String>(clusterXml));
        backingStore.put(MetadataStore.STORES_KEY, new Versioned<String>(storesXml));
        MetadataStore metadata = new MetadataStore(backingStore, 0);

        if (useNative)
            return new NativeAdminClientRequestFormat(metadata, socketPool);

        return new ProtoBuffAdminClientRequestFormat(metadata, socketPool);
    }

    public AdminClientRequestFormat getAdminclient(MetadataStore metadata) {
        return getAdminClient(metadata, false);
        
    }
    public AdminClientRequestFormat getAdminClient(MetadataStore metadata, boolean useNative) {
        if (useNative)
            return new NativeAdminClientRequestFormat(metadata, socketPool);
        
        return new ProtoBuffAdminClientRequestFormat(metadata, socketPool);
    }
}

package voldemort.client;

import java.net.URI;
import java.util.concurrent.Executors;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;

/**
 * @author jay
 * 
 */
public class HttpStoreClientFactory extends AbstractStoreClientFactory {

    public static String URL_SCHEME = "http";

    private static final int DEFAULT_CONNECTION_TIMEOUT = 5000;
    private static final int DEFAULT_SO_TIMEOUT = 5000;
    private static final int DEFAULT_CONNECTION_MANAGER_TIMEOUT = 5000;
    private static final int DEFAULT_MAX_CONNECTIONS = 40;
    private static final int DEFAULT_MAX_HOST_CONNECTIONS = 40;
    private static final int DEFAULT_NUM_RETRIES = 0;
    private static final String VOLDEMORT_USER_AGENT = "vldmrt/0.01";

    private final HttpClient httpClient;
    private final MultiThreadedHttpConnectionManager connectionManager;

    public HttpStoreClientFactory(int numThreads, String... bootstrapUrls) {
        this(numThreads,
             DEFAULT_CONNECTION_MANAGER_TIMEOUT,
             DEFAULT_SO_TIMEOUT,
             DEFAULT_NUM_RETRIES,
             DEFAULT_CONNECTION_TIMEOUT,
             DEFAULT_ROUTING_TIMEOUT_MS,
             AbstractStoreClientFactory.DEFAULT_NODE_BANNAGE_MS,
             DEFAULT_MAX_CONNECTIONS,
             DEFAULT_MAX_HOST_CONNECTIONS,
             bootstrapUrls);
    }

    public HttpStoreClientFactory(int numThreads,
                                  int connectionManagerTimeoutMs,
                                  int socketSoTimeoutMs,
                                  int numRetries,
                                  int connectionTimeoutMs,
                                  int routingTimeoutMs,
                                  int nodeBannageMs,
                                  int maxConnections,
                                  int maxSingleHostConnections,
                                  String... bootstrapUrls) {
        this(numThreads,
             connectionManagerTimeoutMs,
             socketSoTimeoutMs,
             numRetries,
             connectionTimeoutMs,
             routingTimeoutMs,
             nodeBannageMs,
             maxConnections,
             maxSingleHostConnections,
             new DefaultSerializerFactory(),
             bootstrapUrls);
    }

    public HttpStoreClientFactory(int numThreads,
                                  int connectionManagerTimeoutMs,
                                  int socketSoTimeoutMs,
                                  int numRetries,
                                  int connectionTimeoutMs,
                                  int routingTimeoutMs,
                                  int nodeBannageMs,
                                  int maxConnections,
                                  int maxSingleHostConnections,
                                  SerializerFactory serializerFactory,
                                  String... bootstrapUrls) {
        // TODO: Customize this threadpool??
        super(Executors.newFixedThreadPool(numThreads),
              serializerFactory,
              routingTimeoutMs,
              nodeBannageMs,
              bootstrapUrls);
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
        this.connectionManager = new MultiThreadedHttpConnectionManager();
        this.httpClient = new HttpClient(connectionManager);
        this.httpClient.setHostConfiguration(hostConfig);
        HttpClientParams clientParams = this.httpClient.getParams();
        clientParams.setConnectionManagerTimeout(connectionManagerTimeoutMs);
        clientParams.setSoTimeout(socketSoTimeoutMs);
        clientParams.setParameter(HttpClientParams.RETRY_HANDLER,
                                  new DefaultHttpMethodRetryHandler(numRetries, false));
        clientParams.setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        clientParams.setParameter("http.useragent", VOLDEMORT_USER_AGENT);
        HttpConnectionManagerParams managerParams = this.httpClient.getHttpConnectionManager()
                                                                   .getParams();
        managerParams.setConnectionTimeout(connectionTimeoutMs);
        managerParams.setMaxTotalConnections(maxConnections);
        managerParams.setStaleCheckingEnabled(false);
        managerParams.setMaxConnectionsPerHost(httpClient.getHostConfiguration(),
                                               maxSingleHostConnections);
    }

    @Override
    protected Store<byte[], byte[]> getStore(String name, String host, int port) {
        return new HttpStore(name, host, port, httpClient);
    }

    @Override
    protected int getPort(Node node) {
        return node.getHttpPort();
    }

    @Override
    protected void validateUrl(URI url) {
        if (!URL_SCHEME.equals(url.getScheme()))
            throw new IllegalArgumentException("Illegal scheme in bootstrap URL for HttpStoreClientFactory:"
                    + " expected '" + URL_SCHEME + "' but found '" + url.getScheme() + "'.");
    }

}

package voldemort.client.protocol.admin;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import voldemort.client.ClientConfig;


/**
 * AdminClientPool utility class for caching AdminClient.
 * Checkout is non-blocking and if AdminClient does not exist in the cache
 * a new AdminClient will be created and returned.
 * CheckIn is non-blocking as well, when max number of clients are already
 * cached, further CheckIn of AdminClient will be closed and discarded.
 */
public class AdminClientPool {
    private final AdminClientConfig adminConfig;
    private final ClientConfig clientConfig;
    private final BlockingQueue<AdminClient> clientCache;
    private final AtomicBoolean isClosed;

    /**
     * Create AdminClientPool
     * @param maxClients number of clients to cache, due to concurrent checkout,
     *      more number of AdminClient may be created. But on CheckIn, additional
     *      ones are discarded.
     * @param adminConfig AdminClientConfig used for creation of AdminClient
     * @param clientConfig ClientConfig used for creation of AdminClient.
     */

    public AdminClientPool(int maxClients, AdminClientConfig adminConfig, ClientConfig clientConfig) {
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients should be positive");
        }
        
        if (adminConfig == null) {
            throw new IllegalArgumentException("AdminClientConfig is null");
        }

        if (clientConfig == null) {
            throw new IllegalArgumentException("ClientConfig is null");
        }

        String[] bootstrapUrls = clientConfig.getBootstrapUrls();
        if (bootstrapUrls == null || bootstrapUrls.length == 0) {
            throw new IllegalArgumentException("ClientConfig has no bootstrap Urls specified");
        }

        this.adminConfig = adminConfig;
        this.clientConfig = clientConfig;
        this.clientCache = new ArrayBlockingQueue<AdminClient>(maxClients);
        this.isClosed = new AtomicBoolean(false);
    }

    private AdminClient createAdminClient() {
        return new AdminClient(adminConfig, clientConfig);
    }

    /**
     * number of AdminClient in the cache
     * @return number of AdminClient in the cache.
     */
    public int size() {
        if (isClosed.get()) {
            throw new IllegalStateException("Pool is closing");
        }

        return clientCache.size();
    }

    /**
     * get an AdminClient from the cache if exists, if not create new one
     * and return it. This method is non-blocking.
     *
     * All AdminClient returned from checkout, once after the completion of
     * usage must be returned to the pool by calling checkin. If not,
     * there will be leak of AdminClients (connections, threads and file handles).
     *
     * @return AdminClient
     */
    public AdminClient checkout() {
        if (isClosed.get()) {
            throw new IllegalStateException("Pool is closing");
        }

        AdminClient client;

        // Try to get one from the Cache.
        while ((client = clientCache.poll()) != null) {
            if (!client.isClusterModified()) {
                return client;
            } else {
                // Cluster is Modified, after the AdminClient is created. Close it
                client.close();
            }
        }

        // None is available, create new one.
        return createAdminClient();
    }
    
    /**
     * submit the adminClient after usage is completed.
     * Behavior is undefined, if checkin is called with objects not retrieved
     * from checkout.
     *
     * @param client AdminClient retrieved from checkout
     */
    public void checkin(AdminClient client) {
        if (isClosed.get()) {
            throw new IllegalStateException("Pool is closing");
        }

        if (client == null) {
            throw new IllegalArgumentException("client is null");
        }

        boolean isCheckedIn = clientCache.offer(client);

        if (!isCheckedIn) {
            // Cache is already full, close this AdminClient
            client.close();
        }
    }

    /**
     * close the AdminPool, if no long required.
     * After closed, all public methods will throw IllegalStateException
     */
    public void close() {
        boolean isPreviouslyClosed = isClosed.getAndSet(true);
        if (isPreviouslyClosed) {
            return;
        }

        AdminClient client;
        while ((client = clientCache.poll()) != null) {
            client.close();
        }
    }
}

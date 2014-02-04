package voldemort.server.quota;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortApplicationException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.quota.QuotaType;

/**
 * Does basic tests on the rate limiting implementation. More real performance
 * tests (i.e testing the ratelimiting works upto a certain load limit and keeps
 * up) needs to happen else where in an integration environment. This is
 * because, no single rate would work on all test machines and thus would result
 * in this test become flaky.
 * 
 */
public class QuotaLimitingStoreTest {

    private VoldemortServer server;
    private AdminClient adminClient;
    private SocketStoreClientFactory factory;
    private StoreClient<Object, Object> storeClient;

    @Before
    public void setup() throws IOException {

        Properties props = new Properties();
        props.put("enable.quota.limiting", "true");
        server = ServerTestUtils.startStandAloneVoldemortServer(props,
                                                                "test/common/voldemort/config/single-store.xml");

        adminClient = new AdminClient(server.getMetadataStore().getCluster(),
                                      new AdminClientConfig(),
                                      new ClientConfig());
        String bootStrapUrl = "tcp://" + server.getIdentityNode().getHost() + ":"
                              + server.getIdentityNode().getSocketPort();
        factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootStrapUrl));
        storeClient = factory.getStoreClient("test");
    }

    private void ensureThrottled() {
        int numGetExceptions = 0;
        int numPutExceptions = 0;
        for(int i = 0; i < 1000; i++) {
            try {
                // do a put
                storeClient.put("key", "value");
            } catch(VoldemortApplicationException vae) {
                numPutExceptions++;
            }

            try {
                // do a get
                storeClient.get("key");
            } catch(VoldemortApplicationException vae) {
                numGetExceptions++;
            }

        }
        assertTrue("No get operations rate limited", numGetExceptions > 0);
        assertTrue("No put operations rate limited", numPutExceptions > 0);
    }

    private void ensureNotThrottled() {
        for(int i = 0; i < 1000; i++) {
            try {
                // do a put
                storeClient.put("key", "value");
            } catch(VoldemortApplicationException vae) {
                fail("Put throttled when rate is :" + Integer.MAX_VALUE);
            }

            try {
                // do a get
                storeClient.get("key");
            } catch(VoldemortApplicationException vae) {
                fail("Get throttled when rate is :" + Integer.MAX_VALUE);
            }
        }
    }

    @Test
    public void testRateLimiting() {
        // limit gets to 2, puts to 2 to force ratelimits to exceed (with very
        // high probability)
        adminClient.quotaMgmtOps.setQuota("test", QuotaType.GET_THROUGHPUT.toString(), "2");
        adminClient.quotaMgmtOps.setQuota("test", QuotaType.PUT_THROUGHPUT.toString(), "2");

        ensureThrottled();

        // set the quotas to a very high value such that no operations are
        // throttled
        adminClient.quotaMgmtOps.setQuota("test",
                                          QuotaType.GET_THROUGHPUT.toString(),
                                          Integer.MAX_VALUE + "");
        adminClient.quotaMgmtOps.setQuota("test",
                                          QuotaType.PUT_THROUGHPUT.toString(),
                                          Integer.MAX_VALUE + "");
        ensureNotThrottled();

        // Throttle back again
        adminClient.quotaMgmtOps.setQuota("test", QuotaType.GET_THROUGHPUT.toString(), "2");
        adminClient.quotaMgmtOps.setQuota("test", QuotaType.PUT_THROUGHPUT.toString(), "2");
        ensureThrottled();

        // Unset limits and make sure not throttled
        adminClient.quotaMgmtOps.unsetQuota("test", QuotaType.GET_THROUGHPUT.toString());
        adminClient.quotaMgmtOps.unsetQuota("test", QuotaType.PUT_THROUGHPUT.toString());
        ensureNotThrottled();
    }

    @After
    public void teardown() {
        adminClient.close();
        server.stop();

        factory.close();
    }
}

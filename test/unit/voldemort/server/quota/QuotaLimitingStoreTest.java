package voldemort.server.quota;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.configuration.FileBackedCachingStorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.quota.QuotaLimitStats;
import voldemort.store.quota.QuotaLimitingStore;
import voldemort.store.quota.QuotaType;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.stats.StatTrackingStore;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

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
        verifyThrottling(true);
    }

    private void ensureNotThrottled() {
        verifyThrottling(false);
    }

    private void verifyThrottling(boolean isThrottled) {
        int numGetExceptions = 0;
        int numPutExceptions = 0;

        Map<String, String> populatedValues = new HashMap<String, String>();

        for(int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String value = "value" + i;
            try {
                // do a put
                storeClient.put(key, value);
                populatedValues.put(key, value);
            } catch(QuotaExceededException qee) {
                numPutExceptions++;
            }
        }

        if(isThrottled) {
            assertTrue("No put operations rate limited", numPutExceptions > 0);
        } else {
            assertTrue("Put throttled when rate is :" + Integer.MAX_VALUE, numPutExceptions == 0);
        }

        for(int i = 0; i < 1000; i++) {
            String key = "key" + i;
            String expectedValue = "value" + i;

            try {
                // do a get

                Versioned<Object> actualValue = storeClient.get(key);
                if(actualValue == null) {
                    if(populatedValues.containsKey(key) == false) {
                        // expected, continue
                        continue;
                    } else {
                        fail("Put successfully wrote the key, but get was not able to retrieve it, nor it got a quota exception");
                    }
                }

                assertEquals(expectedValue, actualValue.getValue());
            } catch(QuotaExceededException qee) {
                numGetExceptions++;
            }
        }

        if(isThrottled) {
            assertTrue("No get operations rate limited", numGetExceptions > 0);
        } else {
            assertTrue("Get throttled when rate is :" + Integer.MAX_VALUE, numGetExceptions == 0);
        }

    }

    private void setQuota(int throughPut) {
        String throughPutStr = Integer.toString(throughPut);
        adminClient.quotaMgmtOps.setQuota("test",
                                          QuotaType.GET_THROUGHPUT.toString(),
                                          throughPutStr);
        adminClient.quotaMgmtOps.setQuota("test",
                                          QuotaType.PUT_THROUGHPUT.toString(),
                                          throughPutStr);
    }

    private void unSetQuota() {
        adminClient.quotaMgmtOps.unsetQuota("test", QuotaType.GET_THROUGHPUT.toString());
        adminClient.quotaMgmtOps.unsetQuota("test", QuotaType.PUT_THROUGHPUT.toString());
    }

    private void enableQuotaEnforcing() {
        adminClient.metadataMgmtOps.updateRemoteMetadata(Arrays.asList(0),
                                                         MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                         Boolean.toString(true));
    }

    private void disableQuotaEnforcing() {
        adminClient.metadataMgmtOps.updateRemoteMetadata(Arrays.asList(0),
                                                         MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                         Boolean.toString(false));
    }

    @Test
    public void testRateLimiting() {
        // limit gets to 2, puts to 2 to force ratelimits to exceed (with very
        // high probability)
        setQuota(2);
        ensureThrottled();

        // set the quotas to a very high value such that no operations are
        // throttled
        setQuota(Integer.MAX_VALUE);
        ensureNotThrottled();

        // Throttle back again
        setQuota(2);
        ensureThrottled();

        // test quota enforcing metakey
        disableQuotaEnforcing();
        ensureNotThrottled();
        enableQuotaEnforcing();
        ensureThrottled();

        // Unset limits and make sure not throttled
        unSetQuota();
        ensureNotThrottled();
    }

    @Test
    /**
     *  PS: Test will fail if for some reason we cannot do 50 ops/sec against a hash map. So yeah, pretty unlikely.
     */
    public void testQuotaPctUsageCalculation() throws Exception {
        File tempDir = TestUtils.createTempDir();

        FileBackedCachingStorageEngine quotaStore = new FileBackedCachingStorageEngine("quota-usage-test-store",
                                                                                       tempDir.getAbsolutePath());
        InMemoryStorageEngine<ByteArray, byte[], byte[]> inMemoryEngine = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("inMemoryBackingStore");
        QuotaLimitStats quotaStats = new QuotaLimitStats(null, 1000);
        StatTrackingStore statTrackingStore = new StatTrackingStore(inMemoryEngine, null);

        QuotaLimitingStore quotaLimitingStore = new QuotaLimitingStore(statTrackingStore,
                                                                       statTrackingStore.getStats(),
                                                                       quotaStats,
                                                                       quotaStore,
                                                                       server.getMetadataStore());

        int targetRate = 50;
        // provide a quota of 100 gets/sec
        quotaStore.put(new ByteArray(QuotaUtils.makeQuotaKey(statTrackingStore.getName(),
                                                             QuotaType.GET_THROUGHPUT).getBytes()),
                       new Versioned<byte[]>("100.0".getBytes()),
                       null);

        long testIntervalMs = 5000;
        long timeToSleepMs = 1000 / targetRate;
        long startMs = System.currentTimeMillis();
        ByteArray key = new ByteArray("some key".getBytes());
        while((System.currentTimeMillis() - startMs) <= testIntervalMs) {
            quotaLimitingStore.get(key, null);
            Thread.sleep(timeToSleepMs);
        }

        assertEquals("No get operations should be throttled", 0, quotaStats.getRateLimitedGets());
        assertEquals("Put usage should be 0", 0, quotaStats.getQuotaPctUsedPut());
        assertEquals("delete usage should be 0", 0, quotaStats.getQuotaPctUsedDelete());
        assertEquals("getall usage should be 0", 0, quotaStats.getQuotaPctUsedGetAll());

        assertEquals("Computed usage pct must be close to actual observed qps",
                     statTrackingStore.getStats().getThroughput(Tracked.GET),
                     quotaStats.getQuotaPctUsedGet(),
                     1.0);

    }

    @After
    public void teardown() {
        adminClient.close();
        server.stop();

        factory.close();
    }
}

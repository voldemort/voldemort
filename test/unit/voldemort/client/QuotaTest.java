package voldemort.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.quota.QuotaType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Versioned;

// There are lots of tests for Quota, but not all of them are in one 
// Central Place, adding one more class for Quota Test
public class QuotaTest {
    private final Cluster cluster;
    private final List<StoreDefinition> stores;
    private final AdminClient adminClient;

    private final VoldemortServer[] servers;
    private final SocketStoreFactory ssf;

    public QuotaTest() throws IOException {
        // setup cluster
        cluster = ServerTestUtils.getLocalCluster(4);
        stores = ServerTestUtils.getStoreDefs(10);

        ssf = new ClientRequestExecutorPool(2, 10000, 100000, 1024);
        servers = new VoldemortServer[cluster.getNumberOfNodes()];

        for (Node node : cluster.getNodes()) {
            VoldemortConfig config =
                    ServerTestUtils.createServerConfigWithDefs(true, node.getId(), TestUtils.createTempDir()
                            .getAbsolutePath(), cluster, stores, new Properties());
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
            servers[node.getId()] = vs;
        }
        adminClient = new AdminClient(cluster);
    }

    @After
    public void tearDown() throws Exception {
        adminClient.close();
        for (VoldemortServer server : servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        ssf.close();
    }

    private void verifyNoQuota(String storeName, QuotaType quotaType) {
        Versioned<String> currentQuota = adminClient.quotaMgmtOps.getQuota(storeName, quotaType);
        Assert.assertNull("no quota expected for store" + storeName + " type " + quotaType, currentQuota);

        for (Integer nodeId : adminClient.getAdminClientCluster().getNodeIds()) {
            currentQuota = adminClient.quotaMgmtOps.getQuotaForNode(storeName, quotaType, nodeId);
            Assert.assertNull("no quota expected for store" + storeName + " type " + quotaType + " node " + nodeId,
                    currentQuota);
        }
    }

    private Long convertToLong(Versioned<String> versioned) {
        String quota = versioned.getValue();
        return Long.parseLong(quota);
    }

    private void verifyValueMatches(String desc, int expected, Versioned<String> retrieved) {
        Assert.assertNotNull("Quota expected for " + desc, retrieved);

        Long actual = convertToLong(retrieved);
        Assert.assertEquals("Quota does not match" + desc, new Long(expected), actual);
    }

    private void verifyValidQuota(String storeName, QuotaType quotaType, int expected) {
        String description = "Store " + storeName + " type " + quotaType;
        Versioned<String> currentQuota = adminClient.quotaMgmtOps.getQuota(storeName, quotaType);
        verifyValueMatches(description, expected, currentQuota);

        Long actual = convertToLong(currentQuota);
        Assert.assertEquals("Quota does not match", new Long(expected), actual);

        for (Integer nodeId : adminClient.getAdminClientCluster().getNodeIds()) {
            currentQuota = adminClient.quotaMgmtOps.getQuotaForNode(storeName, quotaType, nodeId);
            verifyValueMatches(description + " node " + nodeId, expected, currentQuota);
        }
    }
    
    private void verifyMultipleQuotas(List<String> input, Map<String, Versioned<String>> retrieved,
            Map<String, Integer> storeQuota, String description) {

        Assert.assertEquals("Less number of values returned" + description, input.size(), retrieved.size());


        for (String storeName : input) {
            String message = description + " store " + storeName;
            if (storeQuota.containsKey(storeName)) {
                verifyValueMatches(message, storeQuota.get(storeName), retrieved.get(storeName));
            } else {
                Assert.assertNull(" Null quota expected" + message, retrieved.get(storeName));
            }
        }
    }

    @Test
    public void testBasicOperation() {
        String storeName = stores.get(0).getName();
        for (QuotaType quotaType : QuotaType.values()) {
            verifyNoQuota(storeName, quotaType);

            // Create
            int newQuota = new Random().nextInt(Integer.MAX_VALUE);
            adminClient.quotaMgmtOps.setQuota(storeName, quotaType, newQuota);
            verifyValidQuota(storeName, quotaType, newQuota);

            // Update
            newQuota = new Random().nextInt(Integer.MAX_VALUE);
            adminClient.quotaMgmtOps.setQuota(storeName, quotaType, newQuota);
            verifyValidQuota(storeName, quotaType, newQuota);

            // Delete
            adminClient.quotaMgmtOps.unsetQuota(storeName, quotaType);
            verifyNoQuota(storeName, quotaType);
        }
    }
    
    private void verifyGetAll(List<String> storeNames, Map<String, Integer> storeQuotas, QuotaType quotaType) {
        Map<String, Versioned<String>> retrieved = adminClient.quotaMgmtOps.getQuota(storeNames, quotaType);
        verifyMultipleQuotas(storeNames, retrieved, storeQuotas, "QuotaType " + quotaType);

        for (Integer nodeId : adminClient.getAdminClientCluster().getNodeIds()) {
            retrieved = adminClient.quotaMgmtOps.getQuotaForNode(storeNames, quotaType, nodeId);
            verifyMultipleQuotas(storeNames, retrieved, storeQuotas, "QuotaType " + quotaType + " node " + nodeId);
        }

    }

    @Test
    public void testGetAllQuota() {
        for (QuotaType quotaType : QuotaType.values()) {
            List<String> existingStoreNames = new ArrayList<String>();
            Map<String, Integer> storeQuotas = new HashMap<String, Integer>();

            for (StoreDefinition store : stores) {
                String storeName = store.getName();
                existingStoreNames.add(storeName);

                int quota = new Random().nextInt();
                storeQuotas.put(storeName, quota);
                adminClient.quotaMgmtOps.setQuota(storeName, quotaType, quota);
            }

            // All stores in some order
            Collections.shuffle(existingStoreNames);
            verifyGetAll(existingStoreNames, storeQuotas, quotaType);

            // Pick subset of stores.
            final int REQUIRED_ELEMENTS = 3;
            int numElements =
                    REQUIRED_ELEMENTS + new Random().nextInt(existingStoreNames.size() - REQUIRED_ELEMENTS);

            List<String> fewStores = existingStoreNames.subList(0, numElements);
            verifyGetAll(fewStores, storeQuotas, quotaType);

            // Pick few existing stores and few non-existent stores
            List<String> mixedStores = new ArrayList<String>();
            mixedStores.addAll(fewStores);
            for (int i = 0; i < 10; i++) {
                mixedStores.add("NON_EXISTENT_STORE" + i);
            }
            verifyGetAll(mixedStores, storeQuotas, quotaType);
        }
    }

}

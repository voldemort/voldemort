package voldemort.store.routed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

@RunWith(Parameterized.class)
public class ZoneAffinityGetAllTest {

    private Store<String, String, byte[]> client;
    private Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    private Cluster cluster;
    private final Integer clientZoneId;

    public ZoneAffinityGetAllTest(Integer clientZoneId) {
        this.clientZoneId = clientZoneId;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 2 } });
    }

    @Before
    public void setup() throws IOException {
        byte[] key1 = { (byte) 'K', (byte) '1' }; // good
        byte[] key2 = { (byte) 'K', (byte) '2' }; // stale in local zone
        byte[] key3 = { (byte) 'K', (byte) '3' }; // null
        byte[] bytes1 = { (byte) 'A', (byte) 'B' };
        byte[] bytes2 = { (byte) 'C', (byte) 'D' };
        List<StoreDefinition> stores = ClusterTestUtils.getZZZ322StoreDefs("memory");
        StoreDefinition storeDef = stores.get(0);
        cluster = ClusterTestUtils.getZZZCluster();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(cluster.getNodeById(0).getSocketUrl().toString());
        clientConfig.getZoneAffinity().setEnableGetAllOpZoneAffinity(true);
        clientConfig.setClientZoneId(clientZoneId);
        SocketStoreClientFactory socketStoreClientFactory = new SocketStoreClientFactory(clientConfig);
        for(Integer nodeId: cluster.getNodeIds()) {
            SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  1024);
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                                nodeId,
                                                                                TestUtils.createTempDir()
                                                                                         .getAbsolutePath(),
                                                                                cluster,
                                                                                stores,
                                                                                new Properties());
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                      config,
                                                                      cluster);
            vservers.put(nodeId, vs);
            Store<ByteArray, byte[], byte[]> store = vs.getStoreRepository()
                                                       .getLocalStore(storeDef.getName());
            Node node = cluster.getNodeById(nodeId);

            VectorClock version1 = new VectorClock();
            version1.incrementVersion(0, System.currentTimeMillis());
            VectorClock version2 = version1.incremented(0, System.currentTimeMillis());

            store.put(new ByteArray(key1), new Versioned<byte[]>(bytes1, version1), null);
            if(node.getZoneId() == clientZoneId) {
                store.put(new ByteArray(key2), new Versioned<byte[]>(bytes1, version1), null);
            } else {
                store.put(new ByteArray(key2), new Versioned<byte[]>(bytes2, version2), null);
            }
        }

        client = socketStoreClientFactory.getRawStore(storeDef.getName(), null);
    }

    @After
    public void tearDown() {
        for(VoldemortServer vs: this.vservers.values()) {
            vs.stop();
        }
    }

    @Test
    public void testAllUp() {
        try {
            ArrayList<String> keys = new ArrayList<String>();
            keys.add("K1");
            keys.add("K2");
            keys.add("K3");
            Map<String, List<Versioned<String>>> versioneds = client.getAll(keys, null);
            assertEquals("AB", versioneds.get("K1").get(0).getValue());
            assertEquals("AB", versioneds.get("K2").get(0).getValue());
            assertEquals(null, versioneds.get("K3"));
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testLocalZoneDown() {
        for(Integer nodeId: cluster.getNodeIdsInZone(clientZoneId)) {
            this.vservers.get(nodeId).stop();
        }
        try {
            ArrayList<String> keys = new ArrayList<String>();
            keys.add("K1");
            keys.add("K2");
            keys.add("K3");
            Map<String, List<Versioned<String>>> versioneds = client.getAll(keys, null);
            assertEquals(null, versioneds.get("K1"));
            assertEquals(null, versioneds.get("K2"));
            assertEquals(null, versioneds.get("K3"));
        } catch(InsufficientOperationalNodesException e) {

        }
    }

    @Test
    public void testLocalZonePartialDownSufficientReads() {
        // turn off one node in same zone as client so that reads can still
        // complete
        this.vservers.get(cluster.getNodeIdsInZone(clientZoneId).iterator().next()).stop();
        try {
            ArrayList<String> keys = new ArrayList<String>();
            keys.add("K1");
            keys.add("K2");
            keys.add("K3");
            Map<String, List<Versioned<String>>> versioneds = client.getAll(keys, null);
            assertEquals("AB", versioneds.get("K1").get(0).getValue());
            assertEquals("AB", versioneds.get("K2").get(0).getValue());
            assertEquals(null, versioneds.get("K3"));
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testLocalZonePartialDownInSufficientReads() {
        // Stop all but one node in same zone as client. This is not sufficient
        // for zone reads.
        Set<Integer> nodeIds = cluster.getNodeIdsInZone(clientZoneId);
        nodeIds.remove(nodeIds.iterator().next());
        for(Integer nodeId: nodeIds) {
            this.vservers.get(nodeId).stop();
        }
        try {
            ArrayList<String> keys = new ArrayList<String>();
            keys.add("K1");
            keys.add("K2");
            keys.add("K3");
            Map<String, List<Versioned<String>>> versioneds = client.getAll(keys, null);
            assertEquals(null, versioneds.get("K1"));
            assertEquals(null, versioneds.get("K2"));
            assertEquals(null, versioneds.get("K3"));
        } catch(InsufficientOperationalNodesException e) {

        }
    }
}

package voldemort.store.routed;

import static org.junit.Assert.fail;

import java.io.IOException;
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
public class ZoneAffinityGetVersionsTest {

    private Store<String, String, byte[]> client;
    private Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    private Cluster cluster;
    private final Integer clientZoneId;

    public ZoneAffinityGetVersionsTest(Integer clientZoneId) {
        this.clientZoneId = clientZoneId;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 2 } });
    }

    @Before
    public void setup() throws IOException {
        byte[] bytes1 = { (byte) 'A', (byte) 'B' };
        byte[] bytes2 = { (byte) 'C', (byte) 'D' };
        List<StoreDefinition> stores = ClusterTestUtils.getZZZ322StoreDefs("memory");
        StoreDefinition storeDef = stores.get(0);
        cluster = ClusterTestUtils.getZZZCluster();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(cluster.getNodeById(0).getSocketUrl().toString());
        clientConfig.getZoneAffinity().setEnableGetVersionsOpZoneAffinity(true);
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

            if(node.getZoneId() == clientZoneId) {
                store.put(new ByteArray(bytes1), new Versioned<byte[]>(bytes1, version1), null);
            } else {
                store.put(new ByteArray(bytes1), new Versioned<byte[]>(bytes2, version2), null);
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
            client.getVersions("AB");
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
            client.getVersions("AB");
            fail("Did not fail fast");
        } catch(InsufficientOperationalNodesException e) {

        }
    }

    @Test
    public void testLocalZonePartialDownSufficientReads() {
        // turn off one node in same zone as client so that reads can still
        // complete
        this.vservers.get(cluster.getNodeIdsInZone(clientZoneId).iterator().next()).stop();
        try {
            client.getVersions("AB");
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
            client.getVersions("AB");
            fail("Did not fail fast");
        } catch(InsufficientOperationalNodesException e) {

        }
    }
}

package voldemort.store.routed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class ZoneCountWriteTest {

    private Store<String, String, byte[]> client;
    private Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    private Cluster cluster;
    private StoreDefinition storeDef = null;

    @Before
    public void setup() throws IOException {
        List<StoreDefinition> stores = ClusterTestUtils.getZZZ322StoreDefs("memory");

        storeDef = stores.get(0);
        Integer zoneCountWrite = 1;
        // override
        storeDef = new StoreDefinition(storeDef.getName(),
                                       storeDef.getType(),
                                       storeDef.getDescription(),
                                       storeDef.getKeySerializer(),
                                       storeDef.getValueSerializer(),
                                       storeDef.getTransformsSerializer(),
                                       storeDef.getRoutingPolicy(),
                                       storeDef.getRoutingStrategyType(),
                                       storeDef.getReplicationFactor(),
                                       storeDef.getPreferredReads(),
                                       storeDef.getRequiredReads(),
                                       storeDef.getPreferredWrites(),
                                       storeDef.getRequiredWrites(),
                                       storeDef.getViewTargetStoreName(),
                                       storeDef.getValueTransformation(),
                                       storeDef.getZoneReplicationFactor(),
                                       storeDef.getZoneCountReads(),
                                       zoneCountWrite,
                                       storeDef.getRetentionDays(),
                                       storeDef.getRetentionScanThrottleRate(),
                                       storeDef.getRetentionFrequencyDays(),
                                       storeDef.getSerializerFactory(),
                                       storeDef.getHintedHandoffStrategyType(),
                                       storeDef.getHintPrefListSize(),
                                       storeDef.getOwners(),
                                       storeDef.getMemoryFootprintMB());
        stores.set(0, storeDef);
        cluster = ClusterTestUtils.getZZZCluster();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(cluster.getNodeById(0).getSocketUrl().toString());
        clientConfig.getZoneAffinity().setEnableGetOpZoneAffinity(true);
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
    public void testFastPath() {
        try {
            client.put("AB", new Versioned<String>("CD"), null);
            try {
                Thread.sleep(100);
            } catch(InterruptedException e) {}
            for(Integer nodeId: vservers.keySet()) {
                VoldemortServer vs = vservers.get(nodeId);
                Store<ByteArray, byte[], byte[]> store = vs.getStoreRepository()
                                                           .getLocalStore(storeDef.getName());
                byte[] real = store.get(new ByteArray("AB".getBytes()), null).get(0).getValue();
                assertTrue(Arrays.equals(real, "CD".getBytes()));
            }
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testRemoteZoneNodeFail() {
        try {
            Set<Integer> stoppedServers = new HashSet<Integer>();
            stoppedServers.add(1);
            stoppedServers.add(2);
            stoppedServers.add(3);
            stoppedServers.add(4);
            stoppedServers.add(5);
            stoppedServers.add(6);
            stoppedServers.add(7);
            for(Integer nodeId: stoppedServers) {
                vservers.get(nodeId).stop();
            }
            client.put("AB", new Versioned<String>("CD"), null);
            try {
                Thread.sleep(100);
            } catch(InterruptedException e) {}
            for(Integer nodeId: vservers.keySet()) {
                // skip stopped ones
                if(stoppedServers.contains(nodeId)) {
                    continue;
                }
                VoldemortServer vs = vservers.get(nodeId);
                Store<ByteArray, byte[], byte[]> store = vs.getStoreRepository()
                                                           .getLocalStore(storeDef.getName());
                byte[] real = store.get(new ByteArray("AB".getBytes()), null).get(0).getValue();
                assertTrue(Arrays.equals(real, "CD".getBytes()));
            }
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testRemoteZoneNodeFailInsufficientZone() {
        Set<Integer> stoppedServers = new HashSet<Integer>();
        stoppedServers.add(2);
        stoppedServers.add(3);
        stoppedServers.add(4);
        stoppedServers.add(5);
        stoppedServers.add(6);
        stoppedServers.add(7);
        stoppedServers.add(8);
        for(Integer nodeId: stoppedServers) {
            vservers.get(nodeId).stop();
        }
        try {
            client.put("AB", new Versioned<String>("CD"), null);
            fail("Didn't throw exception");
        } catch(InsufficientZoneResponsesException e) {}
    }
}

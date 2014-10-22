package voldemort.store.routed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

/**
 * This test verifies the zone count write policy in PipelineRoutedStore
 * 
 */
@RunWith(Parameterized.class)
public class ZoneCountWriteTest {

    private Store<String, String, byte[]> client;
    private Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    private Cluster cluster;
    private StoreDefinition storeDef = null;
    private List<StoreDefinition> storeDefs;
    private ClientConfig clientConfig;
    Set<Integer> stoppedServersForRemoteZoneNodeFail;
    Set<Integer> stoppedServersForInsufficientZone;

    public ZoneCountWriteTest(Cluster cluster,
                              List<StoreDefinition> storeDefs,
                              ClientConfig clientConfig,
                              Set<Integer> stoppedServersForRemoteZoneNodeFail,
                              Set<Integer> stoppedServersForInsufficientZone) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
        this.clientConfig = clientConfig;
        this.stoppedServersForRemoteZoneNodeFail = stoppedServersForRemoteZoneNodeFail;
        this.stoppedServersForInsufficientZone = stoppedServersForInsufficientZone;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        Cluster z1z3z5cluster = ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds();
        List<StoreDefinition> z1z3z5StoreDefs = ClusterTestUtils.getZ1Z3Z5322StoreDefs("memory");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClientZoneId(3);
        clientConfig.setBootstrapUrls(z1z3z5cluster.getNodeById(3).getSocketUrl().toString());
        clientConfig.getZoneAffinity().setEnableGetOpZoneAffinity(true);
        Set<Integer> stoppedServersForRemoteZoneNodeFail = new HashSet<Integer>(Arrays.asList(4, 5, 9, 10, 11, 15, 16));
        Set<Integer> stoppedServersForInsufficientZone = new HashSet<Integer>(Arrays.asList(5, 9, 10, 11, 15, 16, 17));
        Cluster zzzCluster = ClusterTestUtils.getZZZCluster();
        List<StoreDefinition> zzzStoreDefs = ClusterTestUtils.getZZZ322StoreDefs("memory");
        ClientConfig zzzClientConfig = new ClientConfig();
        zzzClientConfig.setClientZoneId(0);
        zzzClientConfig.setBootstrapUrls(zzzCluster.getNodeById(0).getSocketUrl().toString());
        zzzClientConfig.getZoneAffinity().setEnableGetOpZoneAffinity(true);
        Set<Integer> zzzstoppedServersForRemoteZoneNodeFail = new HashSet<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        Set<Integer> zzzstoppedServersForInsufficientZone = new HashSet<Integer>(Arrays.asList(2, 3, 4, 5, 6, 7, 8));
        return Arrays.asList(new Object[][] {
                { z1z3z5cluster, z1z3z5StoreDefs, clientConfig,
                        stoppedServersForRemoteZoneNodeFail, stoppedServersForInsufficientZone },
                { zzzCluster, zzzStoreDefs, zzzClientConfig,
                        zzzstoppedServersForRemoteZoneNodeFail,
                        zzzstoppedServersForInsufficientZone }

        });
    }

    @Before
    public void setup() throws IOException {
        storeDef = storeDefs.get(0);
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
                                       storeDef.getMemoryFootprintMB(),
                                       storeDef.getKafkaConsumer());
        storeDefs.set(0, storeDef);
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
                                                                                storeDefs,
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
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
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
            for(Integer nodeId: stoppedServersForRemoteZoneNodeFail) {
                vservers.get(nodeId).stop();
            }
            client.put("AB", new Versioned<String>("CD"), null);
            try {
                Thread.sleep(100);
            } catch(InterruptedException e) {}
            for(Integer nodeId: vservers.keySet()) {
                // skip stopped ones
                if(stoppedServersForRemoteZoneNodeFail.contains(nodeId)) {
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
            e.printStackTrace();
        }
    }

    @Test
    public void testRemoteZoneNodeFailInsufficientZone() {
        for(Integer nodeId: stoppedServersForInsufficientZone) {
            vservers.get(nodeId).stop();
        }
        try {
            client.put("AB", new Versioned<String>("CD"), null);
            fail("Didn't throw exception");
        } catch(InsufficientZoneResponsesException e) {}
    }
}

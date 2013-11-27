package voldemort.store.routed;

import static org.junit.Assert.assertEquals;
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
public abstract class AbstractZoneAffinityTest {

    protected Store<String, String, byte[]> client;
    protected Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    protected Map<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    protected ClientConfig clientConfig;

    protected final List<StoreDefinition> stores;
    protected final StoreDefinition storeDef;
    protected final Cluster cluster;
    protected final Integer clientZoneId;

    public AbstractZoneAffinityTest(Integer clientZoneId, Cluster cluster, List<StoreDefinition> storeDefs) {
        this.clientZoneId = clientZoneId;
        this.cluster = cluster;
        this.stores = storeDefs;
        this.storeDef = stores.get(0);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { 0, ClusterTestUtils.getZZZCluster(),ClusterTestUtils.getZZZ322StoreDefs("memory") },
                { 1, ClusterTestUtils.getZZZCluster(),ClusterTestUtils.getZZZ322StoreDefs("memory") },
                { 2, ClusterTestUtils.getZZZCluster(),ClusterTestUtils.getZZZ322StoreDefs("memory") },
                { 1, ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),ClusterTestUtils.getZ1Z3322StoreDefs("memory") },
                { 3, ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),ClusterTestUtils.getZ1Z3322StoreDefs("memory") } });
    }

    public abstract void setupZoneAffinitySettings();

    @Before
    public void setup() throws IOException {
        byte[] bytes1 = { (byte) 'A', (byte) 'B' };
        byte[] bytes2 = { (byte) 'C', (byte) 'D' };
        clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(cluster.getNodes().iterator().next().getSocketUrl().toString());
        clientConfig.setClientZoneId(clientZoneId);
        setupZoneAffinitySettings();
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
            socketStoreFactories.put(nodeId, socketStoreFactory);
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
        client.close();
        for(VoldemortServer vs: this.vservers.values()) {
            vs.stop();
        }
        for(SocketStoreFactory ssf: this.socketStoreFactories.values()) {
            ssf.close();
        }
        ClusterTestUtils.reset();
    }
}

package voldemort.client.protocol.admin;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.TestSocketStoreFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/*
 * Starts a streaming session and inserts some keys Using fetchKeys we check if
 * they keys made it to the responsible node
 * Three scenarios are tested :
 * (i) Non zoned cluster with contiguous node ids
 * (ii) Non zoned cluster with non contiguous node ids
 * (iii) Zoned cluster with contiguous zone/node ids
 *
 */
@RunWith(Parameterized.class)
public class StreamingClientTest {

    private long startTime;
    public final String SERVER_LOCAL_URL = "tcp://localhost:";
    public final static String TEST_STORE_NAME = "test-store-streaming-1";
    public final String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";
    public int numServers;
    private int NUM_KEYS_1 = 4000;
    private SocketStoreFactory socketStoreFactory = new TestSocketStoreFactory();
    private VoldemortServer[] servers = null;
    private int[] serverPorts = null;
    private Cluster cluster;
    private AdminClient adminClient;
    private SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private StoreDefinition storeDef;
    private int nodeIdOnWhichToVerifyKey;

    public StreamingClientTest(int numServers,
                               Cluster cluster,
                               int nodeIdOnWhichToVerifyKey,
                               StoreDefinition storeDef) {
        this.numServers = numServers;
        this.cluster = cluster;
        this.nodeIdOnWhichToVerifyKey = nodeIdOnWhichToVerifyKey;
        this.storeDef = storeDef;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        StoreDefinition storeDefConsistestStrategy = new StoreDefinitionBuilder().setName(TEST_STORE_NAME)
                                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                                 .setReplicationFactor(2)
                                                                                 .setPreferredReads(1)
                                                                                 .setRequiredReads(1)
                                                                                 .setPreferredWrites(2)
                                                                                 .setRequiredWrites(2)
                                                                                 .build();

        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(1, 2);
        zoneReplicationFactor.put(3, 2);
        StoreDefinition storeDefZoneStrategy = new StoreDefinitionBuilder().setName(TEST_STORE_NAME)
                                                                           .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                           .setKeySerializer(new SerializerDefinition("string"))
                                                                           .setValueSerializer(new SerializerDefinition("string"))
                                                                           .setRoutingPolicy(RoutingTier.CLIENT)
                                                                           .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                           .setReplicationFactor(4)
                                                                           .setPreferredReads(1)
                                                                           .setRequiredReads(1)
                                                                           .setPreferredWrites(1)
                                                                           .setRequiredWrites(1)
                                                                           .setZoneCountReads(0)
                                                                           .setZoneCountWrites(0)
                                                                           .setZoneReplicationFactor(zoneReplicationFactor)
                                                                           .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                           .build();

        return Arrays.asList(new Object[][] {

                { 2,
                  ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 },
                                                  { 4, 5, 6, 7 } }),
                  0,
                  storeDefConsistestStrategy
                },
                {
                  2,
                  ServerTestUtils.getLocalNonContiguousNodesCluster(new int[] { 1, 3 },
                                                                          new int[][] {
                                                                                  { 0, 1, 2, 3 },
                                                                                  { 4, 5, 6, 7 } }),
                  1, storeDefConsistestStrategy
                },
                { 6,
                  ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                  3,
                  storeDefZoneStrategy
                }
        });
    }

    @Before
    public void testSetup() throws IOException {

        if(null == servers) {
            servers = new VoldemortServer[numServers];
            serverPorts = new int[numServers];

            File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
            try {
                FileUtils.writeStringToFile(tempStoreXml,
                                            new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef)));
            } catch(IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                throw e1;
            }

            int count = 0;
            for(int nodeId: cluster.getNodeIds()) {
                try {
                    servers[count] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          ServerTestUtils.createServerConfig(true,
                                                                                                             nodeId,
                                                                                                             TestUtils.createTempDir()
                                                                                                                      .getAbsolutePath(),
                                                                                                             null,
                                                                                                             tempStoreXml.getAbsolutePath(),
                                                                                                             new Properties()),
                                                                          cluster);
                } catch(IOException e) {
                    e.printStackTrace();
                    throw e;
                }
                serverPorts[count] = servers[count].getIdentityNode().getSocketPort();
                count++;
            }
            adminClient = ServerTestUtils.getAdminClient(cluster);
        }

        startTime = System.currentTimeMillis();

    }

    @After
    public void testCleanup() {
        // Teardown for data used by the unit tests
        if(servers != null) {
            for(VoldemortServer server: servers) {
                server.stop();
            }
        }
    }

    @Test
    public void testStreaming() {

        Props property = new Props();
        property.put("streaming.platform.bootstrapURL", SERVER_LOCAL_URL + serverPorts[0]);
        StreamingClientConfig config = new StreamingClientConfig(property);

        BaseStreamingClient streamer = new BaseStreamingClient(config);

        streamer.initStreamingSession(TEST_STORE_NAME, new Callable<Object>() {

            @Override
            public Object call() throws Exception {

                return null;
            }
        }, new Callable<Object>() {

            @Override
            public Object call() throws Exception {

                return null;
            }
        }, true);

        for(int i = 0; i < NUM_KEYS_1; i++) {
            String key = i + "";
            String value = key;

            Versioned<byte[]> outputValue = Versioned.value(value.getBytes());
            // adminClient.streamingPut(new ByteArray(key.getBytes()),
            // outputValue);
            streamer.streamingPut(new ByteArray(key.getBytes()), outputValue);
        }
        streamer.commitToVoldemort();
        streamer.closeStreamingSession();
        assertEquals(verifyKeysExist(nodeIdOnWhichToVerifyKey), true);

    }


    /**
     * Checks if the streamingClient stays calm and not throw NPE when calling
     * commit before it has been initialized
     */
    @Test
    public void testUnInitializedClientPreventNPE() {

        Props property = new Props();
        property.put("streaming.platform.bootstrapURL", SERVER_LOCAL_URL + serverPorts[0]);
        StreamingClientConfig config = new StreamingClientConfig(property);
        BaseStreamingClient streamer = new BaseStreamingClient(config);
        try {
            streamer.commitToVoldemort();
        } catch (NullPointerException e) {
            e.printStackTrace();
            Assert.fail("Should not throw NPE at this stage even though streamingSession not initialized");
        }
    }



    /*
     * Checks if each node has the keys it is reponsible for returns false
     * otherwise
     */
    public boolean verifyKeysExist(int nodeIdToVerifyOn) {
        RoutingStrategyFactory factory = new RoutingStrategyFactory();
        RoutingStrategy storeRoutingStrategy = factory.updateRoutingStrategy(storeDef,
                                                                             adminClient.getAdminClientCluster());

        HashMap<Integer, ArrayList<String>> expectedNodeIdToKeys;
        expectedNodeIdToKeys = new HashMap();
        Collection<Node> nodesInCluster = adminClient.getAdminClientCluster().getNodes();
        for(Node node: nodesInCluster) {
            ArrayList<String> keysForNode = new ArrayList();
            expectedNodeIdToKeys.put(node.getId(), keysForNode);
        }
        for(int i = 0; i < NUM_KEYS_1; i++) {
            String key = i + "";
            String value = key;
            List<Node> nodeList = storeRoutingStrategy.routeRequest(key.getBytes());
            for(Node node: nodeList) {
                ArrayList<String> keysForNode = expectedNodeIdToKeys.get(node.getId());
                keysForNode.add(key);
            }
        }

        ArrayList<String> fetchedKeysForNode = new ArrayList();
        for(Node node: nodesInCluster) {

            List<Integer> partitionIdList = Lists.newArrayList();
            partitionIdList.addAll(node.getPartitionIds());

            Iterator<ByteArray> keyIteratorRef = null;
            keyIteratorRef = adminClient.bulkFetchOps.fetchKeys(node.getId(),
                                                                TEST_STORE_NAME,
                                                                partitionIdList,
                                                                null,
                                                                false);

            final SerializerDefinition serializerDef = storeDef.getKeySerializer();
            final SerializerFactory serializerFactory = new DefaultSerializerFactory();
            @SuppressWarnings("unchecked")
            final Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);

            final CompressionStrategy keysCompressionStrategy;
            if(serializerDef != null && serializerDef.hasCompression()) {
                keysCompressionStrategy = new CompressionStrategyFactory().get(serializerDef.getCompression());
            } else {
                keysCompressionStrategy = null;
            }
            final Iterator<ByteArray> keyIterator = keyIteratorRef;
            while(keyIterator.hasNext()) {

                byte[] keyBytes = keyIterator.next().get();
                try {
                    Object keyObject = serializer.toObject((null == keysCompressionStrategy) ? keyBytes
                                                                                            : keysCompressionStrategy.inflate(keyBytes));
                    fetchedKeysForNode.add((String) keyObject);

                } catch(IOException e) {

                    e.printStackTrace();
                }
            }

        }

        ArrayList<String> keysForNode = expectedNodeIdToKeys.get(nodeIdToVerifyOn);
        if(!fetchedKeysForNode.containsAll(keysForNode))
            return false;
        else
            return true;
    }
}

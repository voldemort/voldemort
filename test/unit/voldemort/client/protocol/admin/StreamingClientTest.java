package voldemort.client.protocol.admin;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/*
 * Starts a streaming session and inserts some keys Using fetchKeys we check if
 * they keys made it to the responsible node
 */
public class StreamingClientTest {

    private static long startTime;
    public static final String SERVER_LOCAL_URL = "tcp://localhost:";
    public static final String TEST_STORE_NAME = "test-store-streaming-1";
    public static final String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";

    public static final int TOTAL_SERVERS = 2;

    private static int NUM_KEYS_1 = 4000;
    private static SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(TOTAL_SERVERS,
                                                                                         10000,
                                                                                         100000,
                                                                                         32 * 1024);
    private static VoldemortServer[] servers = null;
    private static int[] serverPorts = null;
    private static Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] {
            { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
    private static AdminClient adminClient;

    private static SerializerFactory serializerFactory = new DefaultSerializerFactory();

    private static StoreDefinition storeDef;

    @BeforeClass
    public static void testSetup() {

        if(null == servers) {
            servers = new VoldemortServer[TOTAL_SERVERS];
            serverPorts = new int[TOTAL_SERVERS];

            storeDef = new StoreDefinitionBuilder().setName(TEST_STORE_NAME)
                                                   .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                   .setRoutingPolicy(RoutingTier.SERVER)
                                                   .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                   .setReplicationFactor(2)
                                                   .setPreferredReads(1)
                                                   .setRequiredReads(1)
                                                   .setPreferredWrites(2)
                                                   .setRequiredWrites(2)
                                                   .build();

            File tempStoreXml = new File(TestUtils.createTempDir(), "stores.xml");
            try {
                FileUtils.writeStringToFile(tempStoreXml,
                                            new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef)));
            } catch(IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            for(int i = 0; i < TOTAL_SERVERS; i++) {
                try {
                    servers[i] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                      ServerTestUtils.createServerConfig(true,
                                                                                                         i,
                                                                                                         TestUtils.createTempDir()
                                                                                                                  .getAbsolutePath(),
                                                                                                         null,
                                                                                                         tempStoreXml.getAbsolutePath(),
                                                                                                         new Properties()),
                                                                      cluster);
                } catch(IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                serverPorts[i] = servers[i].getIdentityNode().getSocketPort();
            }
            adminClient = ServerTestUtils.getAdminClient(cluster);
        }

        startTime = System.currentTimeMillis();

    }

    @AfterClass
    public static void testCleanup() {
        // Teardown for data used by the unit tests
    }

    @Test
    public void testStreaming() {

        Props property = new Props();
        property.put("streaming.platform.bootstrapURL", SERVER_LOCAL_URL + serverPorts[0]);
        StreamingClientConfig config = new StreamingClientConfig(property);

        StreamingClient streamer = new StreamingClient(config);

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
        assertEquals(verifyKeysExist(), true);

    }

    /*
     * Checks if each node has the keys it is reponsible for returns false
     * otherwise
     */
    public boolean verifyKeysExist() {
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
                // Ugly hack to be able to separate text by newlines
                // vs. spaces
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

        ArrayList<String> keysForNode = expectedNodeIdToKeys.get(0);
        if(!fetchedKeysForNode.containsAll(keysForNode))
            return false;
        else
            return true;
    }
}

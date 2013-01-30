package voldemort.client.protocol.admin;

import java.io.File;
import java.io.IOException;
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
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class StreamingClientTest {

    private static long startTime;
    public static final String SERVER_LOCAL_URL = "tcp://localhost:";
    public static final String TEST_STORE_NAME = "test-store-streaming-1";
    public static final String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";

    public static final int TOTAL_SERVERS = 2;

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
                                                   .setPreferredWrites(1)
                                                   .setRequiredWrites(1)
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

        for(int i = 0; i < 40000; i++) {
            String key = i + "";
            String value = key;

            Versioned<byte[]> outputValue = Versioned.value(value.getBytes());
            // adminClient.streamingPut(new ByteArray(key.getBytes()),
            // outputValue);
            streamer.streamingPut(new ByteArray(key.getBytes()), outputValue);
        }
        streamer.closeStreamingSession();

    }

}

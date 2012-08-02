package voldemort.client;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

public class EndToEndRebootstrapTest extends TestCase {

    private static final String STORE_NAME = "test-replication-persistent";
    private static final String CLUSTER_KEY = "cluster.xml";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private StoreClient<String, String> storeClient;
    SystemStore<String, String> sysStoreVersion;
    private Cluster cluster;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;

    @Override
    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers = new VoldemortServer[2];
        servers[0] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          ServerTestUtils.createServerConfig(true,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClientRegistryUpdateInSecs(5);
        clientConfig.setAsyncMetadataRefreshInMs(5000);
        clientConfig.setBootstrapUrls(bootstrapUrl);
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(clientConfig);
        storeClient = storeClientFactory.getStoreClient(STORE_NAME);
        sysStoreVersion = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                          bootStrapUrls,
                                                          0);

    }

    @Override
    @After
    public void tearDown() throws Exception {
        servers[0].stop();
        servers[1].stop();
    }

    private String getClientInfo(AdminClient adminClient,
                                 String storeName,
                                 StoreDefinition storeDefinition) throws IOException {
        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = adminClient.fetchEntries(0,
                                                                                         storeName,
                                                                                         cluster.getNodeById(0)
                                                                                                .getPartitionIds(),
                                                                                         null,
                                                                                         true);

        boolean hasData = iterator.hasNext();
        assertTrue(hasData);

        String clientInfo = null;
        CompressionStrategy valueCompressionStrategy = null;

        SerializerFactory serializerFactory = new DefaultSerializerFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
        if(null != valueSerializerDef && valueSerializerDef.hasCompression()) {
            valueCompressionStrategy = new CompressionStrategyFactory().get(valueSerializerDef.getCompression());
        }

        @SuppressWarnings("unchecked")
        Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getValueSerializer());

        try {

            Pair<ByteArray, Versioned<byte[]>> kvPair = iterator.next();
            VectorClock version = (VectorClock) kvPair.getSecond().getVersion();
            byte[] valueBytes = kvPair.getSecond().getValue();

            Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                            : valueCompressionStrategy.inflate(valueBytes));

            generator.writeObject(valueObject);

            StringBuffer buf = stringWriter.getBuffer();
            clientInfo = buf.toString();
            buf.setLength(0);
        } finally {

        }
        return clientInfo;
    }

    @Test
    public void testEndToEndRebootstrap() {
        try {
            // Do a sample get, put to check client is correctly initialized.
            String key = "city";
            String value = "SF";
            String storeName = SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name();
            String bootstrapTime = "";
            String newBootstrapTime = "";
            AdminClient adminClient = new AdminClient(bootStrapUrls[0], new AdminClientConfig());

            try {
                storeClient.put(key, value);
                String received = storeClient.getValue(key);
                assertEquals(value, received);
            } catch(VoldemortException ve) {
                fail("Error in doing basic get, put");
            }

            // Track the original bootstrap timestamp published by client
            String originalClientInfo = getClientInfo(adminClient,
                                                      storeName,
                                                      SystemStoreConstants.getSystemStoreDef(storeName));
            try {
                bootstrapTime = originalClientInfo.split("]")[0].split("\\[")[1];
            } catch(Exception e) {
                fail("Error in retrieving bootstrap time: " + e);
            }

            // Update cluster.xml metadata
            VoldemortAdminTool adminTool = new VoldemortAdminTool();
            ClusterMapper mapper = new ClusterMapper();
            for(Node node: cluster.getNodes()) {
                VoldemortAdminTool.executeSetMetadata(node.getId(),
                                                      adminClient,
                                                      CLUSTER_KEY,
                                                      mapper.writeCluster(cluster));
                VoldemortAdminTool.updateMetadataversion(CLUSTER_KEY, sysStoreVersion);

            }

            // Wait for about 15 seconds to be sure
            try {
                Thread.sleep(15000);
            } catch(Exception e) {
                fail("Interrupted .");
            }

            // Retrieve the new client bootstrap timestamp
            String newClientInfo = getClientInfo(adminClient,
                                                 storeName,
                                                 SystemStoreConstants.getSystemStoreDef(storeName));
            try {
                newBootstrapTime = newClientInfo.split("]")[0].split("\\[")[1];
            } catch(Exception e) {
                fail("Error in retrieving bootstrap time: " + e);
            }

            assertFalse(bootstrapTime.equals(newBootstrapTime));
            long origTime = Long.parseLong(bootstrapTime);
            long newTime = Long.parseLong(newBootstrapTime);
            assertTrue(newTime > origTime);

        } catch(Exception e) {
            fail("Error in validating end to end client rebootstrap : " + e);
        }
    }
}

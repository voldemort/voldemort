package voldemort.client;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

@SuppressWarnings({ "unchecked" })
public class ClientRegistryTest extends TestCase {

    public static final String SERVER_LOCAL_URL = "tcp://localhost:";
    public static final String TEST_STORE_NAME = "test-store-eventual-1";
    public static final String TEST_STORE_NAME2 = "test-store-eventual-2";
    public static final String STORES_XML_FILE = "test/common/voldemort/config/stores.xml";
    public static final String CLIENT_CONTEXT_NAME = "testClientRegistryHappyPath";
    public static final String CLIENT_CONTEXT_NAME2 = "testClientRegistryUnhappyPath";
    public static final int CLIENT_REGISTRY_REFRSH_INTERVAL = 1;
    public static final int TOTAL_SERVERS = 2;

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(TOTAL_SERVERS,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private static VoldemortServer[] servers = null;
    private static int[] serverPorts = null;
    private Cluster cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 },
            { 4, 5, 6, 7 } });
    private static AdminClient adminClient;

    private SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(SystemStoreConstants.getSystemStoreDef(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name())
                                                                                                                          .getValueSerializer());
    private long startTime;

    @Before
    public void setUp() throws Exception {

        if(null == servers) {
            servers = new VoldemortServer[TOTAL_SERVERS];
            serverPorts = new int[TOTAL_SERVERS];

            for(int i = 0; i < TOTAL_SERVERS; i++) {
                servers[i] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                  ServerTestUtils.createServerConfig(true,
                                                                                                     i,
                                                                                                     TestUtils.createTempDir()
                                                                                                              .getAbsolutePath(),
                                                                                                     null,
                                                                                                     STORES_XML_FILE,
                                                                                                     new Properties()),
                                                                  cluster);
                serverPorts[i] = servers[i].getIdentityNode().getSocketPort();
            }
            adminClient = ServerTestUtils.getAdminClient(cluster);
        }

        startTime = System.currentTimeMillis();
    }

    @After
    public void tearDown() throws Exception {
        this.clearRegistryContent();
    }

    @Test
    public void testHappyPath() {
        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[0])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client1 = socketFactory.getStoreClient(TEST_STORE_NAME);
        client1.put("k", "v");
        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(0,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);
        assertEquals(TEST_STORE_NAME, infoList.get(0).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertEquals(0, infoList.get(0).getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());

        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
        assertEquals(1, infoList.size());

        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertEquals(TEST_STORE_NAME, infoList.get(0).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertEquals(0, infoList.get(0).getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
        assertEquals(1, infoList.size());

        try {
            Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
        } catch(InterruptedException e) {}
        // now the periodical update has gone through, it shall be higher than
        // the bootstrap time
        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertTrue("Client registry not updated.",
                   infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());

        assertTrue("Client Config received from the Client registry system store is incorrect.",
                   isConfigEqual(infoList.get(0).getClientConfig(), clientConfig));
        socketFactory.close();
    }

    @Test
    public void testTwoClients() {
        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[0])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client1 = socketFactory.getStoreClient(TEST_STORE_NAME);
        StoreClient<String, String> client2 = socketFactory.getStoreClient(TEST_STORE_NAME);

        client1.put("k1", "v1");
        client2.put("k2", "v2");

        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(0,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);
        assertEquals(TEST_STORE_NAME, infoList.get(0).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertTrue("Client registry sequence number incorrect", 1 >= infoList.get(0)
                                                                             .getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());

        assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
        assertTrue("Client registry sequence number incorrect", 1 >= infoList.get(1)
                                                                             .getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(1).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());
        assertEquals(infoList.size(), 2);

        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertEquals(TEST_STORE_NAME, infoList.get(0).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertTrue("Client registry sequence number incorrect", 1 >= infoList.get(0)
                                                                             .getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());

        assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
        assertTrue("Client registry sequence number incorrect", 1 >= infoList.get(1)
                                                                             .getClientSequence());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(1).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        assertEquals(infoList.size(), 2);

        try {
            Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
        } catch(InterruptedException e) {}
        // now the periodical update has gone through, it shall be higher than
        // the bootstrap time
        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertTrue("Client registry not updated.",
                   infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());
        assertTrue("Client registry not updated.",
                   infoList.get(1).getBootstrapTime() < infoList.get(1).getUpdateTime());

        socketFactory.close();
    }

    @Test
    public void testTwoStores() {
        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[0])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
        StoreClient<String, String> client1 = socketFactory.getStoreClient(TEST_STORE_NAME);
        StoreClient<String, String> client2 = socketFactory.getStoreClient(TEST_STORE_NAME2);

        client1.put("k1", "v1");
        client2.put("k2", "v2");

        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(0,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);

        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());

        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(1).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
            assertEquals(0, infoList.get(0).getClientSequence());
            assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
            assertEquals(1, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());
        } else {
            assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
            assertEquals(1, infoList.get(0).getClientSequence());
            assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
        }

        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);

        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(0).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());

        assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
        assertTrue("Client registry bootstrap time incorrect",
                   startTime <= infoList.get(1).getBootstrapTime());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
            assertEquals(0, infoList.get(0).getClientSequence());
            assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
            assertEquals(1, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());
        } else {
            assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
            assertEquals(1, infoList.get(0).getClientSequence());
            assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
        }

        try {
            Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
        } catch(InterruptedException e) {}
        // now the periodical update has gone through, it shall be higher than
        // the bootstrap time
        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertTrue("Client registry not updated.",
                   infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());
        assertTrue("Client registry not updated.",
                   infoList.get(1).getBootstrapTime() < infoList.get(1).getUpdateTime());

        socketFactory.close();
    }

    @Test
    public void testTwoFactories() {
        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[0])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory1 = new SocketStoreClientFactory(clientConfig);

        ClientConfig clientConfig2 = new ClientConfig().setMaxThreads(4)
                                                       .setMaxTotalConnections(4)
                                                       .setMaxConnectionsPerNode(4)
                                                       .setBootstrapUrls(SERVER_LOCAL_URL
                                                                         + serverPorts[0])
                                                       .setClientContextName(CLIENT_CONTEXT_NAME2)
                                                       .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                       .setEnableLazy(false);
        SocketStoreClientFactory socketFactory2 = new SocketStoreClientFactory(clientConfig2);

        StoreClient<String, String> client1 = socketFactory1.getStoreClient(TEST_STORE_NAME);
        StoreClient<String, String> client2 = socketFactory2.getStoreClient(TEST_STORE_NAME2);

        client1.put("k1", "v1");
        client2.put("k2", "v2");

        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(0,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);

        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());

        } else {
            assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
        }

        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);

        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());

        } else {
            assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
        }

        try {
            Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
        } catch(InterruptedException e) {}
        // now the periodical update has gone through, it shall be higher than
        // the bootstrap time
        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertTrue("Client registry not updated.",
                   infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());
        assertTrue("Client registry not updated.",
                   infoList.get(1).getBootstrapTime() < infoList.get(1).getUpdateTime());

        socketFactory1.close();
        socketFactory2.close();
    }

    @Test
    public void testOneServerFailure() {
        // bring down one server before starting up the clients
        servers[0].stop();

        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[1])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory1 = new SocketStoreClientFactory(clientConfig);

        ClientConfig clientConfig2 = new ClientConfig().setMaxThreads(4)
                                                       .setMaxTotalConnections(4)
                                                       .setMaxConnectionsPerNode(4)
                                                       .setBootstrapUrls(SERVER_LOCAL_URL
                                                                         + serverPorts[1])
                                                       .setClientContextName(CLIENT_CONTEXT_NAME2)
                                                       .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                       .setEnableLazy(false);
        SocketStoreClientFactory socketFactory2 = new SocketStoreClientFactory(clientConfig2);

        StoreClient<String, String> client1 = socketFactory1.getStoreClient(TEST_STORE_NAME);
        StoreClient<String, String> client2 = socketFactory2.getStoreClient(TEST_STORE_NAME2);

        client1.put("k1", "v1");
        client2.put("k2", "v2");

        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(1,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);

        assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
        assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

        if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());

        } else {
            assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(0).getContext());
            assertEquals(0, infoList.get(0).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(0).getBootstrapTime());

            assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
            assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
            assertEquals(0, infoList.get(1).getClientSequence());
            assertTrue("Client registry bootstrap time incorrect",
                       startTime <= infoList.get(1).getBootstrapTime());

            assertTrue("Client registry bootstrap time incorrect",
                       infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
        }

        try {
            Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
        } catch(InterruptedException e) {}
        // now the periodical update has gone through, it shall be higher than
        // the bootstrap time
        it = adminClient.fetchEntries(1,
                                      SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                      emptyPartitionList,
                                      null,
                                      false);
        infoList = getClientRegistryContent(it);
        assertTrue("Client registry not updated.",
                   infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());
        assertTrue("Client registry not updated.",
                   infoList.get(1).getBootstrapTime() < infoList.get(1).getUpdateTime());

        socketFactory1.close();
        socketFactory2.close();
    }

    @Test
    public void testRepeatRegistrationSameFactory() {

        List<Integer> emptyPartitionList = Lists.newArrayList();
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                      .setMaxTotalConnections(4)
                                                      .setMaxConnectionsPerNode(4)
                                                      .setBootstrapUrls(SERVER_LOCAL_URL
                                                                        + serverPorts[1])
                                                      .setClientContextName(CLIENT_CONTEXT_NAME)
                                                      .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                      .setEnableLazy(false);
        SocketStoreClientFactory socketFactory1 = new SocketStoreClientFactory(clientConfig);

        ClientConfig clientConfig2 = new ClientConfig().setMaxThreads(4)
                                                       .setMaxTotalConnections(4)
                                                       .setMaxConnectionsPerNode(4)
                                                       .setBootstrapUrls(SERVER_LOCAL_URL
                                                                         + serverPorts[1])
                                                       .setClientContextName(CLIENT_CONTEXT_NAME2)
                                                       .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                       .setEnableLazy(false);
        SocketStoreClientFactory socketFactory2 = new SocketStoreClientFactory(clientConfig2);

        for(int i = 0; i < 3; i++) {

            StoreClient<String, String> client1 = socketFactory1.getStoreClient(TEST_STORE_NAME);
            StoreClient<String, String> client2 = socketFactory2.getStoreClient(TEST_STORE_NAME2);

            client1.put("k1", "v1");
            client2.put("k2", "v2");

        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(1,
                                                                                   SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                   emptyPartitionList,
                                                                                   null,
                                                                                   false);
        ArrayList<ClientInfo> infoList = getClientRegistryContent(it);
        assertEquals("Incrrect # of entries created in client registry", 6, infoList.size());

        socketFactory1.close();
        socketFactory2.close();
    }

    @Test
    public void testRepeatRegistrationDifferentFactories() {
        long client1LastBootstrapTime = 0;
        long client2LastBootstrapTime = 0;
        for(int i = 0; i < 3; i++) {

            List<Integer> emptyPartitionList = Lists.newArrayList();
            ClientConfig clientConfig = new ClientConfig().setMaxThreads(4)
                                                          .setMaxTotalConnections(4)
                                                          .setMaxConnectionsPerNode(4)
                                                          .setBootstrapUrls(SERVER_LOCAL_URL
                                                                            + serverPorts[1])
                                                          .setClientContextName(CLIENT_CONTEXT_NAME)
                                                          .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                          .setEnableLazy(false);
            SocketStoreClientFactory socketFactory1 = new SocketStoreClientFactory(clientConfig);

            ClientConfig clientConfig2 = new ClientConfig().setMaxThreads(4)
                                                           .setMaxTotalConnections(4)
                                                           .setMaxConnectionsPerNode(4)
                                                           .setBootstrapUrls(SERVER_LOCAL_URL
                                                                             + serverPorts[1])
                                                           .setClientContextName(CLIENT_CONTEXT_NAME2)
                                                           .setClientRegistryUpdateInSecs(CLIENT_REGISTRY_REFRSH_INTERVAL)
                                                           .setEnableLazy(false);
            SocketStoreClientFactory socketFactory2 = new SocketStoreClientFactory(clientConfig2);

            StoreClient<String, String> client1 = socketFactory1.getStoreClient(TEST_STORE_NAME);
            StoreClient<String, String> client2 = socketFactory2.getStoreClient(TEST_STORE_NAME2);

            client1.put("k1", "v1");
            client2.put("k2", "v2");

            Iterator<Pair<ByteArray, Versioned<byte[]>>> it = adminClient.fetchEntries(1,
                                                                                       SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                                                                       emptyPartitionList,
                                                                                       null,
                                                                                       false);
            ArrayList<ClientInfo> infoList = getClientRegistryContent(it);

            assertEquals("Incrrect # of entries created in client registry", 2, infoList.size());

            assertNotNull("Client version is null", infoList.get(0).getReleaseVersion());
            assertNotNull("Client version is null", infoList.get(1).getReleaseVersion());

            if(infoList.get(0).getStoreName().equals(TEST_STORE_NAME)) {
                assertEquals(CLIENT_CONTEXT_NAME, infoList.get(0).getContext());
                assertEquals(0, infoList.get(0).getClientSequence());
                assertTrue("Client registry bootstrap time incorrect",
                           startTime <= infoList.get(0).getBootstrapTime());

                assertEquals(TEST_STORE_NAME2, infoList.get(1).getStoreName());
                assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(1).getContext());
                assertEquals(0, infoList.get(1).getClientSequence());
                assertTrue("Client registry bootstrap time incorrect",
                           startTime <= infoList.get(1).getBootstrapTime());

                assertTrue("Client registry bootstrap time incorrect",
                           infoList.get(1).getBootstrapTime() >= infoList.get(0).getBootstrapTime());

            } else {
                assertEquals(TEST_STORE_NAME2, infoList.get(0).getStoreName());
                assertEquals(CLIENT_CONTEXT_NAME2, infoList.get(0).getContext());
                assertEquals(0, infoList.get(0).getClientSequence());
                assertTrue("Client registry bootstrap time incorrect",
                           startTime <= infoList.get(0).getBootstrapTime());

                assertEquals(TEST_STORE_NAME, infoList.get(1).getStoreName());
                assertEquals(CLIENT_CONTEXT_NAME, infoList.get(1).getContext());
                assertEquals(0, infoList.get(1).getClientSequence());
                assertTrue("Client registry bootstrap time incorrect",
                           startTime <= infoList.get(1).getBootstrapTime());

                assertTrue("Client registry bootstrap time incorrect",
                           infoList.get(0).getBootstrapTime() >= infoList.get(1).getBootstrapTime());
            }

            try {
                Thread.sleep(CLIENT_REGISTRY_REFRSH_INTERVAL * 1000 * 5);
            } catch(InterruptedException e) {}
            // now the periodical update has gone through, it shall be higher
            // than
            // the bootstrap time
            it = adminClient.fetchEntries(1,
                                          SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name(),
                                          emptyPartitionList,
                                          null,
                                          false);
            infoList = getClientRegistryContent(it);

            assertTrue("Client registry not updated.",
                       infoList.get(0).getBootstrapTime() < infoList.get(0).getUpdateTime());
            assertTrue("Client registry not updated.",
                       infoList.get(1).getBootstrapTime() < infoList.get(1).getUpdateTime());

            assertTrue("Bootstrap time does not increase client bounces",
                       infoList.get(0).getBootstrapTime() > client1LastBootstrapTime);
            assertTrue("Bootstrap time does not increase client bounces",
                       infoList.get(1).getBootstrapTime() > client2LastBootstrapTime);

            client1LastBootstrapTime = infoList.get(0).getBootstrapTime();
            client2LastBootstrapTime = infoList.get(0).getBootstrapTime();

            socketFactory1.close();
            socketFactory2.close();
        }
    }

    private ArrayList<ClientInfo> getClientRegistryContent(Iterator<Pair<ByteArray, Versioned<byte[]>>> it) {
        ArrayList<ClientInfo> infoList = Lists.newArrayList();
        while(it.hasNext()) {
            String clientInfoString = (String) valueSerializer.toObject(it.next()
                                                                          .getSecond()
                                                                          .getValue());
            Properties props = new Properties();
            try {

                props.load(new ByteArrayInputStream(clientInfoString.getBytes()));

                ClientConfig clientConfig = new ClientConfig();
                clientConfig.setMaxConnectionsPerNode(Integer.parseInt(props.getProperty("max_connections")))
                            .setMaxTotalConnections(Integer.parseInt(props.getProperty("max_total_connections")))
                            .setRoutingTimeout(Integer.parseInt(props.getProperty("routing_timeout_ms")),
                                               TimeUnit.MILLISECONDS)
                            .setConnectionTimeout(Integer.parseInt(props.getProperty("connection_timeout_ms")),
                                                  TimeUnit.MILLISECONDS)
                            .setSocketTimeout(Integer.parseInt(props.getProperty("socket_timeout_ms")),
                                              TimeUnit.MILLISECONDS)
                            .setClientZoneId(Integer.parseInt(props.getProperty("client_zone_id")))
                            .setFailureDetectorImplementation(props.getProperty("failuredetector_implementation"));

                ClientInfo cInfo = new ClientInfo(props.getProperty("storeName"),
                                                  props.getProperty("context"),
                                                  Integer.parseInt(props.getProperty("sequence")),
                                                  Long.parseLong(props.getProperty("bootstrapTime")),
                                                  props.getProperty("releaseVersion"),
                                                  clientConfig);
                cInfo.setUpdateTime(Long.parseLong(props.getProperty("updateTime")));
                cInfo.setDeploymentPath(props.getProperty("deploymentPath"));
                cInfo.setLocalHostName(props.getProperty("localHostName"));
                infoList.add(cInfo);
            } catch(Exception e) {
                fail("Error in retrieving Client Info: " + e);
            }
        }
        return infoList;
    }

    private void clearRegistryContent() {
        for(int i = 0; i < TOTAL_SERVERS; i++) {
            servers[i].getStoreRepository()
                      .getStorageEngine(SystemStoreConstants.SystemStoreName.voldsys$_client_registry.name())
                      .truncate();
        }
    }

    private boolean isConfigEqual(ClientConfig received, ClientConfig expected) {
        return (received.getMaxConnectionsPerNode() == expected.getMaxConnectionsPerNode()
                && received.getMaxTotalConnections() == expected.getMaxTotalConnections()
                && received.getRoutingTimeout(TimeUnit.MILLISECONDS) == expected.getRoutingTimeout(TimeUnit.MILLISECONDS)
                && received.getSocketTimeout(TimeUnit.MILLISECONDS) == expected.getSocketTimeout(TimeUnit.MILLISECONDS)
                && received.getConnectionTimeout(TimeUnit.MILLISECONDS) == expected.getConnectionTimeout(TimeUnit.MILLISECONDS)
                && received.getClientZoneId() == expected.getClientZoneId() && received.getFailureDetectorImplementation()
                                                                                       .equals(expected.getFailureDetectorImplementation()));
    }
}

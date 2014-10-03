package voldemort.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.rest.coordinator.CoordinatorServer;
import voldemort.rest.coordinator.config.ClientConfigUtil;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.restclient.RESTClientConfig;
import voldemort.restclient.admin.CoordinatorAdminClient;
import voldemort.restclient.admin.CoordinatorAdminCommand;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class CoordinatorAdminToolTest {

    private static final String STORE_NAME = "test";
    private static final String FAT_CLIENT_CONFIG_PATH = "test/common/coordinator/config/clientConfigs.avro";
    private static String storesXmlfile = "test/common/voldemort/config/two-stores.xml";

    String[] bootStrapUrls = null;
    private VoldemortServer[] servers;
    private CoordinatorServer coordinator;
    @SuppressWarnings("unused")
    private Cluster cluster;
    public static String socketUrl = "";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private CoordinatorAdminClient adminClient;
    private static final Integer SERVER_PORT = 9999;
    private static final String BOOTSTRAP_URL = "http://localhost:" + SERVER_PORT;
    private static final Integer ADMIN_PORT = 9090;
    private static final String ADMIN_URL = "http://localhost:" + ADMIN_PORT;

    @Before
    public void setUp() {
        final int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3, 4, 5, 6, 7 } };
        try {

            // Setup the cluster
            cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                            servers,
                                                            partitionMap,
                                                            socketStoreFactory,
                                                            true,
                                                            null,
                                                            storesXmlfile,
                                                            new Properties());

        } catch(IOException e) {
            fail("Failure to setup the cluster");
        }

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        List<String> bootstrapUrls = new ArrayList<String>();
        bootstrapUrls.add(socketUrl);

        // Setup the Coordinator
        CoordinatorConfig coordinatorConfig = new CoordinatorConfig();
        coordinatorConfig.setBootstrapURLs(bootstrapUrls)
                         .setCoordinatorCoreThreads(100)
                         .setCoordinatorMaxThreads(100)
                         .setFatClientConfigPath(FAT_CLIENT_CONFIG_PATH)
                         .setServerPort(SERVER_PORT)
                         .setAdminPort(ADMIN_PORT);

        try {
            coordinator = new CoordinatorServer(coordinatorConfig);
            coordinator.start();
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failure to start the Coordinator");
        }

        Properties props = new Properties();
        props.setProperty(ClientConfig.BOOTSTRAP_URLS_PROPERTY, BOOTSTRAP_URL);
        props.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, "1500");

        this.adminClient = new CoordinatorAdminClient(new RESTClientConfig(props));
    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        coordinator.stop();
    }

    @Test
    public void test() {
        String configAvro1 = "{\"test\": {\"connection_timeout_ms\": \"1111\", \"socket_timeout_ms\": \"1024\"}}";
        String configAvro2 = "{\"test\": {\"connection_timeout_ms\": \"2222\", \"socket_timeout_ms\": \"2048\"}}";
        String getConfigAvro;

        // put avro 1
        try {
            CoordinatorAdminCommand.executeCommand(new String[] { "put", "-d", configAvro1, "-u",
                    ADMIN_URL, "--confirm" });
        } catch(Exception e) {
            e.printStackTrace();
        }
        // get avro 1
        getConfigAvro = adminClient.getStoreClientConfigString(Arrays.asList(STORE_NAME), ADMIN_URL);
        // compare avro 1
        assertTrue(ClientConfigUtil.compareMultipleClientConfigAvro(configAvro1, getConfigAvro));

        // update avro 2
        try {
            CoordinatorAdminCommand.executeCommand(new String[] { "put", "-d", configAvro2, "-u",
                    ADMIN_URL, "--confirm" });
        } catch(Exception e) {
            e.printStackTrace();
        }
        // get avro 2
        getConfigAvro = adminClient.getStoreClientConfigString(Arrays.asList(STORE_NAME), ADMIN_URL);
        // compare avro 2
        assertTrue(ClientConfigUtil.compareMultipleClientConfigAvro(configAvro2, getConfigAvro));

        // delete avro 2
        try {
            CoordinatorAdminCommand.executeCommand(new String[] { "delete", "-s", STORE_NAME, "-u",
                    ADMIN_URL, "--confirm" });
        } catch(Exception e) {
            e.printStackTrace();
        }
        // get avro 2
        getConfigAvro = adminClient.getStoreClientConfigString(Arrays.asList(STORE_NAME), ADMIN_URL);
        // compare avro 2
        assertFalse(ClientConfigUtil.compareMultipleClientConfigAvro(configAvro2, getConfigAvro));
    }
}

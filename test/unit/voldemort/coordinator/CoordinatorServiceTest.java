package voldemort.coordinator;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.rest.coordinator.CoordinatorConfig;
import voldemort.rest.coordinator.CoordinatorService;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class CoordinatorServiceTest {

    private VoldemortServer[] servers;
    public static String socketUrl = "";
    private static final String STORE_NAME = "slow-store-test";
    private static final String STORES_XML = "test/common/voldemort/config/single-slow-store.xml";
    private static final String FAT_CLIENT_CONFIG_FILE_PATH = "test/common/voldemort/config/fat-client-config.avro";
    private final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                        10000,
                                                                                        100000,
                                                                                        32 * 1024);
    private CoordinatorService coordinator = null;
    private final String coordinatorURL = "http://localhost:8080";
    private final int nettyServerPort = 1234;

    @Before
    public void setUp() throws Exception {

        int numServers = 1;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 2, 4, 6, 1, 3, 5, 7 } };
        Properties props = new Properties();
        props.setProperty("storage.configs",
                          "voldemort.store.bdb.BdbStorageConfiguration,voldemort.store.slow.SlowStorageConfiguration");
        props.setProperty("testing.slow.queueing.get.ms", "4");
        props.setProperty("testing.slow.queueing.getall.ms", "4");
        props.setProperty("testing.slow.queueing.put.ms", "4");
        props.setProperty("testing.slow.queueing.delete.ms", "4");

        ServerTestUtils.startVoldemortCluster(numServers,
                                              servers,
                                              partitionMap,
                                              socketStoreFactory,
                                              true, // useNio
                                              null,
                                              STORES_XML,
                                              props);

        CoordinatorConfig config = new CoordinatorConfig();
        List<String> bootstrapUrls = new ArrayList<String>();
        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        bootstrapUrls.add(socketUrl);

        System.out.println("\n\n************************ Starting the Coordinator *************************");

        config.setBootstrapURLs(bootstrapUrls);
        config.setFatClientConfigPath(FAT_CLIENT_CONFIG_FILE_PATH);
        config.setServerPort(nettyServerPort);

        this.coordinator = new CoordinatorService(config);
        if(!this.coordinator.isStarted()) {
            this.coordinator.start();
        }
    }

    @After
    public void tearDown() throws Exception {
        if(this.socketStoreFactory != null) {
            this.socketStoreFactory.close();
        }

        if(this.coordinator != null && this.coordinator.isStarted()) {
            this.coordinator.stop();
        }
    }

    @Test
    /**
     * Tests if the coordinator started in the correct port defined by "nettyServerPort"
     */
    public void testCoordinatorPortUsed() {
        System.out.println("Starting test: is coordinator port used correctly");
        ServerSocket socket1 = null;
        try {
            socket1 = new ServerSocket();
            socket1.setReuseAddress(false);
        } catch(IOException e) {
            fail("Failed even beofre trying to bind to the coordinator port");
            e.printStackTrace();
        }
        try {
            socket1.bind(new InetSocketAddress("localhost", nettyServerPort));
        } catch(IOException e) {
            if(e instanceof BindException) {
                e.printStackTrace();
                System.out.println("Test succeeeded");
                return;
            } else {
                fail("The netty server port did not throw bind exception as expected.");
                e.printStackTrace();
            }
        }
    }

}

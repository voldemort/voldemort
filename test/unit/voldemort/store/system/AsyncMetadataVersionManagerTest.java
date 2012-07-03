package voldemort.store.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.SystemStore;
import voldemort.client.SystemStoreRepository;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.cluster.Cluster;
import voldemort.common.service.SchedulerService;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.SystemTime;
import voldemort.versioning.Versioned;

public class AsyncMetadataVersionManagerTest {

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    String[] bootStrapUrls = null;
    private String clusterXml;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;
    private long newVersion = 0;

    private SystemStore<String, Long> sysVersionStore;
    private SystemStoreRepository repository;
    private SchedulerService scheduler;
    private AsyncMetadataVersionManager asyncCheckMetadata;
    private boolean callbackDone = false;
    private long updatedStoresVersion;

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
        sysVersionStore = new SystemStore<String, Long>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version.name(),
                                                        bootStrapUrls,
                                                        this.CLIENT_ZONE_ID);
        repository = new SystemStoreRepository();
        repository.addSystemStore(sysVersionStore,
                                  SystemStoreConstants.SystemStoreName.voldsys$_metadata_version.name());
        this.scheduler = new SchedulerService(2, SystemTime.INSTANCE, true);
    }

    @After
    public void tearDown() throws Exception {
        servers[0].stop();
        servers[1].stop();
    }

    @Test
    public void testBasicAsyncBehaviour() {
        try {
            Callable<Void> rebootstrapCallback = new Callable<Void>() {

                public Void call() throws Exception {
                    callback();
                    return null;
                }
            };

            // Write a base version of 100
            this.sysVersionStore.putSysStore(AsyncMetadataVersionManager.STORES_VERSION_KEY, 100l);

            // Giving enough time to complete the above put.
            Thread.sleep(500);

            // Starting the Version Metadata Manager
            this.asyncCheckMetadata = new AsyncMetadataVersionManager(this.repository,
                                                                      rebootstrapCallback);
            scheduler.schedule(asyncCheckMetadata.getClass().getName(),
                               asyncCheckMetadata,
                               new Date(),
                               500);

            // Wait until the Version Manager is active
            int maxRetries = 0;
            while(maxRetries < 3 && !asyncCheckMetadata.isActive) {
                Thread.sleep(500);
                maxRetries++;
            }

            // Updating the version metadata here for the Version Metadata
            // Manager to detect
            this.newVersion = 101;
            this.sysVersionStore.putSysStore(AsyncMetadataVersionManager.STORES_VERSION_KEY,
                                             this.newVersion);

            maxRetries = 0;
            while(maxRetries < 3 && !callbackDone) {
                Thread.sleep(2000);
                maxRetries++;
            }

            assertEquals(this.updatedStoresVersion, this.newVersion);
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    private void callback() {
        try {
            Versioned<Long> storeVersion = this.asyncCheckMetadata.getStoreMetadataVersion();
            if(storeVersion != null) {
                this.updatedStoresVersion = storeVersion.getValue();
            }
        } catch(Exception e) {
            fail("Error in updating stores.xml version: " + e.getMessage());
        } finally {
            this.callbackDone = true;
        }
    }
}

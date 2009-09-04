package voldemort.server.storage;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;
import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortMetadata;
import voldemort.server.scheduler.SchedulerService;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Test that the storage service is able to load all stores.
 * 
 * @author jay
 * 
 */
public class StorageServiceTest extends TestCase {

    private Cluster cluster;
    private StoreRepository storeRepository;
    private StorageService storage;
    private SchedulerService scheduler;
    private List<StoreDefinition> storeDefs;

    @Override
    public void setUp() {
        File temp = TestUtils.createTempDir();
        VoldemortConfig config = new VoldemortConfig(0, temp.getAbsolutePath());
        new File(config.getMetadataDirectory()).mkdir();
        config.setBdbCacheSize(100000);
        this.scheduler = new SchedulerService(1, new MockTime());
        this.cluster = ServerTestUtils.getLocalCluster(1);
        this.storeDefs = ServerTestUtils.getStoreDefs(2);
        this.storeRepository = new StoreRepository();
        MetadataStore mdStore = MetadataStore.readFromDirectory(new File(config.getMetadataDirectory()));
        mdStore.put(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()),
                    new Versioned<byte[]>(cluster.toString().getBytes()));
        mdStore.put(new ByteArray(MetadataStore.STORES_KEY.getBytes()),
                    new Versioned<byte[]>(storeDefs.toString().getBytes()));
        FailureDetector failureDetector = FailureDetectorUtils.create(config,
                                                                                                 storeRepository);
        storage = new StorageService(storeRepository,
                                     new VoldemortMetadata(cluster, storeDefs, 0),
                                     scheduler,
                                     config,
                                     failureDetector);
        storage.start();
    }

    public void testMetadataStore() {
        assertNotNull(storage.getMetadataStore());
    }

    public void testStores() {
        StoreRepository repo = storage.getStoreRepository();
        for(StoreDefinition def: storeDefs) {
            // test local stores
            assertTrue("Missing local store '" + def.getName() + "'.",
                       repo.hasLocalStore(def.getName()));
            assertEquals(def.getName(), repo.getLocalStore(def.getName()).getName());

            assertTrue("Missing storage engine '" + def.getName() + "'.",
                       repo.hasStorageEngine(def.getName()));
            assertEquals(def.getName(), repo.getStorageEngine(def.getName()).getName());

            for(int node = 0; node < cluster.getNumberOfNodes(); node++) {
                assertTrue("Missing node store '" + def.getName() + "'.",
                           repo.hasNodeStore(def.getName(), node));
                assertEquals(def.getName(), repo.getNodeStore(def.getName(), node).getName());
            }
        }
    }
}

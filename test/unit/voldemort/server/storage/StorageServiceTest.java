package voldemort.server.storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;
import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.common.service.SchedulerService;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Test that the storage service is able to load all stores.
 * 
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
        MetadataStore mdStore = ServerTestUtils.createMetadataStore(cluster, storeDefs);
        storage = new StorageService(storeRepository, mdStore, scheduler, config);
        storage.start();
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

    public void testMetadataVersionsInit() {
        Store<ByteArray, byte[], byte[]> versionStore = storeRepository.getLocalStore(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        Properties props = new Properties();

        try {
            ByteArray metadataVersionsKey = new ByteArray(StorageService.VERSIONS_METADATA_STORE.getBytes());
            List<Versioned<byte[]>> versionList = versionStore.get(metadataVersionsKey, null);

            if(versionList != null && versionList.size() > 0) {
                byte[] versionsByteArray = versionList.get(0).getValue();
                if(versionsByteArray != null) {
                    props.load(new ByteArrayInputStream(versionsByteArray));
                } else {
                    fail("Illegal value returned for metadata key: "
                         + StorageService.VERSIONS_METADATA_STORE);
                }
            } else {
                fail("Illegal value returned for metadata key: "
                     + StorageService.VERSIONS_METADATA_STORE);
            }

            // Check if version exists for cluster.xml
            if(!props.containsKey(StorageService.CLUSTER_VERSION_KEY)) {
                fail(StorageService.CLUSTER_VERSION_KEY + " not present in "
                     + StorageService.VERSIONS_METADATA_STORE);
            }

            // Check if version exists for stores.xml
            if(!props.containsKey(StorageService.STORES_VERSION_KEY)) {
                fail(StorageService.STORES_VERSION_KEY + " not present in "
                     + StorageService.VERSIONS_METADATA_STORE);
            }

            // Check if version exists for each store
            for(StoreDefinition def: storeDefs) {
                if(!props.containsKey(def.getName())) {
                    fail(def.getName() + " store not present in "
                         + StorageService.VERSIONS_METADATA_STORE);
                }
            }
        } catch(Exception e) {
            fail("Error in retrieving : "
                 + StorageService.VERSIONS_METADATA_STORE
                 + " key from "
                 + SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name()
                 + " store. ");
        }
    }
}

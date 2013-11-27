package voldemort.server.storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
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
@RunWith(Parameterized.class)
public class StorageServiceTest extends TestCase {

    private Cluster cluster;
    private StoreRepository storeRepository;
    private StorageService storage;
    private SchedulerService scheduler;
    private List<StoreDefinition> storeDefs;
    private MetadataStore mdStore;
    private VoldemortConfig config;

    public StorageServiceTest(Cluster cluster,
                              List<StoreDefinition> storeDefs,
                              MetadataStore mdStore,
                              VoldemortConfig config) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
        this.mdStore = mdStore;
        this.config = config;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                {
                        ServerTestUtils.getLocalCluster(1),
                        ServerTestUtils.getStoreDefs(2),
                        ServerTestUtils.createMetadataStore(ServerTestUtils.getLocalCluster(1),
                                                            ServerTestUtils.getStoreDefs(2)),
                        new VoldemortConfig(0, TestUtils.createTempDir().getAbsolutePath()) },
                {
                        ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                        ClusterTestUtils.getZ1Z3StoreDefsInMemory(),
                        ServerTestUtils.createMetadataStore(ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds(),
                                                            ClusterTestUtils.getZ1Z3StoreDefsInMemory(),
                                                            3),
                        new VoldemortConfig(3, TestUtils.createTempDir().getAbsolutePath()) } });
    }

    @Override
    @Before
    public void setUp() {
        config.setEnableServerRouting(true); // this is turned off by default
        new File(config.getMetadataDirectory()).mkdir();
        config.setBdbCacheSize(100000);
        this.scheduler = new SchedulerService(1, new MockTime());
        this.storeRepository = new StoreRepository();
        storage = new StorageService(storeRepository, mdStore, scheduler, config);
        storage.start();
    }

    @Test
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

            Integer[] nodeIds = cluster.getNodeIds().toArray(new Integer[0]);

            for (int index = 0; index < cluster.getNumberOfNodes(); index++) {
                assertTrue("Missing node store '" + def.getName() + "'.",
                           repo.hasNodeStore(def.getName(), nodeIds[index]));
                assertEquals(def.getName(), repo.getNodeStore(def.getName(), nodeIds[index])
                                                .getName());
            }
        }
    }

    @Test
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

package voldemort.client.rebalance;

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.SystemStore;
import voldemort.client.SystemStoreRepository;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.scheduler.AsyncMetadataVersionManager;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.SchedulerService;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.SystemTime;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * We simulate the rebalance controller here by changing the cluster state and
 * stores state
 * 
 * On rebootstrap we want to ensure that the cluster and store defs are
 * consistent from a client's perspective
 * 
 */
public class RebalanceRebootstrapConsistencyTest {

    private Cluster cluster;
    private List<VoldemortServer> servers;

    String[] bootStrapUrls = null;
    public static String socketUrl = "";
    protected final int CLIENT_ZONE_ID = 0;

    private SystemStore<String, String> sysVersionStore;
    private SystemStoreRepository repository;
    private SchedulerService scheduler;
    private AsyncMetadataVersionManager asyncCheckMetadata;
    private boolean callbackDone = false;

    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";

    private static final String CLUSTER_VERSION_KEY = "cluster.xml";
    int maxRetries = 0;

    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    AdminClient adminClient;

    List<StoreDefinition> newstoredefs;
    Cluster newCluster;

    @Before
    public void setUp() throws Exception {
        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);

        int numServers = 2;

        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();

        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(2)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();

        List<StoreDefinition> storedefs = new ArrayList<StoreDefinition>();

        storedefs.add(rwStoreDefWithReplication);

        String storesXmlStr = new StoreDefinitionsMapper().writeStoreList(storedefs);

        // create a temp file
        File tempStoresXml = File.createTempFile("tempfile", ".tmp");

        BufferedWriter bw = new BufferedWriter(new FileWriter(tempStoresXml));
        bw.write(storesXmlStr);
        bw.close();

        VoldemortServer[] voldemortServers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1 }, {} };
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        voldemortServers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        false,
                                                        null,
                                                        tempStoresXml.getAbsolutePath(),
                                                        new Properties());

        servers = Lists.newArrayList();
        for(int i = 0; i < numServers; ++i) {
            servers.add(voldemortServers[i]);
        }

        socketUrl = voldemortServers[0].getIdentityNode().getSocketUrl().toString();

        bootStrapUrls = new String[1];
        bootStrapUrls[0] = socketUrl;
        sysVersionStore = new SystemStore<String, String>(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                          bootStrapUrls,
                                                          this.CLIENT_ZONE_ID);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(bootStrapUrls).setClientZoneId(this.CLIENT_ZONE_ID);
        repository = new SystemStoreRepository(clientConfig);
        repository.addSystemStore(sysVersionStore,
                                  SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());
        this.scheduler = new SchedulerService(2, SystemTime.INSTANCE, true);

        Callable<Void> rebootstrapCallback = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                // callbackForClusterChange();
                checkConsistentMetadata();
                return null;
            }
        };

        // Starting the Version Metadata Manager
        this.asyncCheckMetadata = new AsyncMetadataVersionManager(this.repository,
                                                                  rebootstrapCallback,
                                                                  null);
        scheduler.schedule(asyncCheckMetadata.getClass().getName(),
                           asyncCheckMetadata,
                           new Date(),
                           500);

        // Wait until the Version Manager is active

        while(maxRetries < 3 && !asyncCheckMetadata.isActive) {
            Thread.sleep(500);
            maxRetries++;
        }

    }

    @After
    public void tearDown() {
        if(servers != null)
            for(VoldemortServer server: servers)
                server.stop();
    }

    /*
     * simulate rebalance behavior
     */
    public void rebalance() {
        assert servers != null && servers.size() > 1;

        VoldemortConfig config = servers.get(0).getVoldemortConfig();
        adminClient = RebalanceUtils.createTempAdminClient(config, cluster, 4);
        HashMap<Integer, List<Integer>> replicaToPartitionList = Maps.newHashMap();
        replicaToPartitionList.put(0, ImmutableList.of(0, 1));
        int req = adminClient.storeMntOps.migratePartitions(0,
                                                            1,
                                                            testStoreNameRW,
                                                            replicaToPartitionList,
                                                            null,
                                                            null,
                                                            false);
        adminClient.rpcOps.waitForCompletion(1, req, 5, TimeUnit.SECONDS);
        Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(0);

        Node node0 = versionedCluster.getValue().getNodeById(0);
        Node node1 = versionedCluster.getValue().getNodeById(1);
        Node newNode0 = new Node(node0.getId(),
                                 node0.getHost(),
                                 node0.getHttpPort(),
                                 node0.getSocketPort(),
                                 node0.getAdminPort(),
                                 ImmutableList.<Integer> of());
        Node newNode1 = new Node(node1.getId(),
                                 node1.getHost(),
                                 node1.getHttpPort(),
                                 node1.getSocketPort(),
                                 node1.getAdminPort(),
                                 ImmutableList.of(0, 1));
        adminClient.storeMntOps.deletePartitions(0, testStoreNameRW, ImmutableList.of(0, 1), null);

        newCluster = new Cluster(cluster.getName(),
                                 ImmutableList.of(newNode0, newNode1),
                                 Lists.newArrayList(cluster.getZones()));

        newstoredefs = new ArrayList<StoreDefinition>();

        newstoredefs.add(rwStoreDefWithReplication2);
        for(Node node: cluster.getNodes()) {
            VectorClock clock = (VectorClock) versionedCluster.getVersion();
            clock.incrementVersion(node.getId(), System.currentTimeMillis());

            adminClient.metadataMgmtOps.updateRemoteMetadata(node.getId(),
                                                             MetadataStore.STORES_KEY,
                                                             new Versioned<String>(storeMapper.writeStoreList(newstoredefs),
                                                                                   clock));

            adminClient.metadataMgmtOps.updateRemoteMetadata(node.getId(),
                                                             MetadataStore.CLUSTER_KEY,
                                                             new Versioned<String>(clusterMapper.writeCluster(newCluster),
                                                                                   clock));

        }

        adminClient.metadataMgmtOps.updateMetadataversion(CLUSTER_VERSION_KEY);

    }

    @Test
    public void testBasicAsyncBehaviour() {

        try {

            rebalance();
            maxRetries = 0;
            while(maxRetries < 3 && !callbackDone) {
                Thread.sleep(2000);
                maxRetries++;
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail("Failed to start the Metadata Version Manager : " + e.getMessage());
        }
    }

    /*
     * In callback ensure metadata is consistent
     */
    private void checkConsistentMetadata() {

        Versioned<Cluster> versionedCluster = adminClient.metadataMgmtOps.getRemoteCluster(0);
        Versioned<List<StoreDefinition>> versionedStoreDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);

        if(versionedCluster.getValue().equals(newCluster)) {
            Assert.assertEquals(versionedStoreDefs.getValue().get(0), rwStoreDefWithReplication2);
        }
    }

}

/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.rebalance;

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
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
import voldemort.client.SystemStoreClient;
import voldemort.client.SystemStoreClientFactory;
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
import voldemort.utils.SystemTime;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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

    private SystemStoreClient<String, String> sysVersionStore;
    private SystemStoreRepository repository;
    private SchedulerService scheduler;
    private AsyncMetadataVersionManager asyncCheckMetadata;
    private boolean callbackDone = false;

    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    protected static String testStoreNameRW = "test";

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

        /*
         * Bug fix: The old code was trying to rename a store during rebalance !
         * God knows why Renaming it back to the original store name and
         * changing other preferences (required reads = 2)
         */
        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(2)
                                                                 .setPreferredReads(2)
                                                                 .setRequiredReads(2)
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

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapUrls(bootStrapUrls).setClientZoneId(this.CLIENT_ZONE_ID);
        SystemStoreClientFactory<String, String> systemStoreFactory = new SystemStoreClientFactory<String, String>(clientConfig);
        sysVersionStore = systemStoreFactory.createSystemStore(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name());

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
        adminClient = AdminClient.createTempAdminClient(config, cluster, 4);
        List<Integer> partitionIds = ImmutableList.of(0, 1);
        int req = adminClient.storeMntOps.migratePartitions(0,
                                                            1,
                                                            testStoreNameRW,
                                                            partitionIds,
                                                            null,
                                                            null);
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

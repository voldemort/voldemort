package voldemort.client.rebalance;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.grandfather.GrandfatherState;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MigratePartitionsTest {

    private final int NUM_NODES = 2;
    private final int NUM_ZONES = 2;
    private static int TEST_VALUES_SIZE = 1000;
    private VoldemortServer voldemortServer[];

    private Cluster consistentRoutingCluster;
    private Cluster zoneRoutingCluster;
    private Cluster zoneRoutingClusterModified;
    private StoreDefinition beforeStoreDef;
    private StoreDefinition afterStoreDef;
    private AdminClient adminClient;
    private VoldemortConfig voldemortConfig;
    private RoutingStrategy routingStrategy;

    @Before
    public void setUp() throws IOException {
        List<Node> nodes = Lists.newArrayList();
        List<Node> zoneNodes = Lists.newArrayList();
        int[] freePorts = ServerTestUtils.findFreePorts(3 * NUM_NODES);
        List<Zone> zones = Lists.newArrayList();
        for(int i = 0; i < NUM_ZONES; i++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList();
            int zoneId = i + 1;
            for(int j = 0; j < NUM_ZONES; j++) {
                proximityList.add(zoneId % NUM_ZONES);
                zoneId++;
            }
            zones.add(new Zone(i, proximityList));
        }

        for(int i = 0; i < NUM_NODES; i++) {
            nodes.add(new Node(i,
                               "localhost",
                               freePorts[3 * i],
                               freePorts[3 * i + 1],
                               freePorts[3 * i + 2],
                               Lists.newArrayList(i, i + NUM_NODES)));
            if(i < NUM_NODES / 2)
                zoneNodes.add(new Node(i,
                                       "localhost",
                                       freePorts[3 * i],
                                       freePorts[3 * i + 1],
                                       freePorts[3 * i + 2],
                                       0,
                                       Lists.newArrayList(i, i + NUM_NODES)));
            else
                zoneNodes.add(new Node(i,
                                       "localhost",
                                       freePorts[3 * i],
                                       freePorts[3 * i + 1],
                                       freePorts[3 * i + 2],
                                       1,
                                       Lists.newArrayList(i, i + NUM_NODES)));
        }
        consistentRoutingCluster = new Cluster("consistent", nodes);
        zoneRoutingCluster = new Cluster("zone1", zoneNodes, zones);

        nodes = Lists.newArrayList(RebalanceUtils.createUpdatedCluster(consistentRoutingCluster,
                                                                       nodes.get(NUM_NODES - 1), // last
                                                                       // node
                                                                       nodes.get(0), // first
                                                                       // node
                                                                       Lists.newArrayList(0))
                                                 .getNodes());

        zoneNodes = Lists.newArrayList();
        for(Node node: nodes) {
            if(node.getId() < NUM_NODES / 2)
                zoneNodes.add(new Node(node.getId(),
                                       node.getHost(),
                                       node.getHttpPort(),
                                       node.getSocketPort(),
                                       node.getAdminPort(),
                                       0,
                                       node.getPartitionIds()));
            else
                zoneNodes.add(new Node(node.getId(),
                                       node.getHost(),
                                       node.getHttpPort(),
                                       node.getSocketPort(),
                                       node.getAdminPort(),
                                       1,
                                       node.getPartitionIds()));
        }
        zoneRoutingClusterModified = new Cluster("zone2", zoneNodes, zones);
        HashMap<Integer, Integer> zoneReplicationFactors = Maps.newHashMap();
        for(int zoneIds = 0; zoneIds < NUM_ZONES; zoneIds++) {
            zoneReplicationFactors.put(zoneIds, 1);
        }
        beforeStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     1,
                                                     RoutingStrategyType.CONSISTENT_STRATEGY);
        afterStoreDef = ServerTestUtils.getStoreDef("consistent_to_zone_store",
                                                    2,
                                                    1,
                                                    1,
                                                    1,
                                                    0,
                                                    0,
                                                    zoneReplicationFactors,
                                                    HintedHandoffStrategyType.PROXIMITY_STRATEGY,
                                                    RoutingStrategyType.ZONE_STRATEGY);
        voldemortServer = new VoldemortServer[NUM_NODES];
        routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(beforeStoreDef,
                                                                             consistentRoutingCluster);

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            voldemortServer[nodeId] = startServer(nodeId,
                                                  Lists.newArrayList(beforeStoreDef),
                                                  consistentRoutingCluster);
        }
        voldemortConfig = ServerTestUtils.getVoldemortConfig();
        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           consistentRoutingCluster,
                                                           1,
                                                           1);
    }

    private VoldemortServer startServer(int nodeId, List<StoreDefinition> storeDef, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(false,
                                                                            nodeId,
                                                                            TestUtils.createTempDir()
                                                                                     .getAbsolutePath(),
                                                                            cluster,
                                                                            storeDef,
                                                                            new Properties());
        config.setGrandfather(true);
        VoldemortServer server = new VoldemortServer(config);
        server.start();
        return server;
    }

    @After
    public void tearDown() {
        try {
            for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
                if(voldemortServer[nodeId] != null)
                    voldemortServer[nodeId].stop();
            }
        } catch(Exception e) {
            // ignore exceptions here
        }

    }

    @Test
    public void testDelete() throws InterruptedException {
        // First test will have a migration plan for one partition
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(NUM_NODES - 1,
                                                                   0,
                                                                   Lists.newArrayList(0),
                                                                   new ArrayList<Integer>(),
                                                                   new ArrayList<Integer>(),
                                                                   new ArrayList<String>(),
                                                                   new HashMap<String, String>(),
                                                                   new HashMap<String, String>(),
                                                                   0);
        voldemortServer[0].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                                  VoldemortState.GRANDFATHERING_SERVER);
        voldemortServer[0].getMetadataStore().put(MetadataStore.GRANDFATHERING_INFO,
                                                  new GrandfatherState(Lists.newArrayList(info)));

        // Random key-values
        Map<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        Store<ByteArray, byte[], byte[]> store = voldemortServer[0].getStoreRepository()
                                                                   .getLocalStore("consistent_to_zone_store");

        Map<ByteArray, byte[]> entriesMigrating = Maps.newHashMap();
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routingStrategy.getPartitionList(entry.getKey().get());
            if(hasOverLap(partitions, consistentRoutingCluster.getNodeById(0).getPartitionIds())) {
                VectorClock vectorClock = new VectorClock();
                vectorClock.incrementVersion(0, System.currentTimeMillis());
                store.delete(entry.getKey(), vectorClock);
            }
            if(partitions.contains(0))
                entriesMigrating.put(entry.getKey(), entry.getValue());
        }

        // Check that no slops made through
        StorageEngine<ByteArray, Slop, byte[]> slopEngine = voldemortServer[0].getStoreRepository()
                                                                              .getSlopStore()
                                                                              .asSlopStore();
        Thread.sleep(1000);
        checkSlopStore(slopEngine, entriesMigrating, Slop.Operation.DELETE);
    }

    @Test
    public void testPutsWithPlan() throws InterruptedException {
        // First test will have a migration plan for one partition
        RebalancePartitionsInfo info = new RebalancePartitionsInfo(NUM_NODES - 1,
                                                                   0,
                                                                   Lists.newArrayList(0),
                                                                   new ArrayList<Integer>(),
                                                                   new ArrayList<Integer>(),
                                                                   new ArrayList<String>(),
                                                                   new HashMap<String, String>(),
                                                                   new HashMap<String, String>(),
                                                                   0);
        voldemortServer[0].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                                  VoldemortState.GRANDFATHERING_SERVER);
        voldemortServer[0].getMetadataStore().put(MetadataStore.GRANDFATHERING_INFO,
                                                  new GrandfatherState(Lists.newArrayList(info)));

        // Random key-values
        Map<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        Store<ByteArray, byte[], byte[]> store = voldemortServer[0].getStoreRepository()
                                                                   .getLocalStore("consistent_to_zone_store");

        Map<ByteArray, byte[]> entriesMigrating = Maps.newHashMap();
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routingStrategy.getPartitionList(entry.getKey().get());
            if(hasOverLap(partitions, consistentRoutingCluster.getNodeById(0).getPartitionIds())) {
                VectorClock vectorClock = new VectorClock();
                vectorClock.incrementVersion(0, System.currentTimeMillis());
                store.put(entry.getKey(), Versioned.value(entry.getValue(), vectorClock), null);
            }
            if(partitions.contains(0))
                entriesMigrating.put(entry.getKey(), entry.getValue());
        }

        // Check that no slops made through
        StorageEngine<ByteArray, Slop, byte[]> slopEngine = voldemortServer[0].getStoreRepository()
                                                                              .getSlopStore()
                                                                              .asSlopStore();
        Thread.sleep(1000);
        checkSlopStore(slopEngine, entriesMigrating, Slop.Operation.PUT);

        // Second test - Move all partitions on node 0 to node NUM_NODES-1
        info = new RebalancePartitionsInfo(NUM_NODES - 1,
                                           0,
                                           consistentRoutingCluster.getNodeById(0)
                                                                   .getPartitionIds(),
                                           new ArrayList<Integer>(),
                                           new ArrayList<Integer>(),
                                           new ArrayList<String>(),
                                           new HashMap<String, String>(),
                                           new HashMap<String, String>(),
                                           0);
        voldemortServer[0].getMetadataStore().put(MetadataStore.GRANDFATHERING_INFO,
                                                  new GrandfatherState(Lists.newArrayList(info)));

        entriesMigrating = Maps.newHashMap();
        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routingStrategy.getPartitionList(entry.getKey().get());
            if(hasOverLap(partitions, consistentRoutingCluster.getNodeById(0).getPartitionIds())) {
                VectorClock vectorClock = new VectorClock();
                vectorClock.incrementVersion(0, System.currentTimeMillis());
                try {
                    store.put(entry.getKey(), Versioned.value(entry.getValue(), vectorClock), null);
                } catch(ObsoleteVersionException e) {}
                entriesMigrating.put(entry.getKey(), entry.getValue());
            }
        }
        Thread.sleep(1000);
        checkSlopStore(slopEngine, entriesMigrating, Slop.Operation.PUT);
    }

    private void checkSlopStore(StorageEngine<ByteArray, Slop, byte[]> slopEngine,
                                Map<ByteArray, byte[]> entriesMigrating,
                                Slop.Operation operation) {
        int count = 0;
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;
        try {
            iterator = slopEngine.entries();
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<Slop>> slopStored = iterator.next();
                Slop slop = slopStored.getSecond().getValue();
                Assert.assertEquals(slop.getNodeId(), NUM_NODES - 1);
                Assert.assertEquals(slop.getStoreName(), "consistent_to_zone_store");
                Assert.assertEquals(slop.getOperation(), operation);
                if(Slop.Operation.PUT.equals(operation))
                    Assert.assertEquals(ByteUtils.compare(slop.getValue(),
                                                          entriesMigrating.get(slop.getKey())), 0);
                count++;
                // Clear up slop for next test
                slopEngine.delete(slopStored.getFirst(), slopStored.getSecond().getVersion());
            }
            Assert.assertEquals(count, entriesMigrating.size());
        } finally {
            if(iterator != null)
                iterator.close();
        }
    }

    @Test
    public void testPutsWithNoPlan() {

        // First test will put one server in grandfather state forcefully. The
        // plan should have no migration for any of the partitions. In other
        // words, no slops should be stored
        voldemortServer[0].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                                  VoldemortState.GRANDFATHERING_SERVER);
        voldemortServer[0].getMetadataStore()
                          .put(MetadataStore.GRANDFATHERING_INFO,
                               new GrandfatherState(new ArrayList<RebalancePartitionsInfo>()));

        // Random key-values
        Map<ByteArray, byte[]> entryMap = ServerTestUtils.createRandomKeyValuePairs(TEST_VALUES_SIZE);

        Store<ByteArray, byte[], byte[]> store = voldemortServer[0].getStoreRepository()
                                                                   .getLocalStore("consistent_to_zone_store");

        for(Entry<ByteArray, byte[]> entry: entryMap.entrySet()) {
            List<Integer> partitions = routingStrategy.getPartitionList(entry.getKey().get());
            if(hasOverLap(partitions, consistentRoutingCluster.getNodeById(0).getPartitionIds()))
                store.put(entry.getKey(),
                          Versioned.value(entry.getValue(),
                                          new VectorClock().incremented(0,
                                                                        System.currentTimeMillis())),
                          null);
        }

        // Check that no slops made through
        StorageEngine<ByteArray, Slop, byte[]> slopEngine = voldemortServer[0].getStoreRepository()
                                                                              .getSlopStore()
                                                                              .asSlopStore();
        int count = 0;
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = slopEngine.entries();
        while(iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assert.assertEquals(count, 0);

    }

    private boolean hasOverLap(List<Integer> list1, List<Integer> list2) {
        List<Integer> list1Replica = Lists.newArrayList(list1);
        list1Replica.retainAll(list2);
        if(list1Replica.size() != 0)
            return true;
        else
            return false;
    }

    @Test
    public void testStateChange() {
        // Set one of them in GRANDFATHER state in advance and check if finally
        // everyone is finally in NORMAL state
        voldemortServer[NUM_NODES - 1].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                                              VoldemortState.GRANDFATHERING_SERVER);
        MigratePartitions tool = new MigratePartitions(consistentRoutingCluster,
                                                       zoneRoutingCluster,
                                                       Lists.newArrayList(beforeStoreDef),
                                                       Lists.newArrayList(afterStoreDef),
                                                       adminClient,
                                                       voldemortConfig,
                                                       null);
        try {
            tool.migrate();
            fail("Should have failed due to one node being in grandfathering state already");
        } catch(Exception e) {}

        for(int nodeId = 0; nodeId < NUM_NODES - 1; nodeId++) {
            Assert.assertEquals(voldemortServer[nodeId].getMetadataStore()
                                                       .getServerState()
                                                       .toString(),
                                MetadataStore.VoldemortState.NORMAL_SERVER.toString());
        }
        Assert.assertEquals(voldemortServer[NUM_NODES - 1].getMetadataStore()
                                                          .getServerState()
                                                          .toString(),
                            MetadataStore.VoldemortState.GRANDFATHERING_SERVER.toString());

        // Set all nodes back to NORMAL state and try a normal change of state.
        // No change in cluster, only store definition changes
        voldemortServer[NUM_NODES - 1].getMetadataStore().put(MetadataStore.SERVER_STATE_KEY,
                                                              VoldemortState.NORMAL_SERVER);

        // Get server state for all nodes
        HashMap<Integer, VectorClock> serverVersions = Maps.newHashMap();

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            VectorClock currentClock = (VectorClock) voldemortServer[nodeId].getMetadataStore()
                                                                            .get(MetadataStore.SERVER_STATE_KEY,
                                                                                 null)
                                                                            .get(0)
                                                                            .getVersion();
            serverVersions.put(nodeId, currentClock);
        }
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingCluster,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     adminClient,
                                     voldemortConfig,
                                     null);
        tool.migrate();

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            VectorClock currentClock = (VectorClock) voldemortServer[nodeId].getMetadataStore()
                                                                            .get(MetadataStore.SERVER_STATE_KEY,
                                                                                 null)
                                                                            .get(0)
                                                                            .getVersion();

            Assert.assertEquals(serverVersions.get(nodeId).getMaxVersion() + 2,
                                currentClock.getMaxVersion());
        }

    }

    /**
     * To test whether we are generating the correct donor node plans from the
     * stealer node plans generated from RebalanceClusterPlan
     * 
     */
    @Test
    public void testDonorNodePlanGeneration() {

        // Stealer node - 0
        MigratePartitions tool = new MigratePartitions(consistentRoutingCluster,
                                                       zoneRoutingClusterModified,
                                                       Lists.newArrayList(beforeStoreDef),
                                                       Lists.newArrayList(afterStoreDef),
                                                       adminClient,
                                                       voldemortConfig,
                                                       Lists.newArrayList(0));
        HashMap<Integer, List<RebalancePartitionsInfo>> donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 1);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);

        // Stealer node - 1
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     adminClient,
                                     voldemortConfig,
                                     Lists.newArrayList(1));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 1);
        Assert.assertEquals(donorNodePlans.get(0).size(), 1);

        // Stealer node - 0, -1
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     adminClient,
                                     voldemortConfig,
                                     Lists.newArrayList(0, -1));
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 1);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);

        // Stealer node - null = 0, 1
        tool = new MigratePartitions(consistentRoutingCluster,
                                     zoneRoutingClusterModified,
                                     Lists.newArrayList(beforeStoreDef),
                                     Lists.newArrayList(afterStoreDef),
                                     adminClient,
                                     voldemortConfig,
                                     null);
        donorNodePlans = tool.getDonorNodePlan();
        Assert.assertEquals(donorNodePlans.size(), 2);
        Assert.assertEquals(donorNodePlans.get(0).size(), 1);
        Assert.assertEquals(donorNodePlans.get(1).size(), 1);
    }

}

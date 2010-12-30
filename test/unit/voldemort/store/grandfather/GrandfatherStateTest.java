package voldemort.store.grandfather;

import static org.junit.Assert.assertEquals;

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
import voldemort.client.rebalance.RebalancePartitionsInfo;
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
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.slop.Slop;
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
import com.google.common.collect.Sets;

public class GrandfatherStateTest {

    private final int NUM_NODES = 2;
    private final int NUM_PARTITIONS_PER_NODE = 4;
    private final int NUM_ZONES = 2;
    private static int TEST_VALUES_SIZE = 1000;
    private VoldemortServer voldemortServer[];

    private Cluster consistentRoutingCluster;
    private StoreDefinition beforeStoreDef;
    private RoutingStrategy routingStrategy;

    private List<RebalancePartitionsInfo> dummyPartitionsInfo(int numberOfStealerNodes,
                                                              int numberOfPartitionsPerNode,
                                                              boolean includeDuplicate) {

        List<RebalancePartitionsInfo> partitionsInfo = Lists.newArrayList();
        int partitionId = 0;
        int endPartitionId = numberOfStealerNodes * numberOfPartitionsPerNode - 1;

        for(int stealerNodeId = 0; stealerNodeId < numberOfStealerNodes; stealerNodeId++) {
            List<Integer> partitionIds = Lists.newArrayList();
            for(int i = 0; i < numberOfPartitionsPerNode; i++) {
                partitionIds.add(partitionId);
                if(includeDuplicate) {
                    partitionIds.add(endPartitionId);
                    endPartitionId--;
                }
                partitionId++;
            }
            partitionsInfo.add(new RebalancePartitionsInfo(stealerNodeId,
                                                           0,
                                                           partitionIds,
                                                           new ArrayList<Integer>(),
                                                           new ArrayList<Integer>(),
                                                           new ArrayList<String>(),
                                                           new HashMap<String, String>(),
                                                           new HashMap<String, String>(),
                                                           0));
        }
        return partitionsInfo;
    }

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
        voldemortServer = new VoldemortServer[NUM_NODES];
        routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(beforeStoreDef,
                                                                             consistentRoutingCluster);

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            voldemortServer[nodeId] = startServer(nodeId,
                                                  Lists.newArrayList(beforeStoreDef),
                                                  consistentRoutingCluster);
        }
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
    public void testPartitionToNodeMapping() {
        List<RebalancePartitionsInfo> partitionsInfo = dummyPartitionsInfo(NUM_NODES,
                                                                           NUM_PARTITIONS_PER_NODE,
                                                                           false);
        GrandfatherState state = new GrandfatherState(partitionsInfo);

        for(int partitionId = 0; partitionId < NUM_NODES * NUM_PARTITIONS_PER_NODE; partitionId++) {
            assertEquals(state.findNodeIds(partitionId).size(), 1);
            assertEquals(state.findNodeIds(partitionId), Sets.newHashSet(partitionId
                                                                         / NUM_PARTITIONS_PER_NODE));
        }

        partitionsInfo = dummyPartitionsInfo(NUM_NODES, NUM_PARTITIONS_PER_NODE, true);
        state = new GrandfatherState(partitionsInfo);

        for(int partitionId = 0; partitionId < NUM_NODES * NUM_PARTITIONS_PER_NODE; partitionId++) {
            assertEquals(state.findNodeIds(partitionId),
                         Sets.newHashSet(partitionId / NUM_PARTITIONS_PER_NODE,
                                         NUM_NODES - (partitionId / NUM_PARTITIONS_PER_NODE) - 1));
        }

    }
}

package voldemort.store.routed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static voldemort.VoldemortTestConstants.getEightNodeClusterWithZones;
import static voldemort.VoldemortTestConstants.getFourNodeClusterWithZones;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.NoopFailureDetector;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class GetallNodeReachTest {

    private Cluster cluster;
    private StoreDefinition storeDef;
    private RoutedStore store;
    Map<Integer, Store<ByteArray, byte[], byte[]>> subStores;

    @Before
    public void setUp() throws Exception {}

    private void makeStore() {
        subStores = Maps.newHashMap();
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");
            subStores.put(n.getId(), subStore);
        }
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(Executors.newFixedThreadPool(2),
                                                                       new TimeoutConfig(1000L,
                                                                                         false));

        store = routedStoreFactory.create(cluster, storeDef, subStores, new NoopFailureDetector());
    }

    @Test
    public void testGetallTouchOneZone() throws Exception {
        cluster = getFourNodeClusterWithZones();
        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);
        storeDef = new StoreDefinitionBuilder().setName("test")
                                               .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                               .setReplicationFactor(4)
                                               .setZoneReplicationFactor(zoneReplicationFactor)
                                               .setKeySerializer(new SerializerDefinition("string"))
                                               .setValueSerializer(new SerializerDefinition("string"))
                                               .setPreferredReads(2)
                                               .setRequiredReads(1)
                                               .setPreferredWrites(1)
                                               .setRequiredWrites(1)
                                               .setZoneCountReads(0)
                                               .setZoneCountWrites(0)
                                               .build();
        makeStore();
        Versioned<byte[]> v = Versioned.value("v".getBytes());
        subStores.get(0).put(TestUtils.toByteArray("k011_zone0_only"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k011_zone0_only"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k100_zone1_only"), v, null);
        /* test single key getall */
        List<ByteArray> keys011 = new ArrayList<ByteArray>();
        keys011.add(TestUtils.toByteArray("k011_zone0_only"));
        List<ByteArray> keys100 = new ArrayList<ByteArray>();
        keys100.add(TestUtils.toByteArray("k100_zone1_only"));
        assertEquals(2, store.getAll(keys011, null)
                             .get(TestUtils.toByteArray("k011_zone0_only"))
                             .size());
        assertFalse(store.getAll(keys100, null)
                         .containsKey(TestUtils.toByteArray("k100_zone1_only")));
        /* test multiple keys getall */
        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(TestUtils.toByteArray("k011_zone0_only"));
        keys.add(TestUtils.toByteArray("k100_zone1_only"));
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertEquals(2, result.get(TestUtils.toByteArray("k011_zone0_only")).size());
        assertFalse(result.containsKey(TestUtils.toByteArray("k100_zone1_only")));
    }

    @Test
    public void testGetall_211() throws Exception {
        cluster = getFourNodeClusterWithZones();
        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);
        storeDef = new StoreDefinitionBuilder().setName("test")
                                               .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                               .setReplicationFactor(4)
                                               .setZoneReplicationFactor(zoneReplicationFactor)
                                               .setKeySerializer(new SerializerDefinition("string"))
                                               .setValueSerializer(new SerializerDefinition("string"))
                                               .setPreferredReads(1)
                                               .setRequiredReads(1)
                                               .setPreferredWrites(1)
                                               .setRequiredWrites(1)
                                               .setZoneCountReads(0)
                                               .setZoneCountWrites(0)
                                               .build();
        makeStore();
        Versioned<byte[]> v = Versioned.value("v".getBytes());
        // k### indicates existence of itself in different nodes
        // k**1 means this key exists at least on node 0
        // k*1* means this key exists at least on node 1
        // k0** means this key does not exist on node 2
        subStores.get(0).put(TestUtils.toByteArray("k001"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k101"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k111"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k010"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k110"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k111"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k100"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k101"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k110"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k111"), v, null);

        /* test multiple keys getall */
        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(TestUtils.toByteArray("k000"));
        keys.add(TestUtils.toByteArray("k001"));
        keys.add(TestUtils.toByteArray("k010"));
        keys.add(TestUtils.toByteArray("k011"));
        keys.add(TestUtils.toByteArray("k100"));
        keys.add(TestUtils.toByteArray("k101"));
        keys.add(TestUtils.toByteArray("k110"));
        keys.add(TestUtils.toByteArray("k111"));
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertFalse(result.containsKey(TestUtils.toByteArray("not_included")));
        assertFalse(result.containsKey(TestUtils.toByteArray("k000")));
        assertEquals(1, result.get(TestUtils.toByteArray("k011")).size());
        assertFalse(result.containsKey(TestUtils.toByteArray("k100")));
        assertEquals(1, result.get(TestUtils.toByteArray("k111")).size());
    }

    @Test
    public void testGetall_211_zoneCountRead_1() throws Exception {
        cluster = getFourNodeClusterWithZones();
        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);
        /*
         * First n nodes on the preference list will be one node from each
         * remote n zones, where n=zoneCountReads, therefore preferred read
         * should be set > n if want to include local zone node results in
         * parallel request
         */
        storeDef = new StoreDefinitionBuilder().setName("test")
                                               .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                               .setReplicationFactor(4)
                                               .setZoneReplicationFactor(zoneReplicationFactor)
                                               .setKeySerializer(new SerializerDefinition("string"))
                                               .setValueSerializer(new SerializerDefinition("string"))
                                               .setPreferredReads(2)
                                               .setRequiredReads(1)
                                               .setPreferredWrites(1)
                                               .setRequiredWrites(1)
                                               .setZoneCountReads(1)
                                               .setZoneCountWrites(0)
                                               .build();
        makeStore();
        Versioned<byte[]> v = Versioned.value("v".getBytes());
        subStores.get(0).put(TestUtils.toByteArray("k001"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k101"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k111"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k010"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k110"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k111"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k100"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k101"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k110"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k111"), v, null);

        /* test multiple keys getall */
        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(TestUtils.toByteArray("k000"));
        keys.add(TestUtils.toByteArray("k001"));
        keys.add(TestUtils.toByteArray("k010"));
        keys.add(TestUtils.toByteArray("k011"));
        keys.add(TestUtils.toByteArray("k100"));
        keys.add(TestUtils.toByteArray("k101"));
        keys.add(TestUtils.toByteArray("k110"));
        keys.add(TestUtils.toByteArray("k111"));
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertFalse(result.containsKey(TestUtils.toByteArray("not_included")));
        /* client will first try all the nodes in local zone */
        assertFalse(result.containsKey(TestUtils.toByteArray("k000")));
        assertEquals(1, result.get(TestUtils.toByteArray("k011")).size());
        assertFalse(result.containsKey(TestUtils.toByteArray("not_included")));
        assertFalse(result.containsKey(TestUtils.toByteArray("k000")));
        assertEquals(1, result.get(TestUtils.toByteArray("k011")).size());
        assertEquals(1, result.get(TestUtils.toByteArray("k100")).size());
        assertEquals(2, result.get(TestUtils.toByteArray("k111")).size());
    }

    @Test
    public void testGetall_322() throws Exception {
        cluster = getEightNodeClusterWithZones();
        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(0, 3);
        zoneReplicationFactor.put(1, 3);
        storeDef = new StoreDefinitionBuilder().setName("test")
                                               .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                               .setReplicationFactor(6)
                                               .setZoneReplicationFactor(zoneReplicationFactor)
                                               .setKeySerializer(new SerializerDefinition("string"))
                                               .setValueSerializer(new SerializerDefinition("string"))
                                               .setPreferredReads(2)
                                               .setRequiredReads(2)
                                               .setPreferredWrites(2)
                                               .setRequiredWrites(2)
                                               .setZoneCountReads(0)
                                               .setZoneCountWrites(0)
                                               .build();
        makeStore();
        Versioned<byte[]> v = Versioned.value("v".getBytes());
        subStores.get(0).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(0).put(TestUtils.toByteArray("k0000_1111"), v, null);

        subStores.get(1).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k0000_1111"), v, null);

        subStores.get(2).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k0000_1111"), v, null);

        subStores.get(3).put(TestUtils.toByteArray("k0000_1111"), v, null);
        subStores.get(3).put(TestUtils.toByteArray("k1111_1111"), v, null);

        subStores.get(4).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(4).put(TestUtils.toByteArray("k1111_0000"), v, null);
        subStores.get(5).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(5).put(TestUtils.toByteArray("k1111_0000"), v, null);
        subStores.get(6).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(6).put(TestUtils.toByteArray("k1111_0000"), v, null);
        subStores.get(7).put(TestUtils.toByteArray("k1111_1111"), v, null);
        subStores.get(7).put(TestUtils.toByteArray("k1111_0000"), v, null);

        /* test multiple keys getall */
        List<ByteArray> keys = new ArrayList<ByteArray>();
        keys.add(TestUtils.toByteArray("k0000_0000"));
        keys.add(TestUtils.toByteArray("k0000_1111"));
        keys.add(TestUtils.toByteArray("k1111_0000"));
        keys.add(TestUtils.toByteArray("k1111_1111"));
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertFalse(result.containsKey(TestUtils.toByteArray("not_included")));
        assertFalse(result.containsKey(TestUtils.toByteArray("k0000_0000")));
        assertEquals(2, result.get(TestUtils.toByteArray("k0000_1111")).size());
        assertFalse(result.containsKey(TestUtils.toByteArray("k1111_0000")));
        assertEquals(2, result.get(TestUtils.toByteArray("k1111_1111")).size());
    }
}

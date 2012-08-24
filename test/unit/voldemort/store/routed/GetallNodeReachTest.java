package voldemort.store.routed;

import static org.junit.Assert.assertEquals;
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

    @Before
    public void setUp() throws Exception {
        cluster = getFourNodeClusterWithZones();
    }

    @Test
    public void testGetallTouchOne() throws Exception {
        RoutedStore store = null;
        HashMap<Integer, Integer> zoneReplicationFactor = new HashMap<Integer, Integer>();
        zoneReplicationFactor.put(0, 2);
        zoneReplicationFactor.put(1, 1);
        zoneReplicationFactor.put(2, 1);
        StoreDefinition storeDef = new StoreDefinitionBuilder().setName("test")
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
        Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = Maps.newHashMap();
        for(Node n: cluster.getNodes()) {
            Store<ByteArray, byte[], byte[]> subStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test");
            subStores.put(n.getId(), subStore);

        }
        RoutedStoreFactory routedStoreFactory = new RoutedStoreFactory(true,
                                                                       Executors.newFixedThreadPool(2),
                                                                       new TimeoutConfig(1000L,
                                                                                         false));

        store = routedStoreFactory.create(cluster,
                                          storeDef,
                                          subStores,
                                          true,
                                          new NoopFailureDetector());
        Versioned<byte[]> v = Versioned.value("v".getBytes());
        subStores.get(0).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(1).put(TestUtils.toByteArray("k011"), v, null);
        subStores.get(2).put(TestUtils.toByteArray("k100"), v, null);
        List<ByteArray> keys011 = new ArrayList<ByteArray>();
        keys011.add(TestUtils.toByteArray("k011"));
        List<ByteArray> keys100 = new ArrayList<ByteArray>();
        keys100.add(TestUtils.toByteArray("k100"));
        assertEquals(store.getAll(keys011, null).get(TestUtils.toByteArray("k011")).size(), 2);
        assertEquals(store.getAll(keys100, null).get(TestUtils.toByteArray("k100")).size(), 0);
    }
}

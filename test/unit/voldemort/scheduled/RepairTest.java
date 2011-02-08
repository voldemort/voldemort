package voldemort.scheduled;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.slop.RepairJob;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RepairTest {

    private static int NUM_KEYS = 100;
    private static String clusterXmlFile = "test/common/voldemort/config/four-node-cluster-with-zones.xml";

    @Test
    public void testRepair() throws IOException {

        Cluster cluster = new ClusterMapper().readCluster(new File(clusterXmlFile));

        // Create 3 stores - RO, Memory
        StoreDefinition def1 = new StoreDefinitionBuilder().setName("test1")
                                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                           .setKeySerializer(new SerializerDefinition("string"))
                                                           .setValueSerializer(new SerializerDefinition("string"))
                                                           .setRoutingPolicy(RoutingTier.SERVER)
                                                           .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                           .setReplicationFactor(2)
                                                           .setPreferredReads(1)
                                                           .setRequiredReads(1)
                                                           .setPreferredWrites(1)
                                                           .setRequiredWrites(1)
                                                           .build();
        StoreDefinition def2 = new StoreDefinitionBuilder().setName("test2")
                                                           .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                           .setKeySerializer(new SerializerDefinition("string"))
                                                           .setValueSerializer(new SerializerDefinition("string"))
                                                           .setRoutingPolicy(RoutingTier.SERVER)
                                                           .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                           .setReplicationFactor(2)
                                                           .setPreferredReads(1)
                                                           .setRequiredReads(1)
                                                           .setPreferredWrites(1)
                                                           .setRequiredWrites(1)
                                                           .build();
        HashMap<Integer, Integer> zoneRepFactor = Maps.newHashMap();
        zoneRepFactor.put(0, 1);
        zoneRepFactor.put(1, 1);
        StoreDefinition def3 = new StoreDefinitionBuilder().setName("test3")
                                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                           .setKeySerializer(new SerializerDefinition("string"))
                                                           .setValueSerializer(new SerializerDefinition("string"))
                                                           .setRoutingPolicy(RoutingTier.SERVER)
                                                           .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                           .setReplicationFactor(2)
                                                           .setZoneCountReads(0)
                                                           .setZoneCountWrites(0)
                                                           .setZoneReplicationFactor(zoneRepFactor)
                                                           .setPreferredReads(1)
                                                           .setRequiredReads(1)
                                                           .setPreferredWrites(1)
                                                           .setRequiredWrites(1)
                                                           .build();

        // create new metadata store.
        MetadataStore metadata = ServerTestUtils.createMetadataStore(cluster,
                                                                     Lists.newArrayList(def1,
                                                                                        def2,
                                                                                        def3));

        RoutingStrategyFactory factory = new RoutingStrategyFactory();
        RoutingStrategy strategy1 = factory.updateRoutingStrategy(def1, cluster), strategy3 = factory.updateRoutingStrategy(def3,

                                                                                                                            cluster);

        for(int nodeId = 0; nodeId < 4; nodeId++) {
            StoreRepository repository = new StoreRepository();

            StorageEngine<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test1");
            repository.addStorageEngine(store);

            StorageEngine<ByteArray, byte[], byte[]> store2 = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test2");
            repository.addStorageEngine(store2);

            StorageEngine<ByteArray, byte[], byte[]> store3 = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test3");
            repository.addStorageEngine(store3);

            repository.addLocalStore(metadata);

            StorageEngine<ByteArray, byte[], byte[]> slopStore = new InMemoryStorageEngine<ByteArray, byte[], byte[]>("slop");
            SlopStorageEngine slopStorageEngine = new SlopStorageEngine(slopStore,
                                                                        metadata.getCluster());
            repository.setSlopStore(slopStorageEngine);

            // Populate the stores with keys
            for(int i = 0; i < NUM_KEYS; i++) {
                store.put(new ByteArray(new String("key" + i).getBytes()),
                          new Versioned<byte[]>(new String("value" + i).getBytes()),
                          null);
                store2.put(new ByteArray(new String("key" + i).getBytes()),
                           new Versioned<byte[]>(new String("value" + i).getBytes()),
                           null);
                store3.put(new ByteArray(new String("key" + i).getBytes()),
                           new Versioned<byte[]>(new String("value" + i).getBytes()),
                           null);
            }

            metadata.put(MetadataStore.NODE_ID_KEY, nodeId);
            RepairJob job = new RepairJob(repository, metadata, new Semaphore(1));
            job.run();

            long slopsTest1 = job.getRepairSlopsPerStore("test1");
            long slopsTest2 = job.getRepairSlopsPerStore("test2");
            long slopsTest3 = job.getRepairSlopsPerStore("test3");
            long slop1 = 0, slop2 = 0, slop3 = 0;

            // Go over every slop and check if everything should be present
            ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = slopStorageEngine.asSlopStore()
                                                                                           .entries();
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<Slop>> keyVal = iterator.next();
                String storeName = keyVal.getSecond().getValue().getStoreName();
                ByteArray key = keyVal.getSecond().getValue().getKey();
                List<Versioned<byte[]>> values = null;

                if(storeName.compareTo("test1") == 0) {
                    slop1++;
                    assertFalse(containsNode(strategy1.routeRequest(key.get()), nodeId));

                    values = store.get(key, null);
                    Assert.assertEquals(values.size(), 0);

                } else if(storeName.compareTo("test3") == 0) {
                    slop3++;
                    assertFalse(containsNode(strategy3.routeRequest(key.get()), nodeId));

                    values = store3.get(key, null);
                    Assert.assertEquals(values.size(), 0);

                } else if(storeName.compareTo("test2") == 0) {

                    Assert.fail("Cannot have slops for any other store");
                }
            }
            Assert.assertEquals(slop1, slopsTest1);
            Assert.assertEquals(slop2, slopsTest2);
            Assert.assertEquals(slop3, slopsTest3);

            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> values = store.entries();
            int valuesLeft = 0;
            while(values.hasNext()) {
                values.next();
                valuesLeft++;
            }
            Assert.assertEquals(valuesLeft, NUM_KEYS - (slop1 / 2));

            values = store2.entries();
            valuesLeft = 0;
            while(values.hasNext()) {
                values.next();
                valuesLeft++;
            }
            Assert.assertEquals(valuesLeft, NUM_KEYS - (slop2 / 2));

            values = store3.entries();
            valuesLeft = 0;
            while(values.hasNext()) {
                values.next();
                valuesLeft++;
            }
            Assert.assertEquals(valuesLeft, NUM_KEYS - (slop3 / 2));

            slopStore.truncate();

        }
    }

    private boolean containsNode(List<Node> nodes, int nodeId) {
        for(Node node: nodes) {
            if(node.getId() == nodeId) {
                return true;
            }
        }
        return false;
    }
}

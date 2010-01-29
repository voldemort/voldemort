package voldemort.store.readonly;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.compress.CompressingStore;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

public class ReadOnlyStorageEngineTestInstance {

    private final Map<String, String> data;
    private final File baseDir;
    private final Map<Integer, Store<String, String>> nodeStores;
    private final RoutingStrategy routingStrategy;
    private final Serializer<String> keySerializer;

    private ReadOnlyStorageEngineTestInstance(Map<String, String> data,
                                              File baseDir,
                                              Map<Integer, Store<String, String>> nodeStores,
                                              RoutingStrategy routingStrategy,
                                              Serializer<String> keySerializer) {
        this.data = data;
        this.baseDir = baseDir;
        this.nodeStores = nodeStores;
        this.routingStrategy = routingStrategy;
        this.keySerializer = keySerializer;
    }

    public void delete() {
        Utils.rm(this.baseDir);
    }

    private static Map<String, String> createTestData(int testSize) {
        Map<String, String> data = new HashMap<String, String>(testSize);
        for(int i = 0; i < testSize; i++) {
            String letters = TestUtils.randomLetters(10);
            data.put(letters, letters);
        }
        return data;
    }

    private static JsonReader makeTestDataReader(Map<String, String> data, File dir)
            throws Exception {
        File dataFile = File.createTempFile("test-data", ".txt", dir);
        dataFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile));
        for(Map.Entry<String, String> entry: data.entrySet())
            writer.write("\"" + entry.getKey() + "\"\t\"" + entry.getValue() + "\"\n");
        writer.close();
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        return new JsonReader(reader);
    }

    public static ReadOnlyStorageEngineTestInstance create(SearchStrategy strategy,
                                                           File baseDir,
                                                           int testSize,
                                                           int numNodes,
                                                           int repFactor,
                                                           SerializerDefinition keySerDef,
                                                           SerializerDefinition valueSerDef)
            throws Exception {
        // create some test data
        Map<String, String> data = createTestData(testSize);
        JsonReader reader = makeTestDataReader(data, baseDir);

        // set up definitions for cluster and store
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numNodes; i++) {
            nodes.add(new Node(i,
                               "localhost",
                               8080 + i,
                               6666 + i,
                               7000 + i,
                               Arrays.asList(4 * i, 4 * i + 1, 4 * i + 2, 4 * i + 3)));
        }
        Cluster cluster = new Cluster("test", nodes);
        StoreDefinition storeDef = new StoreDefinitionBuilder().setName("test")
                                                               .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                               .setKeySerializer(keySerDef)
                                                               .setValueSerializer(valueSerDef)
                                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                                               .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                               .setReplicationFactor(repFactor)
                                                               .setPreferredReads(1)
                                                               .setRequiredReads(1)
                                                               .setPreferredWrites(1)
                                                               .setRequiredWrites(1)
                                                               .build();
        RoutingStrategy router = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                    cluster);

        // build store files in outputDir
        File outputDir = TestUtils.createTempDir(baseDir);
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(reader,
                                                             cluster,
                                                             storeDef,
                                                             router,
                                                             outputDir,
                                                             null,
                                                             testSize / 5,
                                                             1,
                                                             2,
                                                             10000);
        storeBuilder.build();

        File nodeDir = TestUtils.createTempDir(baseDir);
        @SuppressWarnings("unchecked")
        Serializer<String> keySerializer = (Serializer<String>) new DefaultSerializerFactory().getSerializer(keySerDef);
        Serializer<String> valueSerializer = (Serializer<String>) new DefaultSerializerFactory().getSerializer(valueSerDef);
        Map<Integer, Store<String, String>> nodeStores = Maps.newHashMap();
        for(int i = 0; i < numNodes; i++) {
            File currNode = new File(nodeDir, Integer.toString(i));
            currNode.mkdirs();
            currNode.deleteOnExit();
            Utils.move(new File(outputDir, "node-" + Integer.toString(i)), new File(currNode,
                                                                                    "version-0"));

            CompressionStrategyFactory comppressionStrategyFactory = new CompressionStrategyFactory();
            CompressionStrategy keyCompressionStrat = comppressionStrategyFactory.get(keySerDef.getCompression());
            CompressionStrategy valueCompressionStrat = comppressionStrategyFactory.get(valueSerDef.getCompression());
            Store<ByteArray, byte[]> innerStore = new CompressingStore(new ReadOnlyStorageEngine("test",
                                                                                                 strategy,
                                                                                                 currNode,
                                                                                                 1),
                                                                       keyCompressionStrat,
                                                                       valueCompressionStrat);

            nodeStores.put(i, SerializingStore.wrap(innerStore, keySerializer, valueSerializer));
        }

        return new ReadOnlyStorageEngineTestInstance(data,
                                                     baseDir,
                                                     nodeStores,
                                                     router,
                                                     keySerializer);
    }

    public List<Node> routeRequest(String key) {
        return this.routingStrategy.routeRequest(this.keySerializer.toBytes(key));
    }

    public Map<String, String> getData() {
        return data;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public Map<Integer, Store<String, String>> getNodeStores() {
        return nodeStores;
    }

    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

}
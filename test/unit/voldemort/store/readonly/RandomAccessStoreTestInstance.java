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
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.store.StorageEngineType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

public class RandomAccessStoreTestInstance {

    private final Map<String, String> data;
    private final File baseDir;
    private final Map<Integer, Store<String, String>> nodeStores;
    private final RoutingStrategy routingStrategy;
    private final Serializer<String> serializer;

    private RandomAccessStoreTestInstance(Map<String, String> data,
                                          File baseDir,
                                          Map<Integer, Store<String, String>> nodeStores,
                                          RoutingStrategy routingStrategy,
                                          Serializer<String> serializer) {
        this.data = data;
        this.baseDir = baseDir;
        this.nodeStores = nodeStores;
        this.routingStrategy = routingStrategy;
        this.serializer = serializer;
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

    public static RandomAccessStoreTestInstance create(File baseDir,
                                                       int testSize,
                                                       int numNodes,
                                                       int repFactor) throws Exception {
        // create some test data
        Map<String, String> data = createTestData(testSize);
        JsonReader reader = makeTestDataReader(data, baseDir);

        // set up definitions for cluster and store
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numNodes; i++) {
            nodes.add(new Node(i, "localhost", 8080 + i, 6666 + i, Arrays.asList(4 * i,
                                                                                 4 * i + 1,
                                                                                 4 * i + 2,
                                                                                 4 * i + 3)));
        }
        Cluster cluster = new Cluster("test", nodes);
        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        StoreDefinition storeDef = new StoreDefinition("test",
                                                       StorageEngineType.READONLY,
                                                       serDef,
                                                       serDef,
                                                       RoutingTier.CLIENT,
                                                       repFactor,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1);
        RoutingStrategy router = new ConsistentRoutingStrategy(cluster.getNodes(), repFactor);

        // build and open store
        File outputDir = TestUtils.createTempDir(baseDir);
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(reader,
                                                             cluster,
                                                             storeDef,
                                                             router,
                                                             outputDir,
                                                             testSize / 5,
                                                             1);
        storeBuilder.build();

        @SuppressWarnings("unchecked")
        Serializer<String> serializer = (Serializer<String>) new DefaultSerializerFactory().getSerializer(serDef);
        Map<Integer, Store<String, String>> nodeStores = Maps.newHashMap();
        File nodeDataDir = new File(baseDir, "nodes");
        nodeDataDir.deleteOnExit();
        for(int i = 0; i < numNodes; i++) {
            File nodeDir = new File(nodeDataDir, Integer.toString(i));
            nodeDir.mkdirs();

            // rename files
            boolean indexCreated = new File(outputDir, i + ".index").renameTo(new File(nodeDir,
                                                                                       "test.index"));
            boolean dataFileCreated = new File(outputDir, i + ".data").renameTo(new File(nodeDir,
                                                                                         "test.data"));
            if(!indexCreated || !dataFileCreated)
                throw new IllegalArgumentException("Failed to create index or data file.");

            // open store
            nodeStores.put(i,
                           new SerializingStore<String, String>(new RandomAccessFileStore("test",
                                                                                          nodeDir,
                                                                                          1,
                                                                                          3,
                                                                                          1000,
                                                                                          100 * 1024),
                                                                serializer,
                                                                serializer));
        }

        return new RandomAccessStoreTestInstance(data, baseDir, nodeStores, router, serializer);
    }

    public List<Node> routeRequest(String key) {
        return this.routingStrategy.routeRequest(this.serializer.toBytes(key));
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
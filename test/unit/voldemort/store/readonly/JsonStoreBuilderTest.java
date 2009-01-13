package voldemort.store.readonly;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.StorageEngineType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class JsonStoreBuilderTest extends TestCase {

    private static final int TEST_SIZE = 1000;

    private Map<String, String> data;
    private File dataDir;
    private Store<Object, Object> store;

    public void setUp() throws Exception {
        // create test data
        this.data = new HashMap<String, String>(TEST_SIZE);
        for(int i = 0; i < TEST_SIZE; i++)
            this.data.put(TestUtils.randomLetters(10), TestUtils.randomLetters(10));

        // write data to file
        File dataFile = File.createTempFile("test", ".txt");
        // dataFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile));
        for(Map.Entry<String, String> entry: this.data.entrySet())
            writer.write("\"" + entry.getKey() + "\"\t\"" + entry.getValue() + "\"\n");
        writer.close();
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        JsonReader jsonReader = new JsonReader(reader);

        // set up definitions for cluster and store
        Cluster cluster = new Cluster("test", Collections.singletonList(new Node(0,
                                                                                 "localhost",
                                                                                 8080,
                                                                                 6666,
                                                                                 Arrays.asList(0,
                                                                                               1,
                                                                                               2,
                                                                                               3,
                                                                                               4))));
        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        StoreDefinition storeDef = new StoreDefinition("test",
                                                       StorageEngineType.READONLY,
                                                       serDef,
                                                       serDef,
                                                       RoutingTier.CLIENT,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1);
        RoutingStrategy router = new ConsistentRoutingStrategy(cluster.getNodes(), 1);
        this.dataDir = TestUtils.getTempDirectory();

        // build and open store
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(jsonReader,
                                                             cluster,
                                                             storeDef,
                                                             router,
                                                             dataDir,
                                                             10);
        storeBuilder.build();
        Serializer<Object> serializer = new JsonTypeSerializer("'string'");
        this.store = new SerializingStore<Object, Object>(new RandomAccessFileStore("test",
                                                                                    this.dataDir,
                                                                                    1,
                                                                                    3,
                                                                                    1000),
                                                          serializer,
                                                          serializer);
    }

    public void tearDown() {
        Utils.rm(this.dataDir);
    }

    /**
     * For each key/value pair we built into the store, look it up and test that
     * the correct value is returned
     */
    public void XXXtestCanGetGoodValues() {
        for(Map.Entry<String, String> entry: this.data.entrySet()) {
            List<Versioned<Object>> found = this.store.get(entry.getKey());
            assertEquals(found.size(), 1);
            Versioned<Object> obj = found.get(0);
            assertEquals(entry.getValue(), obj.getValue());
        }
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    public void XXXtestCantGetBadValues() {
        for(int i = 0; i < TEST_SIZE; i++) {
            String key = TestUtils.randomLetters(10);
            if(!this.data.containsKey(key))
                assertEquals(null, this.store.get(key));
        }
    }

}

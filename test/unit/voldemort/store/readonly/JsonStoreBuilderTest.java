/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
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
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

public class JsonStoreBuilderTest extends TestCase {

    private static final int TEST_SIZE = 500;

    private Map<String, String> data;
    private File dataDir;
    private Store<Object, Object> nodeStore;

    @Override
    public void setUp() throws Exception {
        // create test data
        this.data = new HashMap<String, String>(TEST_SIZE);
        for(int i = 0; i < TEST_SIZE; i++)
            this.data.put(TestUtils.randomLetters(10), TestUtils.randomLetters(10));

        // write data to file
        File dataFile = File.createTempFile("test", ".txt");
        dataFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile));
        for(Map.Entry<String, String> entry: this.data.entrySet())
            writer.write("\"" + entry.getKey() + "\"\t\"" + entry.getValue() + "\"\n");
        writer.close();
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        JsonReader jsonReader = new JsonReader(reader);

        // set up definitions for cluster and store
        List<Node> nodes = ImmutableList.of(new Node(0, "localhost", 8080, 6666, Arrays.asList(0,
                                                                                               1,
                                                                                               2,
                                                                                               3,
                                                                                               4)),
                                            new Node(1, "localhost", 8081, 6667, Arrays.asList(5,
                                                                                               6,
                                                                                               7,
                                                                                               8,
                                                                                               9)));

        Cluster cluster = new Cluster("test", nodes);
        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        StoreDefinition storeDef = new StoreDefinition("test",
                                                       StorageEngineType.READONLY,
                                                       serDef,
                                                       serDef,
                                                       RoutingTier.CLIENT,
                                                       2,
                                                       1,
                                                       1,
                                                       1,
                                                       1,
                                                       1);
        RoutingStrategy router = new ConsistentRoutingStrategy(cluster.getNodes(), 2);
        this.dataDir = TestUtils.getTempDirectory();

        // build and open store
        File outputDir = TestUtils.getTempDirectory();
        JsonStoreBuilder storeBuilder = new JsonStoreBuilder(jsonReader,
                                                             cluster,
                                                             storeDef,
                                                             router,
                                                             outputDir,
                                                             100,
                                                             1);
        storeBuilder.build();

        // rename files
        new File(outputDir, "0.index").renameTo(new File(dataDir, "test.index"));
        new File(outputDir, "0.data").renameTo(new File(dataDir, "test.data"));

        // open store
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);
        this.nodeStore = new SerializingStore<Object, Object>(new RandomAccessFileStore("test",
                                                                                        this.dataDir,
                                                                                        1,
                                                                                        3,
                                                                                        1000,
                                                                                        100 * 1024),
                                                              serializer,
                                                              serializer);
    }

    @Override
    public void tearDown() {
        Utils.rm(this.dataDir);
    }

    /**
     * For each key/value pair we built into the store, look it up and test that
     * the correct value is returned
     */
    public void testCanGetGoodValues() {
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: this.data.entrySet()) {
                List<Versioned<Object>> found = this.nodeStore.get(entry.getKey());
                assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i,
                             1,
                             found.size());
                Versioned<Object> obj = found.get(0);
                assertEquals(entry.getValue(), obj.getValue());
            }
        }
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    public void testCantGetBadValues() {
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(int j = 0; j < TEST_SIZE; j++) {
                String key = TestUtils.randomLetters(10);
                if(!this.data.containsKey(key))
                    assertEquals(0, this.nodeStore.get(key).size());
            }
        }
    }

    public void testCanMultigetGoodValues() {
        Set<Object> keys = new HashSet<Object>(this.data.keySet());
        Map<Object, List<Versioned<Object>>> results = nodeStore.getAll(keys);
        for(String key: data.keySet()) {
            assertTrue("Key '" + key + "' not found in result set.", results.containsKey(key));
            assertEquals(1, results.get(key).size());
            assertEquals(data.get(key), results.get(key).get(0).getValue());
        }
    }
}

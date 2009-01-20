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

public class JsonStoreBuilderTest extends TestCase {

    private static final int TEST_SIZE = 500;

    private Map<String, String> data;
    private File dataDir;
    private Store<Object, Object> store;

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
                                                             100,
                                                             1);
        storeBuilder.build();

        // rename files
        new File(dataDir, "0.index").renameTo(new File(dataDir, "test.index"));
        new File(dataDir, "0.data").renameTo(new File(dataDir, "test.data"));

        // open store
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);
        this.store = new SerializingStore<Object, Object>(new RandomAccessFileStore("test",
                                                                                    this.dataDir,
                                                                                    1,
                                                                                    3,
                                                                                    1000),
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
        for(Map.Entry<String, String> entry: this.data.entrySet()) {
            List<Versioned<Object>> found = this.store.get(entry.getKey());
            assertEquals(1, found.size());
            Versioned<Object> obj = found.get(0);
            assertEquals(entry.getValue(), obj.getValue());
        }
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    public void testCantGetBadValues() {
        for(int i = 0; i < TEST_SIZE; i++) {
            String key = TestUtils.randomLetters(10);
            if(!this.data.containsKey(key))
                assertEquals(0, this.store.get(key).size());
        }
    }

}

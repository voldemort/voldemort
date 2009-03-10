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

package test.voldemort.contrib.batchswapper;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.contrib.batchswapper.ReadOnlyBatchIndexSwapper;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.xml.ClusterMapper;

public class ReadOnlySimpleSwapperTest extends TestCase {

    private static final String baseDir = TestUtils.createTempDir().getAbsolutePath();

    private static final String clusterFile = "contrib/common/config/two-node-cluster.xml";
    private static final String storerFile = "contrib/common/config/testSwapStore.xml";
    private static final String storeName = "swapTestStore";

    VoldemortServer server1;
    VoldemortServer server2;

    @Override
    public void setUp() throws Exception {
        // clean baseDir to be sure
        FileDeleteStrategy.FORCE.delete(new File(baseDir));

        String indexDir = makeReadOnlyIndex(1, 1000);
        server1 = startServer(0, indexDir);
        server2 = startServer(1, indexDir);
    }

    private VoldemortServer startServer(int nodeId, String indexDir) throws Exception {
        VoldemortConfig config = ServerTestUtils.createServerConfig(nodeId,
                                                                    baseDir,
                                                                    clusterFile,
                                                                    storerFile);
        VoldemortServer server = new VoldemortServer(config);
        // copy read-only index before starting
        FileUtils.copyFile(new File(indexDir, nodeId + ".index"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".index"));
        FileUtils.copyFile(new File(indexDir, nodeId + ".data"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".data"));
        server.start();
        return server;
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server1.stop();
        server2.stop();
        FileDeleteStrategy.FORCE.delete(new File(baseDir));
    }

    private String makeReadOnlyIndex(int minKey, int maxKey) throws Exception {

        Map<String, String> entryMap = new HashMap<String, String>();
        for(int i = minKey; i <= maxKey; i++) {
            entryMap.put("key" + i, "value-" + i);
        }

        Cluster cluster = new ClusterMapper().readCluster(new FileReader(new File("contrib/common/config/two-node-cluster.xml")));
        return ReadOnlySwapperTestUtils.createReadOnlyIndex(cluster, entryMap, baseDir);
    }

    public void testswap() throws Throwable {
        // assert that read-only store is working
        Store<ByteArray, byte[]> store1 = server1.getStoreMap().get(storeName);
        Store<ByteArray, byte[]> store2 = server2.getStoreMap().get(storeName);

        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        Serializer<Object> serializer = StoreUtils.unsafeGetSerializer(new DefaultSerializerFactory(),
                                                                       serDef);

        // initial keys are from 1 to 1000
        for(int i = 1; i < 1000; i++) {

            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            byte[] value = serializer.toBytes("value" + i);

            assertEquals("either store1 or store2 will have the key:'key-" + i + "'",
                         true,
                         store1.get(key).size() > 0 || store2.get(key).size() > 0);
            assertEquals("value should match",
                         value,
                         (store1.get(key).size() > 0) ? store1.get(key).get(0) : store2.get(key)
                                                                                       .get(0));
        }

        // lets create new index files
        final String newIndexDir = makeReadOnlyIndex(2000, 3000);

        ReadOnlyBatchIndexSwapper indexSwapper = new ReadOnlyBatchIndexSwapper() {

            @Override
            public void configure(Props props) {
                props.put("voldemort.cluster.local.filePath", clusterFile);
                props.put("voldemort.store.name", storeName);
                props.put("source.local.path", newIndexDir);
                props.put("destination.remote.path", baseDir + File.separatorChar
                                                     + (int) (Math.random() * 1000));
            }

            @Override
            public boolean copyRemoteFile(String hostname, String source, String destination) {
                // for test both files are local just
                int i = 0;
                while(i++ < 5)
                    try {
                        FileUtils.copyFile(new File(source), new File(destination));
                        if(new File(destination).exists()) {
                            return true;
                        }

                    } catch(IOException e) {
                        // ignore
                    }

                return false;
            }
        };

        // do Index Swap
        indexSwapper.run();

        // check that only new keys can be seen
        for(int i = 1; i < 1000; i++) {
            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            assertEquals("store 1 get for key:" + i + " should be empty", 0, store1.get(key).size());
            assertEquals("store 2 get for key:" + i + " should be empty", 0, store2.get(key).size());
        }

        for(int i = 2000; i < 3000; i++) {
            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            byte[] value = serializer.toBytes("value" + i);
            assertEquals("either store1 or store2 will have the key:'key-" + i + "'",
                         true,
                         store1.get(key).size() > 0 || store2.get(key).size() > 0);
            assertEquals("value should match",
                         value,
                         (store1.get(key).size() > 0) ? store1.get(key).get(0) : store2.get(key)
                                                                                       .get(0));
        }

    }
}

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;

import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.contrib.batchswapper.AbstractSwapperMapper;
import voldemort.contrib.batchswapper.ReadOnlyBatchIndexHadoopSwapper;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.xml.ClusterMapper;

public class TestReadOnlyHadoopSwapper extends TestCase {

    private static final int TEST_SIZE = 500;
    private static final String baseDir = TestUtils.createTempDir().getAbsolutePath();

    private static final String clusterFile = "contrib/test/common/config/two-node-cluster.xml";
    private static final String storerFile = "contrib/test/common/config/testSwapStore.xml";
    private static final String storeName = "swapTestStore";

    VoldemortServer server1;
    VoldemortServer server2;

    @Override
    public void setUp() throws Exception {
        // clean baseDir to be sure
        FileDeleteStrategy.FORCE.delete(new File(baseDir));

        // First make the readOnlyIndex and copy the index to start Read-Only
        // store cleanly
        String indexDir = makeReadOnlyIndex(1, 1000);

        VoldemortConfig config = TestUtils.createServerConfig(0, baseDir, clusterFile, storerFile);
        server1 = new VoldemortServer(config);
        // copy read-only index before starting
        FileUtils.copyFile(new File(indexDir, "0.index"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".index"));
        FileUtils.copyFile(new File(indexDir, "0.data"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".data"));
        server1.start();

        config = TestUtils.createServerConfig(1, baseDir, clusterFile, storerFile);
        server2 = new VoldemortServer(config);
        // copy read-only index before starting
        FileUtils.copyFile(new File(indexDir, "1.index"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".index"));
        FileUtils.copyFile(new File(indexDir, "1.data"),
                           new File(config.getReadOnlyDataStorageDirectory(), storeName + ".data"));
        server2.start();
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

        Cluster cluster = new ClusterMapper().readCluster(new FileReader(new File("contrib/test/common/config/two-node-cluster.xml")));
        return TestUtils.createReadOnlyIndex(cluster, entryMap, baseDir);
    }

    public void testswap() throws Throwable {
        // assert that read-only store is working
        Store<ByteArray, byte[]> store1 = server1.getStoreMap().get(storeName);
        Store<ByteArray, byte[]> store2 = server2.getStoreMap().get(storeName);

        SerializerDefinition serDef = new SerializerDefinition("json", "'string'");
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);

        // initial keys are from 1 to 1000
        for(int i = 1; i < 1000; i++) {

            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            byte[] value = serializer.toBytes("value" + i);

            assertEquals("either store1 or store2 will have the key:'key-" + i + "'",
                         true,
                         store1.get(key).size() > 0 || store2.get(key).size() > 0);
        }

        // lets create new index files
        final String newIndexDir = makeReadOnlyIndex(2000, 3000);

        ReadOnlyBatchIndexHadoopSwapper indexSwapper = new ReadOnlyBatchIndexHadoopSwapper() {

            @Override
            public void configure(JobConf conf) {
                conf.set("voldemort.cluster.local.filePath", clusterFile);
                conf.set("voldemort.store.name", storeName);
                conf.set("source.path", newIndexDir);
                conf.set("destination.path", baseDir + File.separatorChar
                                             + (int) (Math.random() * 1000));
            }

            @Override
            public Class<? extends Mapper<LongWritable, Text, Text, Text>> getSwapperMapperClass() {
                return SwapperMapper.class;
            }
        };

        // do Index Swap
        indexSwapper.run(null);

        // check that only new keys can be seen
        for(int i = 1; i < 1000; i++) {
            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            byte[] value = serializer.toBytes("value" + i);
            assertEquals("store 1 get for key:" + i + " should be empty", 0, store1.get(key).size());
            assertEquals("store 2 get for key:" + i + " should be empty", 0, store2.get(key).size());
        }

        for(int i = 2000; i < 3000; i++) {
            ByteArray key = new ByteArray(serializer.toBytes("key" + i));
            byte[] value = serializer.toBytes("value" + i);
            assertEquals("either store1 or store2 will have the key:'key-" + i + "'",
                         true,
                         store1.get(key).size() > 0 || store2.get(key).size() > 0);
        }

    }

    static class SwapperMapper extends AbstractSwapperMapper {

        @Override
        public boolean copyRemoteFile(String hostname, String source, String destination) {
            // for test both files are local just
            System.out.println("copy Remote Files called host:" + hostname + " source:" + source
                               + " destination:" + destination);
            assertEquals("source file should be present", true, new File(source).exists());
            try {
                FileUtils.copyFile(new File(source), new File(destination));
            } catch(IOException e) {
                System.out.println("copy call Failed");
                e.printStackTrace();
            }

            return new File(destination).exists();
        }
    }
}

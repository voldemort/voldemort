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

package voldemort.store.readonly.mr;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.BinarySearchStrategy;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

/**
 * Unit test to check Read-Only Batch Indexer <strong>in Local mode numReduce
 * will be only one hence we will see only one node files irrespective of
 * cluster details.</strong>
 * 
 * 
 */
public class HadoopStoreBuilderTest extends TestCase {

    public static class TextStoreMapper extends
            AbstractHadoopStoreBuilderMapper<LongWritable, Text> {

        @Override
        public Object makeKey(LongWritable key, Text value) {
            String[] tokens = value.toString().split("\\s+");
            return tokens[0];
        }

        @Override
        public Object makeValue(LongWritable key, Text value) {
            String[] tokens = value.toString().split("\\s+");
            return tokens[1];
        }

    }

    public void testHadoopBuild() throws Exception {
        // create test data
        Map<String, String> values = new HashMap<String, String>();
        File testDir = TestUtils.createTempDir();
        File tempDir = new File(testDir, "temp"), tempDir2 = new File(testDir, "temp2");
        File outputDir = new File(testDir, "output"), outputDir2 = new File(testDir, "output2");
        File storeDir = TestUtils.createTempDir(testDir);
        for(int i = 0; i < 200; i++)
            values.put(Integer.toString(i), Integer.toBinaryString(i));

        // write test data to text file
        File inputFile = File.createTempFile("input", ".txt", testDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        for(Map.Entry<String, String> entry: values.entrySet())
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        FileUtils.writeStringToFile(inputFile, contents.toString());

        String storeName = "test";
        SerializerDefinition serDef = new SerializerDefinition("string");
        Cluster cluster = ServerTestUtils.getLocalCluster(1);

        // Test backwards compatibility
        StoreDefinition def = new StoreDefinitionBuilder().setName(storeName)
                                                          .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                          .setKeySerializer(serDef)
                                                          .setValueSerializer(serDef)
                                                          .setRoutingPolicy(RoutingTier.CLIENT)
                                                          .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                          .setReplicationFactor(1)
                                                          .setPreferredReads(1)
                                                          .setRequiredReads(1)
                                                          .setPreferredWrites(1)
                                                          .setRequiredWrites(1)
                                                          .build();
        HadoopStoreBuilder builder = new HadoopStoreBuilder(new Configuration(),
                                                            TextStoreMapper.class,
                                                            TextInputFormat.class,
                                                            cluster,
                                                            def,
                                                            2,
                                                            64 * 1024,
                                                            new Path(tempDir2.getAbsolutePath()),
                                                            new Path(outputDir2.getAbsolutePath()),
                                                            new Path(inputFile.getAbsolutePath()));
        builder.build();

        builder = new HadoopStoreBuilder(new Configuration(),
                                         TextStoreMapper.class,
                                         TextInputFormat.class,
                                         cluster,
                                         def,
                                         2,
                                         64 * 1024,
                                         new Path(tempDir.getAbsolutePath()),
                                         new Path(outputDir.getAbsolutePath()),
                                         new Path(inputFile.getAbsolutePath()),
                                         CheckSumType.MD5);
        builder.build();

        // Check if checkSum is generated in outputDir
        File nodeFile = new File(outputDir, "node-0");
        File checkSumFile = new File(nodeFile, "md5checkSum.txt");
        assertTrue(checkSumFile.exists());

        // Check contents of checkSum file
        byte[] md5 = new byte[16];
        DataInputStream in = new DataInputStream(new FileInputStream(checkSumFile));
        in.read(md5);
        in.close();
        checkSumFile.delete();

        byte[] checkSumBytes = CheckSumTests.calculateCheckSum(nodeFile.listFiles(),
                                                               CheckSumType.MD5);
        assertEquals(0, ByteUtils.compare(checkSumBytes, md5));

        // rename files
        File versionDir = new File(storeDir, "version-0");
        versionDir.mkdirs();
        assertTrue("Rename failed.", nodeFile.renameTo(versionDir));

        // open store
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);
        Store<Object, Object> store = SerializingStore.wrap(new ReadOnlyStorageEngine(storeName,
                                                                                      new BinarySearchStrategy(),
                                                                                      storeDir,
                                                                                      1),
                                                            serializer,
                                                            serializer);

        // check values
        for(Map.Entry<String, String> entry: values.entrySet()) {
            List<Versioned<Object>> found = store.get(entry.getKey());
            assertEquals("Incorrect number of results", 1, found.size());
            assertEquals(entry.getValue(), found.get(0).getValue());
        }
    }
}

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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.BinarySearchStrategy;
import voldemort.store.readonly.InterpolationSearchStrategy;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.SearchStrategy;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.fetcher.HdfsFetcher;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * Unit test to check Read-Only Batch Indexer <strong>in Local mode numReduce
 * will be only one hence we will see only one node files irrespective of
 * cluster details.</strong>
 * 
 * 
 */
@RunWith(Parameterized.class)
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderTest {

    private SearchStrategy searchStrategy;
    private boolean saveKeys;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { new BinarySearchStrategy(), true },
                { new InterpolationSearchStrategy(), true }, { new BinarySearchStrategy(), false },
                { new InterpolationSearchStrategy(), false } });
    }

    public HadoopStoreBuilderTest(SearchStrategy searchStrategy, boolean saveKeys) {
        this.saveKeys = saveKeys;
        this.searchStrategy = searchStrategy;
    }

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

    /**
     * Issue 258 : 'node--1' produced during store building if some reducer does
     * not get any data.
     * 
     * @throws Exception
     */
    @Test
    public void testRowsLessThanNodes() throws Exception {
        Map<String, String> values = new HashMap<String, String>();
        File testDir = TestUtils.createTempDir();
        File tempDir = new File(testDir, "temp");
        File outputDir = new File(testDir, "output");

        // write test data to text file
        File inputFile = File.createTempFile("input", ".txt", testDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        for(Map.Entry<String, String> entry: values.entrySet())
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        FileUtils.writeStringToFile(inputFile, contents.toString());

        String storeName = "test";
        SerializerDefinition serDef = new SerializerDefinition("string");
        Cluster cluster = ServerTestUtils.getLocalCluster(10);

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
                                                            new Path(tempDir.getAbsolutePath()),
                                                            new Path(outputDir.getAbsolutePath()),
                                                            new Path(inputFile.getAbsolutePath()),
                                                            CheckSumType.MD5,
                                                            saveKeys,
                                                            false,
                                                            64 * 1024,
                                                            -1,
                                                            false,
                null);
        builder.build();

        // Should not produce node--1 directory + have one folder for every node
        Assert.assertEquals(cluster.getNumberOfNodes(), outputDir.listFiles().length);
        for(File f: outputDir.listFiles()) {
            Assert.assertFalse(f.toString().contains("node--1"));
        }

        // Check if individual nodes exist, along with their metadata file
        for(int nodeId = 0; nodeId < 10; nodeId++) {
            File nodeFile = new File(outputDir, "node-" + Integer.toString(nodeId));
            Assert.assertTrue(nodeFile.exists());
            Assert.assertTrue(new File(nodeFile, ".metadata").exists());
        }
    }

    @Test
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
        HadoopStoreBuilder builder = new HadoopStoreBuilder(
                new Configuration(),
                TextStoreMapper.class,
                TextInputFormat.class,
                cluster,
                def,
                new Path(tempDir2.getAbsolutePath()),
                new Path(outputDir2.getAbsolutePath()),
                new Path(inputFile.getAbsolutePath()),
                CheckSumType.MD5,
                saveKeys,
                false,
                64 * 1024,
                -1,
                false,
                null);
        builder.build();

        builder = new HadoopStoreBuilder(
                new Configuration(),
                TextStoreMapper.class,
                TextInputFormat.class,
                cluster,
                def,
                new Path(tempDir.getAbsolutePath()),
                new Path(outputDir.getAbsolutePath()),
                new Path(inputFile.getAbsolutePath()),
                CheckSumType.MD5,
                saveKeys,
                false,
                64 * 1024,
                -1,
                false,
                null);
        builder.build();

        // Check if checkSum is generated in outputDir
        File nodeFile = new File(outputDir, "node-0");

        // Check if metadata file exists
        File metadataFile = new File(nodeFile, ".metadata");
        Assert.assertTrue(metadataFile.exists());

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata(metadataFile);
        if(saveKeys)
            Assert.assertEquals(metadata.get(ReadOnlyStorageMetadata.FORMAT),
                                ReadOnlyStorageFormat.READONLY_V2.getCode());
        else
            Assert.assertEquals(metadata.get(ReadOnlyStorageMetadata.FORMAT),
                                ReadOnlyStorageFormat.READONLY_V1.getCode());

        Assert.assertEquals(metadata.get(ReadOnlyStorageMetadata.CHECKSUM_TYPE),
                            CheckSum.toString(CheckSumType.MD5));

        // Check contents of checkSum file
        byte[] md5 = Hex.decodeHex(((String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM)).toCharArray());
        byte[] checkSumBytes = CheckSumTests.calculateCheckSum(nodeFile.listFiles(),
                                                               CheckSumType.MD5);
        Assert.assertEquals(0, ByteUtils.compare(checkSumBytes, md5));

        // check if fetching works
        HdfsFetcher fetcher = new HdfsFetcher();

        // Fetch to version directory
        File versionDir = new File(storeDir, "version-0");
        fetcher.fetch(nodeFile.getAbsolutePath(), versionDir.getAbsolutePath());
        Assert.assertTrue(versionDir.exists());

        // open store
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine(storeName,
                                                                 searchStrategy,
                                                                 new RoutingStrategyFactory().updateRoutingStrategy(def,
                                                                                                                    cluster),
                                                                 0,
                                                                 storeDir,
                                                                 1);
        Store<Object, Object, Object> store = SerializingStore.wrap(engine,
                                                                    serializer,
                                                                    serializer,
                                                                    serializer);

        // check values
        for(Map.Entry<String, String> entry: values.entrySet()) {
            List<Versioned<Object>> found = store.get(entry.getKey(), null);
            Assert.assertEquals("Incorrect number of results", 1, found.size());
            Assert.assertEquals(entry.getValue(), found.get(0).getValue());
        }

        // also check the iterator - first key iterator...
        try {
            ClosableIterator<ByteArray> keyIterator = engine.keys();
            if(!saveKeys) {
                fail("Should have thrown an exception since this RO format does not support iterators");
            }
            int numElements = 0;
            while(keyIterator.hasNext()) {
                Assert.assertTrue(values.containsKey(serializer.toObject(keyIterator.next().get())));
                numElements++;
            }

            Assert.assertEquals(numElements, values.size());
        } catch(UnsupportedOperationException e) {
            if(saveKeys) {
                fail("Should not have thrown an exception since this RO format does support iterators");
            }
        }

        // ... and entry iterator
        try {
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = engine.entries();
            if(!saveKeys) {
                fail("Should have thrown an exception since this RO format does not support iterators");
            }
            int numElements = 0;
            while(entryIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
                Assert.assertEquals(values.get(serializer.toObject(entry.getFirst().get())),
                                    serializer.toObject(entry.getSecond().getValue()));
                numElements++;
            }

            Assert.assertEquals(numElements, values.size());
        } catch(UnsupportedOperationException e) {
            if(saveKeys) {
                fail("Should not have thrown an exception since this RO format does support iterators");
            }
        }
    }
}

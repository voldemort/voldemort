package voldemort.store.readonly.mr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyFactory;
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
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.fetcher.HdfsFetcher;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Test for {@link ReadOnlyStorageFormat}-READONLY_V2 where-in we try to
 * generate collisions and check if edge cases are caught correctly
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderCollisionTest {

    private static HashMap<ByteArray, byte[]> oldMd5ToNewMd5 = Maps.newHashMap();

    /**
     * Adding a new custom binary search which remaps the old md5 key to new md5
     * key
     */
    public class CustomBinarySearchStrategy extends BinarySearchStrategy {

        @Override
        public int indexOf(ByteBuffer index, byte[] key, int indexFileSize) {
            return super.indexOf(index, oldMd5ToNewMd5.get(new ByteArray(key)), indexFileSize);
        }
    }

    /**
     * Custom mapper which generates collisions
     */
    public static class CollidingTextStoreMapper extends
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

        @Override
        public void map(LongWritable key,
                        Text value,
                        OutputCollector<BytesWritable, BytesWritable> output,
                        Reporter reporter) throws IOException {
            byte[] keyBytes = keySerializer.toBytes(makeKey(key, value));
            byte[] valBytes = valueSerializer.toBytes(makeValue(key, value));

            // Generate partition and node list this key is destined for
            List<Integer> partitionList = routingStrategy.getPartitionList(keyBytes);
            Node[] partitionToNode = routingStrategy.getPartitionToNode();

            // Leave initial offset for (a) node id (b) partition id
            // since they are written later
            int offsetTillNow = 2 * ByteUtils.SIZE_OF_INT;

            // In order - 4 ( for node id ) + 4 ( partition id ) + 1 ( replica
            // type - primary | secondary | tertiary... ] + 4 ( key size )
            // size ) + 4 ( value size ) + key + value
            byte[] outputValue = new byte[valBytes.length + keyBytes.length
                                          + ByteUtils.SIZE_OF_BYTE + 4 * ByteUtils.SIZE_OF_INT];

            // Write key length - leave byte for replica type
            offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            ByteUtils.writeInt(outputValue, keyBytes.length, offsetTillNow);

            // Write value length
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            ByteUtils.writeInt(outputValue, valBytes.length, offsetTillNow);

            // Write key
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            System.arraycopy(keyBytes, 0, outputValue, offsetTillNow, keyBytes.length);

            // Write value
            offsetTillNow += keyBytes.length;
            System.arraycopy(valBytes, 0, outputValue, offsetTillNow, valBytes.length);

            // Generate MR key - upper 8 bytes of 16 byte md5
            byte[] oldMd5 = ByteUtils.copy(md5er.digest(keyBytes), 0, 2 * ByteUtils.SIZE_OF_INT);
            ByteArray oldMd5ByteArray = new ByteArray(oldMd5);
            BytesWritable outputKey = new BytesWritable(oldMd5ToNewMd5.get(oldMd5ByteArray));

            int replicaType = 0;
            for(Integer partition: partitionList) {

                ByteUtils.writeInt(outputValue, partitionToNode[partition].getId(), 0);
                ByteUtils.writeInt(outputValue, partition, ByteUtils.SIZE_OF_INT);

                if(getSaveKeys()) {
                    ByteUtils.writeBytes(outputValue,
                                         replicaType,
                                         2 * ByteUtils.SIZE_OF_INT,
                                         ByteUtils.SIZE_OF_BYTE);
                }
                BytesWritable outputVal = new BytesWritable(outputValue);

                output.collect(outputKey, outputVal);
                replicaType++;

            }
            md5er.reset();
        }
    }

    @Test
    public void testCollision() throws IOException {
        testCollisionWithParams(500, 10);
        try {
            testCollisionWithParams(2 * (Short.MAX_VALUE + 1), (Short.MAX_VALUE + 1));
            fail("Should have failed since we exceed the number of tuple collisions possible");
        } catch(Exception e) {}
    }

    @SuppressWarnings( { "unchecked" })
    public void testCollisionWithParams(int totalElements, int maxCollisions) throws IOException {

        assertEquals(totalElements % maxCollisions, 0);

        // create test data
        Map<String, String> values = new HashMap<String, String>();
        List<String> valuesLeft = Lists.newArrayList();

        File testDir = TestUtils.createTempDir();
        File tempDir = new File(testDir, "temp");
        File outputDir = new File(testDir, "output");
        File storeDir = TestUtils.createTempDir(testDir);

        for(int i = 0; i < totalElements; i++) {
            values.put(Integer.toString(i), Integer.toString(i));
            valuesLeft.add(Integer.toString(i));
        }

        String storeName = "test";
        SerializerDefinition serDef = new SerializerDefinition("string");
        Cluster cluster = ServerTestUtils.getLocalCluster(1);
        Serializer<Object> serializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(serDef);

        // write test data to text file
        File inputFile = File.createTempFile("input", ".txt", testDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        byte[] currentMd5 = TestUtils.randomBytes(2 * ByteUtils.SIZE_OF_INT);
        int entryId = 0;
        for(Map.Entry<String, String> entry: values.entrySet()) {
            if(entryId % maxCollisions == 0) {
                currentMd5 = TestUtils.randomBytes(2 * ByteUtils.SIZE_OF_INT);
            }
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");

            byte[] oldMd5 = ByteUtils.copy(ByteUtils.md5(serializer.toBytes(entry.getKey())),
                                           0,
                                           2 * ByteUtils.SIZE_OF_INT);
            oldMd5ToNewMd5.put(new ByteArray(oldMd5), currentMd5);
            entryId++;
        }
        FileUtils.writeStringToFile(inputFile, contents.toString());

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
                CollidingTextStoreMapper.class,
                TextInputFormat.class,
                cluster,
                def,
                new Path(tempDir.getAbsolutePath()),
                new Path(outputDir.getAbsolutePath()),
                new Path(inputFile.getAbsolutePath()),
                CheckSumType.MD5,
                true,
                false,
                1024 * 1024 * 1024,
                -1,
                false,
                null);
        builder.build();

        File nodeFile = new File(outputDir, "node-0");
        File versionDir = new File(storeDir, "version-0");
        HdfsFetcher fetcher = new HdfsFetcher();
        fetcher.fetch(nodeFile.getAbsolutePath(), versionDir.getAbsolutePath());

        // Test if we work in the normal collision scenario open store
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine(storeName,
                                                                 new CustomBinarySearchStrategy(),
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
        List<String> valuesLeft2 = Lists.newArrayList(valuesLeft);
        ClosableIterator<ByteArray> keyIterator = engine.keys();
        int numElements = 0;
        while(keyIterator.hasNext()) {
            Object object = serializer.toObject(keyIterator.next().get());
            assertEquals(valuesLeft.remove(object), true);
            Assert.assertTrue(values.containsKey(object));
            numElements++;
        }

        Assert.assertEquals(numElements, values.size());
        Assert.assertEquals(valuesLeft.size(), 0);

        // ... and entry iterator
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = engine.entries();
        numElements = 0;
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            assertEquals(valuesLeft2.remove(serializer.toObject(entry.getFirst().get())), true);
            Assert.assertEquals(values.get(serializer.toObject(entry.getFirst().get())),
                                serializer.toObject(entry.getSecond().getValue()));
            numElements++;
        }

        Assert.assertEquals(numElements, values.size());
        Assert.assertEquals(valuesLeft2.size(), 0);
    }
}

package voldemort.store.readonly.mr;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.Utils;

public class HadoopStoreBuilderPatchTest {

    private final int NUM_NODES = 1;
    private final int NUM_VALUES = 10;
    private File testDir;
    private File tempDir;
    private File outputDir;
    private File previousDir;
    private File inputFile;
    private Cluster cluster;
    private StoreDefinition def;

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

    @Before
    public void setUp() throws IOException {
        Map<String, String> values = new HashMap<String, String>();
        for(int i = 0; i < NUM_VALUES; i++)
            values.put(Integer.toString(i), Integer.toString(i));
        testDir = TestUtils.createTempDir();
        tempDir = new File(testDir, "temp");
        outputDir = new File(testDir, "output");
        previousDir = TestUtils.createTempDir();

        // write test data to text file
        inputFile = File.createTempFile("input", ".txt", testDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        for(Map.Entry<String, String> entry: values.entrySet())
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        FileUtils.writeStringToFile(inputFile, contents.toString());

        cluster = ServerTestUtils.getLocalCluster(NUM_NODES);
        String storeName = "test";
        SerializerDefinition serDef = new SerializerDefinition("string");

        def = new StoreDefinitionBuilder().setName(storeName)
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

    }

    @Test
    public void testPatchWithNoChanges() {

        HadoopStoreBuilder builder = new HadoopStoreBuilder(new Configuration(),
                                                            TextStoreMapper.class,
                                                            TextInputFormat.class,
                                                            cluster,
                                                            def,
                                                            64 * 1024,
                                                            new Path(tempDir.getAbsolutePath()),
                                                            new Path(outputDir.getAbsolutePath()),
                                                            new Path(inputFile.getAbsolutePath()),
                                                            CheckSumType.MD5,
                                                            true,
                                                            null);
        builder.build();

        File newOutputDir = new File(testDir, Integer.toString(new Random().nextInt()));
        builder = new HadoopStoreBuilder(new Configuration(),
                                         TextStoreMapper.class,
                                         TextInputFormat.class,
                                         cluster,
                                         def,
                                         64 * 1024,
                                         new Path(tempDir.getAbsolutePath()),
                                         new Path(newOutputDir.getAbsolutePath()),
                                         new Path(inputFile.getAbsolutePath()),
                                         CheckSumType.MD5,
                                         true,
                                         new Path(outputDir.getAbsolutePath()));
        builder.build();

    }

    @Test
    public void testPreviousDirInitialization() throws IOException {

        // 1) Save keys off, but previous folder given

        HadoopStoreBuilder builder = new HadoopStoreBuilder(new Configuration(),
                                                            TextStoreMapper.class,
                                                            TextInputFormat.class,
                                                            cluster,
                                                            def,
                                                            64 * 1024,
                                                            new Path(tempDir.getAbsolutePath()),
                                                            new Path(outputDir.getAbsolutePath()),
                                                            new Path(inputFile.getAbsolutePath()),
                                                            CheckSumType.MD5,
                                                            false,
                                                            new Path(previousDir.getAbsolutePath()));
        try {
            builder.build();
            fail("Should have thrown an exception since save keys is false");
        } catch(Exception e) {}

        // 2) Node missing exception
        builder = new HadoopStoreBuilder(new Configuration(),
                                         TextStoreMapper.class,
                                         TextInputFormat.class,
                                         cluster,
                                         def,
                                         64 * 1024,
                                         new Path(tempDir.getAbsolutePath()),
                                         new Path(outputDir.getAbsolutePath()),
                                         new Path(inputFile.getAbsolutePath()),
                                         CheckSumType.MD5,
                                         true,
                                         new Path(previousDir.getAbsolutePath()));
        try {
            builder.build();
            fail("Should have thrown an exception because all nodes are missing");
        } catch(Exception e) {}

        // 3) Nodes present but metadata file not present
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            Utils.mkdirs(new File(previousDir, "node-" + Integer.toString(nodeId)));
        }

        try {
            builder.build();
            fail("Should have thrown an exception because metadata is missing");
        } catch(Exception e) {}

        // 4) Add metadata file but with no data
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            Utils.mkdirs(new File(previousDir, "node-" + Integer.toString(nodeId)));
            FileUtils.touch(new File(previousDir + "/node-" + Integer.toString(nodeId), ".metadata"));
        }

        try {
            builder.build();
            fail("Should have thrown an exception because metadata data is empty");
        } catch(Exception e) {}

        // 5) Add metadata file with incorrect version
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V0.getCode());

        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            FileUtils.writeStringToFile(new File(previousDir + "/node-" + Integer.toString(nodeId),
                                                 ".metadata"), metadata.toJsonString());
        }

        try {
            builder.build();
            fail("Should have thrown an exception because metadata data contains wrong version");
        } catch(Exception e) {}

        // 6) No data file
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            FileUtils.writeStringToFile(new File(previousDir + "/node-" + Integer.toString(nodeId),
                                                 ".metadata"), metadata.toJsonString());
        }

        try {
            builder.build();
            assertTrue(outputDir.exists());
        } catch(Exception e) {
            fail("Should not throw exception because it falls back to normal data generation");
        }
        Utils.rm(outputDir);

        // 7) Has some data files, with some extra
        for(int nodeId = 0; nodeId < NUM_NODES; nodeId++) {
            for(int partitionId: cluster.getNodeById(nodeId).getPartitionIds()) {
                for(int replicaType = 0; replicaType < def.getReplicationFactor(); replicaType++) {
                    FileUtils.touch(new File(previousDir + "/node-" + Integer.toString(nodeId),
                                             Integer.toString(partitionId) + "_"
                                                     + Integer.toString(replicaType) + "_0.data"));
                }
            }
            // Add a extra file to spoil the folder
            FileUtils.touch(new File(previousDir + "/node-" + Integer.toString(nodeId),
                                     "300_0_0.data"));
        }

        try {
            builder.build();
            fail("Should throw exceptions because of extra file");
        } catch(Exception e) {}
    }
}

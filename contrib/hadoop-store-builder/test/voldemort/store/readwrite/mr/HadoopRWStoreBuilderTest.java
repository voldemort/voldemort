package voldemort.store.readwrite.mr;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.After;
import org.junit.Before;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class HadoopRWStoreBuilderTest extends TestCase {

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);
    private static int numEntries = 20;
    private String storeName = "test";
    private Cluster cluster;
    private StoreDefinition storeDef;
    private VoldemortServer server;
    private AdminClient adminClient;

    public static class TextStoreMapper extends
            AbstractRWHadoopStoreBuilderMapper<LongWritable, Text> {

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

    @Override
    @Before
    public void setUp() throws Exception {

        cluster = ServerTestUtils.getLocalCluster(1);
        SerializerDefinition serDef = new SerializerDefinition("string");
        storeDef = new StoreDefinitionBuilder().setName(storeName)
                                               .setType(BdbStorageConfiguration.TYPE_NAME)
                                               .setKeySerializer(serDef)
                                               .setValueSerializer(serDef)
                                               .setRoutingPolicy(RoutingTier.CLIENT)
                                               .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                               .setReplicationFactor(1)
                                               .setRequiredReads(1)
                                               .setRequiredWrites(1)
                                               .build();

        // Store.xml file
        File tempConfDir = TestUtils.createTempDir();
        File storeXml = new File(tempConfDir, "stores.xml");
        FileUtils.writeStringToFile(storeXml,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(storeDef)));
        server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                      ServerTestUtils.createServerConfig(true,
                                                                                         0,
                                                                                         tempConfDir.getAbsolutePath(),
                                                                                         null,
                                                                                         storeXml.getAbsolutePath(),
                                                                                         new Properties()),
                                                      cluster);
        adminClient = new AdminClient(cluster, new AdminClientConfig().setMaxThreads(1));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        adminClient.stop();
        server.stop();
        socketStoreFactory.close();
    }

    public void testHadoopBuild() throws Exception {
        Map<String, String> values = new HashMap<String, String>();
        File inputDir = TestUtils.createTempDir();
        File tempDir = TestUtils.createTempDir();
        for(int i = 0; i < numEntries; i++)
            values.put(Integer.toString(i), Integer.toBinaryString(i));

        // write test data to text file
        File inputFile = File.createTempFile("input", ".txt", inputDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        for(Map.Entry<String, String> entry: values.entrySet())
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        FileUtils.writeStringToFile(inputFile, contents.toString());

        int hadoopNodeId = 123;
        int hadoopPushVersion = 456;
        HadoopRWStoreBuilder builder = new HadoopRWStoreBuilder(new Configuration(),
                                                                TextStoreMapper.class,
                                                                TextInputFormat.class,
                                                                cluster,
                                                                storeDef,
                                                                1,
                                                                hadoopNodeId,
                                                                hadoopPushVersion,
                                                                new Path(tempDir.getAbsolutePath()),
                                                                new Path(inputDir.getAbsolutePath()));
        builder.build();

        int countDown = numEntries * storeDef.getReplicationFactor();
        for(int partitionId = 0; partitionId < cluster.getNumberOfPartitions(); partitionId++) {
            Iterator<Pair<ByteArray, Versioned<byte[]>>> iter = adminClient.fetchEntries(0,
                                                                                         storeDef.getName(),
                                                                                         Lists.newArrayList(partitionId),
                                                                                         null,
                                                                                         false);
            while(iter.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> currentEntry = iter.next();
                String key = new String(currentEntry.getFirst().get());
                assertTrue(values.containsKey(key));

                String value = new String(currentEntry.getSecond().getValue());
                assertEquals(values.get(key), value);

                VectorClock vectorClock = (VectorClock) currentEntry.getSecond().getVersion();
                assertEquals(vectorClock.getEntries().size(), 1);
                assertEquals(vectorClock.getEntries().get(0).getNodeId(), hadoopNodeId);
                assertEquals(vectorClock.getEntries().get(0).getVersion(), hadoopPushVersion);
                countDown--;
            }
        }
        assertTrue(countDown == 0);

    }
}

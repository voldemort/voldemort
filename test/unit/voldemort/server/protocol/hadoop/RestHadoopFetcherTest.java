package voldemort.server.protocol.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ConfigurationException;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class RestHadoopFetcherTest {

    private Properties createDefaultProperties() {
        Properties props = new Properties();
        String baseDir = TestUtils.createTempDir().getAbsolutePath();
        props.setProperty("node.id", "0");
        props.put("voldemort.home", baseDir + "/node-" + "0");
        return props;
    }

    /*
     * Test the default case where RestHdfs is enabled and the class is
     * specified by default
     */
    @Test
    public void testVoldemortConfigDefault() {
        Properties props = createDefaultProperties();
        VoldemortConfig config = new VoldemortConfig(props);
        assertEquals(config.getFileFetcherClass(), null);
        assertEquals(config.isRestHdfsEnabled(), false);
    }

    /*
     * Test the mis-match cases between enable.readonly.rest.hdfs and
     * file.fetcher.class that will result in exception
     */
    @Test
    public void testVoldemortConfigMismatchNegative() {
        boolean exceptionFound = false;
        Properties props = createDefaultProperties();
        props.setProperty("enable.readonly.rest.hdfs", "true");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertTrue(exceptionFound);

        exceptionFound = false;
        props = createDefaultProperties();
        props.setProperty("file.fetcher.class",
                          "voldemort.server.protocol.hadoop.RestHadoopFetcher");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertTrue(exceptionFound);

        exceptionFound = false;
        props = createDefaultProperties();
        props.setProperty("enable.readonly.rest.hdfs", "true");
        props.setProperty("file.fetcher.class", "voldemort.store.readonly.fetcher.HdfsFetcher");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertTrue(exceptionFound);

        exceptionFound = false;
        props = createDefaultProperties();
        props.setProperty("enable.readonly.rest.hdfs", "false");
        props.setProperty("file.fetcher.class",
                          "voldemort.server.protocol.hadoop.RestHadoopFetcher");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    /*
     * Test the mis-match cases between enable.readonly.rest.hdfs and
     * file.fetcher.class that will NOT result in exception
     */
    @Test
    public void testVoldemortConfigMismatchPossitive() {
        boolean exceptionFound = false;
        Properties props = createDefaultProperties();
        props.setProperty("enable.readonly.rest.hdfs", "false");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertFalse(exceptionFound);

        exceptionFound = false;
        props = createDefaultProperties();
        props.setProperty("file.fetcher.class", "voldemort.store.readonly.fetcher.HdfsFetcher");
        try {
            VoldemortConfig config = new VoldemortConfig(props);
        } catch(ConfigurationException e) {
            exceptionFound = true;
        }
        assertFalse(exceptionFound);
    }

    /*
     * Test that read-only servers can be brought up properly with
     * RestHdfsClient enable while kerberos is not available for authentication
     */
    // @Test
    public void testRegularHdfsFetcherInstantiation() {
        VoldemortServer server = null;
        try {
            ClientRequestExecutorPool socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                         10000,
                                                                                         100000,
                                                                                         32 * 1024);
            Cluster currentCluster = ServerTestUtils.getLocalCluster(1, new int[][] { { 0, 1, 2, 3,
                    4, 5, 6 } });
            StoreDefinition store = new StoreDefinitionBuilder().setName("testRO")
                                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
            File file = File.createTempFile("ro-stores-", ".xml");
            FileUtils.writeStringToFile(file,
                                        new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(store)));
            String storeXmlFile = file.getAbsolutePath();
            Properties props = new Properties();
            props.setProperty("enable.readonly.rest.hdfs", "false");
            props.setProperty("file.fetcher.class", "voldemort.store.readonly.fetcher.HdfsFetcher");
            VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                        0,
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        storeXmlFile,
                                                                        props);
            server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          config,
                                                          currentCluster);
            AdminClient adminClient = ServerTestUtils.getAdminClient(currentCluster);

            // this is just some random admin operation to trigger the loading
            // of the fetcher class when request handler is instantiated
            adminClient.readonlyOps.getROMaxVersionDir(0, new ArrayList<String>());

            ServerTestUtils.stopVoldemortServer(server);
        } catch(IOException ioe) {
            // igore ioexception here
        }
    }

    /*
     * Test that read-only servers can be brought up properly with
     * RestHdfsClient enable while kerberos is not available for authentication
     */
    @Test
    public void testRestHdfsFetcherInstantiation() {
        VoldemortServer server = null;
        try {
            ClientRequestExecutorPool socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                         10000,
                                                                                         100000,
                                                                                         32 * 1024);
            Cluster currentCluster = ServerTestUtils.getLocalCluster(1, new int[][] { { 0, 1, 2, 3,
                    4, 5, 6 } });
            StoreDefinition store = new StoreDefinitionBuilder().setName("testRO")
                                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
            File file = File.createTempFile("ro-stores-", ".xml");
            FileUtils.writeStringToFile(file,
                                        new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(store)));
            String storeXmlFile = file.getAbsolutePath();
            Properties props = new Properties();
            props.setProperty("enable.readonly.rest.hdfs", "true");
            props.setProperty("file.fetcher.class",
                              "voldemort.server.protocol.hadoop.RestHadoopFetcher");
            VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                        0,
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        storeXmlFile,
                                                                        props);
            server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                          config,
                                                          currentCluster);
            AdminClient adminClient = ServerTestUtils.getAdminClient(currentCluster);

            // this is just some random admin operation to trigger the loading
            // of the fetcher class when request handler is instantiated
            adminClient.readonlyOps.getROMaxVersionDir(0, new ArrayList<String>());

            ServerTestUtils.stopVoldemortServer(server);
        } catch(IOException ioe) {
            // igore ioexception here
        }
    }
}

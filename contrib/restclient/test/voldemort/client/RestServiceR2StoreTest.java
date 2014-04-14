package voldemort.client;

import static voldemort.TestUtils.getClock;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.restclient.R2Store;
import voldemort.restclient.RESTClientConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Lists;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.http.client.HttpClientFactory;

public class RestServiceR2StoreTest extends AbstractByteArrayStoreTest {

    private static Store<ByteArray, byte[], byte[]> store;
    private static HttpClientFactory clientFactory;

    private static final String STORE_NAME = "test";
    private static String storesXmlfile = "test/common/coordinator/config/stores.xml";
    private static String clusterXmlFile = "test/common/coordinator/config/rest_cluster.xml";
    protected static final ClusterMapper clusterMapper = new ClusterMapper();

    String[] bootStrapUrls = null;
    private VoldemortServer[] servers;
    private Cluster cluster;
    public static String socketUrl = "";
    private int nodeId;

    @Parameters
    public static Collection<Object[]> modes() {
        Object[][] data = new Object[][] { { true }, { false } };
        return Arrays.asList(data);
    }

    @Override
    @Before
    public void setUp() {
        final int numServers = 1;
        this.nodeId = 0;
        servers = new VoldemortServer[numServers];
        try {

            // Setup the cluster
            Properties props = new Properties();
            props.setProperty("rest.enable", "true");
            props.setProperty("http.enable", "true");

            Cluster customCluster = clusterMapper.readCluster(new FileReader(clusterXmlFile), false);

            cluster = ServerTestUtils.startVoldemortCluster(servers,
                                                            null,
                                                            clusterXmlFile,
                                                            storesXmlfile,
                                                            props,
                                                            customCluster);

        } catch(IOException e) {
            fail("Failure to setup the cluster");
        }

        // Creating R2Store
        RESTClientConfig restClientConfig = new RESTClientConfig();
        restClientConfig.setHttpBootstrapURL("http://localhost:"
                                             + cluster.getNodeById(0).getRestPort())
                        .setTimeoutMs(10000, TimeUnit.MILLISECONDS)
                        .setMaxR2ConnectionPoolSize(100);
        clientFactory = new HttpClientFactory();
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(HttpClientFactory.POOL_SIZE_KEY,
                       Integer.toString(restClientConfig.getMaxR2ConnectionPoolSize()));
        TransportClient transportClient = clientFactory.getClient(properties);
        R2Store r2Store = new R2Store(STORE_NAME,
                                      restClientConfig.getHttpBootstrapURL(),
                                      "0",
                                      transportClient,
                                      restClientConfig,
                                      0);
        store = r2Store;

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        store.close();
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() throws Exception {
        return store;
    }

    public VectorClock getNewIncrementedVectorClock() {
        VectorClock vectorClock = new VectorClock();
        vectorClock.incrementVersion(this.nodeId, System.currentTimeMillis());
        return vectorClock;
    }

    @Override
    @Test
    public void testFetchedEqualsPut() throws Exception {
        System.out.println("                    Testing Fetchhed equals put                    ");
        ByteArray key = getKey();
        Store<ByteArray, byte[], byte[]> store = getStore();
        VectorClock clock = getClock(1, 1, 2, 3, 3, 4);
        byte[] value = getValue();
        System.out.println("Value chosen : " + value);
        List<Versioned<byte[]>> resultList = store.get(key, null);
        assertNotNull("Null result list obtained from a get request", resultList);
        assertEquals("Store not empty at start!", 0, resultList.size());
        Versioned<byte[]> versioned = new Versioned<byte[]>(value, clock);
        store.put(key, versioned, null);

        List<Versioned<byte[]>> found = store.get(key, null);
        assertEquals("Should only be one version stored.", 1, found.size());

        System.out.println("individual bytes");
        System.out.println("input");
        printBytes(versioned.getValue());

        System.out.println("found");
        printBytes(found.get(0).getValue());
        assertTrue("Values not equal!", valuesEqual(versioned.getValue(), found.get(0).getValue()));
    }

    public void printBytes(byte[] in) {
        System.out.println("Lenght: " + in.length);
        for(int i = 0; i < in.length; i++)
            System.out.print(in[i] + ",");
        System.out.println("\n");
    }

    @Override
    @Test
    public void testEmptyByteArray() throws Exception {
        System.out.println("                    Testing empty ByteArray                    ");
        // No-op ... Empty values are not allowed in REST client
    }

    @Override
    @Test
    public void testNullKeys() throws Exception {
        System.out.println("                    Testing null Keys                    ");
        Store<ByteArray, byte[], byte[]> store = getStore();
        try {
            store.put(null, new Versioned<byte[]>(getValue(), getNewIncrementedVectorClock()), null);
            fail("Store should not put null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        } catch(NullPointerException npe) {
            // this is good
        }
        try {
            store.get(null, null);
            fail("Store should not get null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        } catch(NullPointerException npe) {
            // this is good
        }
        try {
            store.getAll(null, null);
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        } catch(NullPointerException npe) {
            // this is good
        }
        try {
            store.getAll(Collections.<ByteArray> singleton(null),
                         Collections.<ByteArray, byte[]> singletonMap(null, null));
            fail("Store should not getAll null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        } catch(NullPointerException npe) {
            // this is good
        }
        try {
            store.delete(null, new VectorClock());
            fail("Store should not delete null keys!");
        } catch(IllegalArgumentException e) {
            // this is good
        } catch(NullPointerException npe) {
            // this is good
        }
    }

    @Override
    @Test
    public void testGetVersions() throws Exception {
        List<ByteArray> keys = getKeys(2);
        ByteArray key = keys.get(0);
        byte[] value = getValue();
        VectorClock vc = getClock(0, 0);
        Store<ByteArray, byte[], byte[]> store = getStore();
        store.put(key, Versioned.value(value, vc), null);
        List<Versioned<byte[]>> versioneds = store.get(key, null);
        List<Version> versions = store.getVersions(key);
        assertEquals(1, versioneds.size());
        assertTrue(versions.size() > 0);
        for(int i = 0; i < versions.size(); i++)
            assertEquals(versioneds.get(0).getVersion(), versions.get(i));

        assertEquals(0, store.getVersions(keys.get(1)).size());
    }

    @Override
    @Test
    public void testGetAll() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();
        int putCount = 10;
        List<ByteArray> keys = getKeys(putCount);
        List<byte[]> values = getValues(putCount);
        assertEquals(putCount, values.size());
        for(int i = 0; i < putCount; i++) {
            VectorClock vc = getClock(0, 0);
            store.put(keys.get(i), new Versioned<byte[]>(values.get(i), vc), null);
        }

        int countForGet = putCount / 2;
        List<ByteArray> keysForGet = keys.subList(0, countForGet);
        List<byte[]> valuesForGet = values.subList(0, countForGet);
        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keysForGet, null);
        assertGetAllValues(keysForGet, valuesForGet, result);
    }

    @Test
    public void testGetAllWithBinaryData() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();
        List<ByteArray> keys = Lists.newArrayList();
        List<byte[]> values = Lists.newArrayList();

        // The Byte 0x8c is interesting, because if you use GetContent method of
        // MimeBodyPart it gets converted to 0xc2 and 0x8c
        // This thread tracks this question
        // http://stackoverflow.com/questions/23023583/mimebodypart-getcontent-corrupts-binary-data

        byte[] interestingByte = new byte[] { (byte) 0x8c };
        ByteArray interestingKey = new ByteArray(interestingByte);

        keys.add(interestingKey);
        values.add(interestingByte);

        // Add all possible byte values
        byte[] allPossibleBytes = getAllPossibleBytes();
        ByteArray allPossibleKey = new ByteArray(allPossibleBytes);
        keys.add(allPossibleKey);
        values.add(allPossibleBytes);

        assertEquals(keys.size(), values.size());
        int count = keys.size();

        for (int i = 0; i < count; i++) {
            VectorClock vc = getClock(0, 0);
            store.put(keys.get(i), new Versioned<byte[]>(values.get(i), vc), null);
        }

        Map<ByteArray, List<Versioned<byte[]>>> result = store.getAll(keys, null);
        assertGetAllValues(keys, values, result);
    }

    @Test
    public void testGetWithBinaryData() throws Exception {
        Store<ByteArray, byte[], byte[]> store = getStore();

        byte[] allPossibleBytes = getAllPossibleBytes();
        ByteArray key = new ByteArray(allPossibleBytes);
        VectorClock vc = getClock(0, 0);
        Versioned<byte[]> versioned = new Versioned<byte[]>(allPossibleBytes, vc);
        store.put(key, versioned, null);

        List<Versioned<byte[]>> found = store.get(key, null);
        assertEquals("Should only be one version stored.", 1, found.size());

        System.out.println("individual bytes");
        System.out.println("input");
        printBytes(versioned.getValue());

        System.out.println("found");
        printBytes(found.get(0).getValue());
        assertTrue("Values not equal!", valuesEqual(versioned.getValue(), found.get(0).getValue()));
    }
}

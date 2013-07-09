package voldemort.restclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class RestServerAPITest {

    private static final Logger logger = Logger.getLogger(RestServerAPITest.class);

    /*
     * TODO REST-Server temporaily hard coded the store name and port. This
     * should be formally obtained from stores.xml and cluster.xml
     */

    private final static R2Store r2store = new R2Store("http://localhost:8081", "test", "2");
    private final static Store<ByteArray, byte[], byte[]> store = r2store;
    private static VoldemortConfig config = null;
    private static VoldemortServer server;

    private static ByteArray key;
    private static Versioned<byte[]> value;
    private static List<Versioned<byte[]>> input, output;
    private static VectorClock vectorClock;

    @BeforeClass
    public static void oneTimeSetUp() {
        config = VoldemortConfig.loadFromVoldemortHome("config/single_node_rest_server/");
        key = new ByteArray("key1".getBytes());
        vectorClock = new VectorClock();
        vectorClock.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        value = new Versioned<byte[]>("value1".getBytes(), vectorClock);
        server = new VoldemortServer(config);
        if(!server.isStarted())
            server.start();
        logger.info("********************Starting REST Server********************");
        deleteCreatedKeys(key);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if(server != null && server.isStarted()) {
            server.stop();
        }
    }

    @After
    public void cleanUp() {
        deleteCreatedKeys(key);
        input = null;
        output = null;
    }

    public static void deleteCreatedKeys(ByteArray key) {
        output = store.get(key, null);
        for(Versioned<byte[]> versionedValue: output) {
            store.delete(key, versionedValue.getVersion());
        }
    }

    /**
     * Basic put test
     * 
     * @throws UnsupportedEncodingException
     */

    @Test
    public void testGetAfterPut() throws UnsupportedEncodingException {
        logger.info("\n\n********************  Testing Get After Put *******************\n\n");
        input = new ArrayList<Versioned<byte[]>>();
        input.add(value);
        store.put(key, value, null);
        output = store.get(key, null);
        assertEquals(input, output);
    }

    /**
     * Basic getall test
     */

    @Test
    public void testGetAll() {

        logger.info("\n\n********************  Testing Get All *******************\n\n");
        VectorClock vectorClock1 = new VectorClock();
        vectorClock1.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        ByteArray key2 = new ByteArray("key2".getBytes());
        Versioned<byte[]> value2 = new Versioned<byte[]>("value2".getBytes(), vectorClock1);
        store.put(key2, value2, null);

        vectorClock1 = new VectorClock();
        vectorClock1.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        ByteArray key3 = new ByteArray("key3".getBytes());
        Versioned<byte[]> value3 = new Versioned<byte[]>("value3".getBytes(), vectorClock1);
        store.put(key3, value3, null);

        Map<ByteArray, List<Versioned<byte[]>>> input = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        List<Versioned<byte[]>> valuesList2 = new ArrayList<Versioned<byte[]>>();
        valuesList2.add(value2);
        input.put(key2, valuesList2);
        List<Versioned<byte[]>> valuesList3 = new ArrayList<Versioned<byte[]>>();
        valuesList3.add(value3);
        input.put(key3, valuesList3);

        Map<ByteArray, List<Versioned<byte[]>>> output = store.getAll(input.keySet(), null);

        assertEquals(input, output);

        // cleanup specific to this test case
        deleteCreatedKeys(key2);
        deleteCreatedKeys(key3);
    }

    /**
     * test getall with k1,k2. k1 has v1,v2 and k2 has v3
     */

    @Test
    public void testGetAllWithConflictingVersions() {

        logger.info("\n\n********************  Testing Get All with multiple versions *******************\n\n");
        Map<ByteArray, List<Versioned<byte[]>>> input = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        List<Versioned<byte[]>> valuesList2 = new ArrayList<Versioned<byte[]>>();

        VectorClock vectorClock1 = new VectorClock();
        vectorClock1.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        ByteArray key2 = new ByteArray("key22".getBytes());
        Versioned<byte[]> value1 = new Versioned<byte[]>("value22".getBytes(), vectorClock1);
        store.put(key2, value1, null);
        valuesList2.add(value1);

        VectorClock vectorClock2 = new VectorClock();
        vectorClock2.incrementVersion(1, System.currentTimeMillis());
        Versioned<byte[]> value2 = new Versioned<byte[]>("value23".getBytes(), vectorClock2);
        store.put(key2, value2, null);
        valuesList2.add(value2);
        input.put(key2, valuesList2);

        List<Versioned<byte[]>> valuesList3 = new ArrayList<Versioned<byte[]>>();
        VectorClock vectorClock3 = new VectorClock();
        vectorClock3.incrementVersion(config.getNodeId(), System.currentTimeMillis());
        ByteArray key3 = new ByteArray("key23".getBytes());
        Versioned<byte[]> value3 = new Versioned<byte[]>("value43".getBytes(), vectorClock3);
        store.put(key3, value3, null);
        valuesList3.add(value3);
        input.put(key3, valuesList3);

        Map<ByteArray, List<Versioned<byte[]>>> output = store.getAll(input.keySet(), null);
        assertEquals(input, output);

        // cleanup specific to this test case
        deleteCreatedKeys(key2);
        deleteCreatedKeys(key3);

    }

    /**
     * basic test for delete
     */

    @Test
    public void testDelete() {
        logger.info("\n\n********************  Testing Delete *******************\n\n");
        input = new ArrayList<Versioned<byte[]>>();
        input.add(value);
        store.put(key, value, null);
        output = store.get(key, null);
        if(!output.equals(input)) {
            fail("key does not exist after put");
        } else {
            boolean result = store.delete(key, output.get(0).getVersion());
            if(!result) {
                fail("Notthing to delete");
            } else {
                output = store.get(key, null);
                assertTrue(output.size() == 0);
            }
        }
    }

    /**
     * test delete on a k with v1,v2 recursively to see if the key is completely
     * deleted
     */

    @Test
    public void testRecursiveDeleteOnSameKeyWithTwoVersions() {
        logger.info("\n\n********************  Testing recursive Delete on a key with two versions *******************\n\n");

        store.put(key, value, null);
        List<Versioned<byte[]>> resultList, previousResultList;
        resultList = store.get(key, null);

        VectorClock vectorClock2 = new VectorClock();
        vectorClock2.incrementVersion(config.getNodeId() + 1, System.currentTimeMillis());
        Versioned<byte[]> value2 = new Versioned<byte[]>("value32".getBytes(), vectorClock2);
        store.put(key, value2, null);
        previousResultList = resultList;
        resultList = store.get(key, null);

        if(resultList.size() != previousResultList.size() + 1) {
            fail("Failed to add another version");
        } else {
            previousResultList = resultList;
            store.delete(key, value.getVersion());
            resultList = store.get(key, null);
            if(resultList.size() != previousResultList.size() - 1) {
                fail("Delete failed");
            } else {
                previousResultList = resultList;
                store.delete(key, value2.getVersion());
                resultList = store.get(key, null);
                assertTrue(resultList.size() == previousResultList.size() - 1);
            }
        }

    }

}

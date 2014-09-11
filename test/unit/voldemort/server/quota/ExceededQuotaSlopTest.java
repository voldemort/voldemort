package voldemort.server.quota;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.server.VoldemortServer;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.store.StoreDefinition;
import voldemort.store.quota.QuotaType;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.routed.NodeValue;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class ExceededQuotaSlopTest {

    private SocketStoreClientFactory factory;
    private StoreClient<String, String> storeClient;
    private Cluster cluster;
    private AdminClient adminClient;
    private VoldemortServer[] servers = new VoldemortServer[2];
    private int partitionMap[][] = { { 0 }, { 1 } };
    private HashMap<Integer, Integer> partitionToNodeMap;
    private static final String quotaStoreName = "voldsys$_store_quotas";
    private static final String encodingType = "UTF-8";
    private static final Integer SLOP_FREQUENCY_MS = 5000;
    private final Logger logger = Logger.getLogger(ExceededQuotaSlopTest.class);

    String storesxml = "test/common/voldemort/config/single-store-with-handoff-strategy.xml";
    String storeName = "test";

    @Before
    public void setup() throws IOException {
        fillPartitionsToNodeMap();
        startVoldemortTwoNodeCluster();
    }

    public void fillPartitionsToNodeMap() {
        partitionToNodeMap = new HashMap<Integer, Integer>();
        int partitionId = 0;
        for(int i = 0; i < servers.length; i++) {
            for(int j = 0; j < partitionMap[i].length; j++) {
                partitionToNodeMap.put(partitionId++, i);
            }
        }
    }

    public void startVoldemortTwoNodeCluster() throws IOException {
        Properties serverProperties = new Properties();
        serverProperties.setProperty("pusher.type", StreamingSlopPusherJob.TYPE_NAME);
        serverProperties.setProperty("slop.frequency.ms", SLOP_FREQUENCY_MS.toString());
        serverProperties.setProperty("auto.purge.dead.slops", "true");
        serverProperties.setProperty("enable.quota.limiting", "true");
        cluster = ServerTestUtils.startVoldemortCluster(servers,
                                                        partitionMap,
                                                        serverProperties,
                                                        storesxml);
    }

    public void setGetPutQuotasForEachServer() throws Exception {
        Properties adminProperties = new Properties();
        adminProperties.setProperty("max_connections", "2");
        adminClient = new AdminClient(cluster,
                                      new AdminClientConfig(adminProperties),
                                      new ClientConfig());

        Map<Pair<Integer, QuotaType>, Integer> throughPutMap = new HashMap<Pair<Integer, QuotaType>, Integer>();
        // Set Node0 Quota
        throughPutMap.put(new Pair<Integer, QuotaType>(0, QuotaType.PUT_THROUGHPUT), 5);
        throughPutMap.put(new Pair<Integer, QuotaType>(0, QuotaType.GET_THROUGHPUT), 20);

        // Set Node1 Quota
        throughPutMap.put(new Pair<Integer, QuotaType>(1, QuotaType.PUT_THROUGHPUT), 2);
        throughPutMap.put(new Pair<Integer, QuotaType>(1, QuotaType.GET_THROUGHPUT), 20);

        for(Entry<Pair<Integer, QuotaType>, Integer> throughPut: throughPutMap.entrySet()) {

            int nodeId = throughPut.getKey().getFirst();
            QuotaType type = throughPut.getKey().getSecond();
            int value = throughPut.getValue();

            VectorClock clock = VectorClockUtils.makeClockWithCurrentTime(cluster.getNodeIds());
            NodeValue<ByteArray, byte[]> operationValue = new NodeValue<ByteArray, byte[]>(nodeId,
                                                                                           new ByteArray(getKeyBytes(type)),
                                                                                           new Versioned<byte[]>(ByteUtils.getBytes(Integer.toString(value),
                                                                                                                                    encodingType),
                                                                                                                 clock));
            try {
                adminClient.storeOps.putNodeKeyValue(quotaStoreName, operationValue);
            } catch(Exception e) {
                throw new Exception("Exception when setting put quota for node " + nodeId
                                    + " Operation " + type + "." + e.getMessage());
            }
        }
    }

    private byte[] getKeyBytes(QuotaType quotaType) {
        return ByteUtils.getBytes(QuotaUtils.makeQuotaKey(storeName,
                                                          QuotaType.valueOf(quotaType.toString())),
                                  encodingType);
    }

    private HashMap<String, String> generateKeysForMasterNode(int numKeys) throws IOException {
        // Assumes master node is 0 and generates keys that goes to master node
        HashMap<String, String> keyValuePairs = new HashMap<String, String>();
        StoreDefinitionsMapper storedDefMapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefs = storedDefMapper.readStoreList(new File(storesxml));
        StoreDefinition testStoreDef = storeDefs.get(0);
        BaseStoreRoutingPlan baseStoreRoutingPlan = new BaseStoreRoutingPlan(cluster, testStoreDef);

        /*
         * Generating simple key values pairs of the form 3:3 where key and
         * value are same but route to the Master node's partition
         */

        int key = 0;
        int partiionId = -1;
        for(int count = 0; count < numKeys;) {
            byte[] keyBytes = Integer.toString(key).getBytes();
            partiionId = baseStoreRoutingPlan.getMasterPartitionId(keyBytes);
            if(partitionToNodeMap.get(partiionId) == 0) {
                keyValuePairs.put(String.valueOf(key), String.valueOf(key));
                count++;
            }
            key++;
        }
        return keyValuePairs;
    }

    @Test
    public void testAsyncWritesSloppedOnQuotaExceeed() throws Exception {

        // Set quotas on each server
        setGetPutQuotasForEachServer();

        // This test is non-deterministic.
        // 1) The QuotaException is thrown by SerialPut, but parallelPut ignores
        // QuotaException and throws InsufficientOperationalNodesException
        // as the QuotaExceptions are silently (warning logs) eaten away.
        // 2) When you increase the key/value pairs beyond 100, slops start
        // randomly failing as there are only 2 nodes and backlog of slops on
        // other node causes the slop to be dropped
        // But when you set this <= 100, no Put receives a QuotaException

        // Correct way is creating a mock SocketStore which can inject
        // failures of QuotaException and test the pipeline actions and handling
        // by node. The Server side needs a mock time where you can freeze the
        // time and see if it fails after the quota. But saving these ones for
        // later.

        HashMap<String, String> keyValuePairsToMasterNode = generateKeysForMasterNode(100);

        String bootStrapUrl = "tcp://" + cluster.getNodeById(0).getHost() + ":"
                              + cluster.getNodeById(0).getSocketPort();
        factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootStrapUrl));
        storeClient = factory.getStoreClient(storeName);

        int numPutExceptions = 0;

        ArrayList<String> putKeysThatSucceeded = new ArrayList<String>();

        for(Entry<String, String> entry: keyValuePairsToMasterNode.entrySet()) {
            try {
                // do a put on node 0
                storeClient.put(entry.getKey(), entry.getValue());
                putKeysThatSucceeded.add(entry.getValue());
            } catch(VoldemortException e) {
                logger.warn(e, e);
                numPutExceptions++;
            }
        }

        logger.info("#Puts that failed due to Exception: " + numPutExceptions);

        // wait for the slop pushed to finish its job
        Thread.sleep(2 * SLOP_FREQUENCY_MS);

        // check the secondary node (node id:1) to see if all successful put
        // keys exist
        for(String val: putKeysThatSucceeded) {
            int nodeId = 1;
            // do a get on node 1
            List<Versioned<byte[]>> valueBytes = adminClient.storeOps.getNodeKey(storeName,
                                                                                 nodeId,
                                                                                 new ByteArray(ByteUtils.getBytes(val,
                                                                                                                  encodingType)));

            assertEquals("Expect 1 value from PUT " + val, 1, valueBytes.size());
            assertEquals("GET returned different value than put",
                         val,
                         ByteUtils.getString(valueBytes.get(0).getValue(), encodingType));
        }

        int numDeleteExceptions = 0;
        ArrayList<String> deleteKeysThatSucceeded = new ArrayList<String>();
        for(Entry<String, String> entry: keyValuePairsToMasterNode.entrySet()) {
            try {
                // do a put on node 0
                storeClient.delete(entry.getKey());
                deleteKeysThatSucceeded.add(entry.getValue());
            } catch(VoldemortException e) {
                logger.warn(e, e);
                numDeleteExceptions++;
            }
        }
        logger.info("#Deletes that failed due to Exceptions: " + numDeleteExceptions);

        // wait for the slop pushed to finish its job
        Thread.sleep(2 * SLOP_FREQUENCY_MS);

        for(String val: deleteKeysThatSucceeded) {
            for(int nodeId: cluster.getNodeIds()) {
                // do a get on node 1
                List<Versioned<byte[]>> valueBytes = adminClient.storeOps.getNodeKey(storeName,
                                                                                     nodeId,
                                                                                     new ByteArray(ByteUtils.getBytes(val,
                                                                                                                      encodingType)));

                assertTrue("Deleted value should be null or zero on node " + nodeId,
                           valueBytes == null || valueBytes.size() == 0);
            }
        }

    }

    @After
    public void tearDown() {
        for(VoldemortServer server: servers) {
            server.stop();
        }
        adminClient.close();
    }
}

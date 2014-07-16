package voldemort.server.quota;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
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
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.quota.QuotaType;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.routed.NodeValue;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class AsyncPutQuotaTest {

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
    private final Logger logger = Logger.getLogger(AsyncPutQuotaTest.class);

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

        VectorClock clock = VectorClockUtils.makeClockWithCurrentTime(cluster.getNodeIds());

        // set put quota = 5 for first server
        NodeValue<ByteArray, byte[]> putNodeValueForNode0 = new NodeValue<ByteArray, byte[]>(0,
                                                                                             new ByteArray(getKeyBytes(QuotaType.PUT_THROUGHPUT)),
                                                                                             new Versioned<byte[]>(ByteUtils.getBytes("5",
                                                                                                                                      encodingType),
                                                                                                                   clock));
        try {
            adminClient.storeOps.putNodeKeyValue(quotaStoreName, putNodeValueForNode0);
        } catch(Exception e) {
            throw new Exception("Exception when setting put quota for node 0. " + e.getMessage());
        }

        // set get quota = 20 for first server
        clock = VectorClockUtils.makeClockWithCurrentTime(cluster.getNodeIds());
        NodeValue<ByteArray, byte[]> getNodeValueForNode0 = new NodeValue<ByteArray, byte[]>(0,
                                                                                             new ByteArray(getKeyBytes(QuotaType.GET_THROUGHPUT)),
                                                                                             new Versioned<byte[]>(ByteUtils.getBytes("20",
                                                                                                                                      encodingType),
                                                                                                                   clock));
        try {
            adminClient.storeOps.putNodeKeyValue(quotaStoreName, getNodeValueForNode0);
        } catch(Exception e) {
            throw new Exception("Exception when setting get quota for node 0. " + e.getMessage());
        }

        // set put quota = 2 for second server
        clock = VectorClockUtils.makeClockWithCurrentTime(cluster.getNodeIds());
        NodeValue<ByteArray, byte[]> putNodeValueForNode1 = new NodeValue<ByteArray, byte[]>(1,
                                                                                             new ByteArray(getKeyBytes(QuotaType.PUT_THROUGHPUT)),
                                                                                             new Versioned<byte[]>(ByteUtils.getBytes("2",
                                                                                                                                      encodingType),
                                                                                                                   clock));
        try {
            adminClient.storeOps.putNodeKeyValue(quotaStoreName, putNodeValueForNode1);
        } catch(Exception e) {
            throw new Exception("Exception when setting put quota for node 1. " + e.getMessage());
        }
        // set get quota = 20 for second server
        clock = VectorClockUtils.makeClockWithCurrentTime(cluster.getNodeIds());
        NodeValue<ByteArray, byte[]> getNodeValueForNode1 = new NodeValue<ByteArray, byte[]>(1,
                                                                                             new ByteArray(getKeyBytes(QuotaType.GET_THROUGHPUT)),
                                                                                             new Versioned<byte[]>(ByteUtils.getBytes("20",
                                                                                                                                      encodingType),
                                                                                                                   clock));
        try {
            adminClient.storeOps.putNodeKeyValue(quotaStoreName, getNodeValueForNode1);
        } catch(Exception e) {
            throw new Exception("Exception when setting get quota for node 1. " + e.getMessage());
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

        // Generate 15 key value pairs that route to Master node
        HashMap<String, String> keyValuePairsToMasterNode = generateKeysForMasterNode(15);

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
            } catch(QuotaExceededException quotaexception) {
                numPutExceptions++;
            }
        }

        logger.info("#Puts that failed due to QuotaExceededException: " + numPutExceptions);

        // wait for the slop pushed to finish its job
        Thread.sleep(2 * SLOP_FREQUENCY_MS);

        // check the secondary node (node id:1) to see if all successful put
        // keys exist
        int numNullEmptyIncoorectValue = 0;
        for(String val: putKeysThatSucceeded) {
            int nodeId = 1;
            // do a get on node 1
            List<Versioned<byte[]>> valueBytes = adminClient.storeOps.getNodeKey(storeName,
                                                                                 nodeId,
                                                                                 new ByteArray(ByteUtils.getBytes(val,
                                                                                                                  encodingType)));

            if(valueBytes == null || valueBytes.size() == 0
               || !(val.equals(ByteUtils.getString(valueBytes.get(0).getValue(), encodingType)))) {
                logger.info("Either the value returned for key: " + val
                            + " is null/empty/not same as that put");
                numNullEmptyIncoorectValue++;
                continue;
            }
            logger.info("key: " + val + " value: "
                        + ByteUtils.getString(valueBytes.get(0).getValue(), encodingType));
        }
        assertTrue("Not all expected keys found in secondary node", numNullEmptyIncoorectValue == 0);
    }

    @After
    public void tearDown() {
        for(VoldemortServer server: servers) {
            server.stop();
        }
        adminClient.close();
    }
}

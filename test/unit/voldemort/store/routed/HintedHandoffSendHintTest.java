package voldemort.store.routed;

import static org.junit.Assert.assertTrue;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.cluster.failuredetector.MutableStoreVerifier;
import voldemort.cluster.failuredetector.ThresholdFailureDetector;
import voldemort.common.service.ServiceType;
import voldemort.common.service.VoldemortService;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SlopSerializer;
import voldemort.server.RequestRoutingType;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.storage.StorageService;
import voldemort.store.ForceFailStore;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class HintedHandoffSendHintTest {

    private final static Logger logger = Logger.getLogger(HintedHandoffSendHintTest.class);

    private final static String STORE_NAME = "test";
    private final static String SLOP_STORE_NAME = "slop";

    private final static int NUM_NODES_TOTAL = 9;
    private final static int NUM_NODES_FAILED = 4;

    private int REPLICATION_FACTOR = 3;
    private final static int P_READS = 1;
    private final static int R_READS = 1;
    private int P_WRITES = 1;
    private int R_WRITES = 1;

    private final static int KEY_LENGTH = 32;
    private final static int VALUE_LENGTH = 32;
    private final static int SOCKET_TIMEOUT_MS = 500;

    private final Class<? extends FailureDetector> failureDetectorCls = ThresholdFailureDetector.class;
    private final HintedHandoffStrategyType hintRoutingStrategy;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    private final Map<Integer, ForceFailStore<ByteArray, byte[], byte[]>> forceFailStores = new ConcurrentHashMap<Integer, ForceFailStore<ByteArray, byte[], byte[]>>();
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = new ConcurrentHashMap<Integer, Store<ByteArray, Slop, byte[]>>();
    private final Map<Integer, NonblockingStore> socketTestStores = new HashMap<Integer, NonblockingStore>();
    private final Map<Integer, NonblockingStore> socketSlopStores = new HashMap<Integer, NonblockingStore>();
    private final Map<Integer, SlopStorageEngine> slopStorageEngines = new ConcurrentHashMap<Integer, SlopStorageEngine>();
    private final Multimap<ByteArray, Integer> keysToNodes = HashMultimap.create();
    private final Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();
    private final Map<Integer, VoldemortServer> voldemortServers = new HashMap<Integer, VoldemortServer>();

    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private RoutingStrategy strategy;
    private RoutedStore routedStore;
    private final List<ByteArray> keyList = new ArrayList<ByteArray>();

    final static class SocketStoreClientFactoryForTest {

        private final String storeName;
        private final String slopStoreName;
        private final ClientRequestExecutorPool storeFactory;
        private final ClientConfig config;

        public SocketStoreClientFactoryForTest(String testStoreName, String slopStoreName) {
            this.storeName = testStoreName;
            this.slopStoreName = slopStoreName;
            config = new ClientConfig();
            storeFactory = new ClientRequestExecutorPool(config.getSelectors(),
                                                         config.getMaxConnectionsPerNode(),
                                                         config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                         SOCKET_TIMEOUT_MS,
                                                         config.getSocketBufferSize(),
                                                         config.getSocketKeepAlive(),
                                                         false,
                                                         new String());
        }

        protected SocketStore getSocketTestStoreByNode(Node node) {
            return storeFactory.create(storeName,
                                       node.getHost(),
                                       node.getSocketPort(),
                                       config.getRequestFormatType(),
                                       RequestRoutingType.getRequestRoutingType(false, false));
        }

        protected SocketStore getSocketSlopStoreByNode(Node node) {
            return storeFactory.create(slopStoreName,
                                       node.getHost(),
                                       node.getSocketPort(),
                                       config.getRequestFormatType(),
                                       RequestRoutingType.getRequestRoutingType(false, false));
        }
    }

    public HintedHandoffSendHintTest(HintedHandoffStrategyType hintRoutingStrategy,
                                     int replicationFactor,
                                     int requiredWrites,
                                     int preferredWrites) {
        this.hintRoutingStrategy = hintRoutingStrategy;
        this.REPLICATION_FACTOR = replicationFactor;
        this.R_WRITES = requiredWrites;
        this.P_WRITES = preferredWrites;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 3, 1, 1 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 3, 1, 2 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 3, 1, 3 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 3, 2, 2 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 3, 2, 3 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 2, 1, 1 },
                { HintedHandoffStrategyType.CONSISTENT_STRATEGY, 2, 1, 2 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 3, 1, 1 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 3, 1, 2 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 3, 1, 3 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 3, 2, 2 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 3, 2, 3 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 2, 1, 1 },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY, 2, 1, 2 } });
    }

    private StoreDefinition getStoreDef() {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(STORE_NAME)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.CLIENT)
                                           .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                           .setReplicationFactor(REPLICATION_FACTOR)
                                           .setPreferredReads(P_READS)
                                           .setRequiredReads(R_READS)
                                           .setPreferredWrites(P_WRITES)
                                           .setRequiredWrites(R_WRITES)
                                           .setHintedHandoffStrategy(this.hintRoutingStrategy)
                                           .build();
    }

    @Before
    public void setUp() throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug("Test Started: replication[" + REPLICATION_FACTOR + "], preferredW["
                         + P_WRITES + "], requiredW[" + R_WRITES + "]");
        }
        cluster = getNineNodeCluster();
        storeDef = getStoreDef();

        // create voldemort servers
        for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
            SocketStoreFactory socketStoreFactory;
            socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 1024);
            List<StoreDefinition> stores = new ArrayList<StoreDefinition>();
            stores.add(storeDef);
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true,
                                                                                nodeId,
                                                                                TestUtils.createTempDir()
                                                                                         .getAbsolutePath(),
                                                                                cluster,
                                                                                stores,
                                                                                new Properties());
            config.setNioAdminConnectorSelectors(1);
            config.setNioConnectorSelectors(2);
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(socketStoreFactory, config);
            VoldemortService vsrv = vs.getService(ServiceType.STORAGE);
            StorageService ss = (StorageService) vsrv;
            voldemortServers.put(nodeId, vs);

            slopStorageEngines.put(nodeId, ss.getStoreRepository().getSlopStore());
            slopStores.put(nodeId, SerializingStore.wrap(ss.getStoreRepository().getSlopStore(),
                                                         new ByteArraySerializer(),
                                                         new SlopSerializer(),
                                                         new IdentitySerializer()));
            // wrap original store with force fail store
            Store<ByteArray, byte[], byte[]> store = ss.getStoreRepository()
                                                       .removeLocalStore(STORE_NAME);
            UnreachableStoreException exception = new UnreachableStoreException("Force failed");
            ForceFailStore<ByteArray, byte[], byte[]> forceFailStore = new ForceFailStore<ByteArray, byte[], byte[]>(store,
                                                                                                                     exception);
            forceFailStores.put(nodeId, forceFailStore);
            ss.getStoreRepository().addLocalStore(forceFailStore);
        }

        strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        // create client socket stores and slop stores
        SocketStoreClientFactoryForTest clientSocketStoreFactory = new SocketStoreClientFactoryForTest(STORE_NAME,
                                                                                                       SLOP_STORE_NAME);
        Serializer<ByteArray> slopKeySerializer = new ByteArraySerializer();
        Serializer<Slop> slopValueSerializer = new SlopSerializer();
        Map<Integer, Store<ByteArray, byte[], byte[]>> testStores = subStores;
        Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = new HashMap<Integer, Store<ByteArray, Slop, byte[]>>();
        for(Node node: cluster.getNodes()) {
            // test store
            SocketStore socketTestStore = clientSocketStoreFactory.getSocketTestStoreByNode(node);
            socketTestStores.put(node.getId(), socketTestStore);
            testStores.put(node.getId(), socketTestStore);

            // slop store
            SocketStore socketSlopStore = clientSocketStoreFactory.getSocketSlopStoreByNode(node);
            Store<ByteArray, Slop, byte[]> slopStore = SerializingStore.wrap(socketSlopStore,
                                                                             slopKeySerializer,
                                                                             slopValueSerializer,
                                                                             new IdentitySerializer());
            socketSlopStores.put(node.getId(), socketSlopStore);
            slopStores.put(node.getId(), slopStore);
        }

        // set failure detector
        if(failureDetector != null)
            failureDetector.destroy();
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(failureDetectorCls.getName());
        failureDetectorConfig.setThreshold(50);
        failureDetectorConfig.setCluster(cluster);
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));
        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);

        // make routedStore
        RoutedStoreFactory factory = new RoutedStoreFactory();
        routedStore = factory.create(cluster,
                                     storeDef,
                                     testStores,
                                     socketTestStores,
                                     slopStores,
                                     socketSlopStores,
                                     failureDetector,
                                     new RoutedStoreConfig().setTimeoutConfig(new TimeoutConfig(1500L,
                                                                                                false)));

        // generate the keys
        for(int i = 0; i < 5; i++) {
            Set<Integer> nodesCovered = Sets.newHashSet();
            while(nodesCovered.size() < NUM_NODES_TOTAL) {
                ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
                byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

                if(randomKey.length() > 0 && randomValue.length > 0) {
                    if(!keyList.contains(randomKey)) {
                        for(Node node: strategy.routeRequest(randomKey.get())) {
                            keysToNodes.put(randomKey, node.getId());
                            nodesCovered.add(node.getId());
                        }
                        logger.info("Inserting key [" + randomKey + "] to key list as id:"
                                    + keyList.size());
                        keyList.add(randomKey);
                        keyValues.put(randomKey, new ByteArray(randomValue));
                    }
                }
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
        for(VoldemortServer vs: voldemortServers.values()) {
            vs.stop();
        }
        routedStore.close();
        if(logger.isDebugEnabled()) {
            logger.debug("Test Ended: replication[" + REPLICATION_FACTOR + "], preferredW["
                         + P_WRITES + "], requiredW[" + R_WRITES + "]");
        }
    }

    @Test
    public void testHintedHandoff() throws Exception {
        Set<Integer> failedNodeSet = chooseFailedNodeSet(NUM_NODES_FAILED);
        Multimap<Integer, ByteArray> nodeToFailedKeysMap = doBatchPut(failedNodeSet);

        // wait for async operations
        // must be greater than socket timeout to ensure slop is registered
        logger.debug("Sleep for async operations to finish");
        Thread.sleep(Math.max(2000, SOCKET_TIMEOUT_MS * 2));

        Map<Integer, Map<ByteArray, byte[]>> nodeToSlopData = new HashMap<Integer, Map<ByteArray, byte[]>>();
        Set<ByteArray> slopKeys = makeSlopKeys(nodeToFailedKeysMap, Slop.Operation.PUT);
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> getAllResult = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: getAllResult.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                Integer nodeId = slop.getNodeId();
                // get data
                if(!nodeToSlopData.containsKey(nodeId)) {
                    nodeToSlopData.put(nodeId, new HashMap<ByteArray, byte[]>());
                }
                Map<ByteArray, byte[]> perNodeSlopMap = nodeToSlopData.get(nodeId);
                perNodeSlopMap.put(slop.getKey(), slop.getValue());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        int errorCount = 0;
        for(Map.Entry<Integer, ByteArray> failedKey: nodeToFailedKeysMap.entries()) {
            Integer nodeId = failedKey.getKey();
            ByteArray key = failedKey.getValue();
            byte[] expected = keyValues.get(key).get();

            Integer id = keyList.indexOf(key);

            // check if map exist
            Map<ByteArray, byte[]> perNodeSlopMap = nodeToSlopData.get(nodeId);
            if(perNodeSlopMap == null) {
                logger.error("Slop does not have key[" + key + "][id:" + id + "]");
                errorCount++;
                continue;
            }

            byte[] actual = perNodeSlopMap.get(key);

            if(actual == null) {
                logger.error("Slop does not have key[" + key + "][nodeId:" + nodeId + "]");
                errorCount++;
            } else if(ByteUtils.compare(actual, expected) != 0) {
                logger.error("Slop key[" + key + "][nodeId:" + nodeId
                             + "] does not have the correct value");
                errorCount++;
            } else {
                logger.debug("Slop has key[" + key + "][nodeId:" + nodeId + "] with value["
                             + actual + "]");
            }
        }
        assertTrue(errorCount + " Slop(s) incorrect. See log for more info", errorCount == 0);
    }

    private Set<ByteArray> makeSlopKeys(Multimap<Integer, ByteArray> failedKeys,
                                        Slop.Operation operation) {
        Set<ByteArray> slopKeys = Sets.newHashSet();

        for(Map.Entry<Integer, ByteArray> entry: failedKeys.entries()) {
            byte[] opCode = new byte[] { operation.getOpCode() };
            byte[] spacer = new byte[] { (byte) 0 };
            byte[] storeName = ByteUtils.getBytes(STORE_NAME, "UTF-8");
            byte[] nodeIdBytes = new byte[ByteUtils.SIZE_OF_INT];
            ByteUtils.writeInt(nodeIdBytes, entry.getKey(), 0);
            ByteArray slopKey = new ByteArray(ByteUtils.cat(opCode,
                                                            spacer,
                                                            storeName,
                                                            spacer,
                                                            nodeIdBytes,
                                                            spacer,
                                                            entry.getValue().get()));
            slopKeys.add(slopKey);
        }
        return slopKeys;
    }

    private static Set<Integer> chooseFailedNodeSet(int count) {
        // decide failed nodes
        Set<Integer> failedNodes = new HashSet<Integer>();
        Random rand = new Random();
        while(failedNodes.size() < count && failedNodes.size() <= NUM_NODES_TOTAL) {
            int n = rand.nextInt(NUM_NODES_TOTAL);
            failedNodes.add(n);
        }

        if(logger.isDebugEnabled()) {
            logger.debug("Failing requests to " + failedNodes);
        }
        return failedNodes;
    }

    private Multimap<Integer, ByteArray> doBatchPut(Set<Integer> failedNodeSet) {
        for(Integer nodeId: failedNodeSet) {
            forceFailStores.get(nodeId).setFail(true);
        }
        // put keys into the nodes
        Multimap<Integer, ByteArray> nodeToFailedKeys = ArrayListMultimap.create();
        for(ByteArray key: keysToNodes.keySet()) {
            List<Node> nodes = strategy.routeRequest(key.get());
            List<Node> failedNodes = new ArrayList<Node>();
            for(Node node: nodes) {
                if(failedNodeSet != null && failedNodeSet.contains(node.getId())) {
                    failedNodes.add(node);
                }
            }
            // determine if write will succeed (if numGoodNodes > required)
            if((nodes.size() - failedNodes.size()) >= R_WRITES) {
                for(Node node: failedNodes) {
                    nodeToFailedKeys.put(node.getId(), key);
                    logger.debug("[key:" + key + "] to [nodeId:" + node.getId() + "] should fail");
                }
            } else {
                logger.debug("[key:" + key + "] should fail overall due to insufficient nodes");
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                if(logger.isTraceEnabled())
                    logger.trace("PUT key [" + key + "] to store");
                routedStore.put(key, versioned, null);
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }
        return nodeToFailedKeys;
    }
}

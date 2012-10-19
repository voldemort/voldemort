package voldemort.store.routed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.cluster.failuredetector.MutableStoreVerifier;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.ForceFailStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class HintedHandoffTest {

    private final static Logger logger = Logger.getLogger(HintedHandoffTest.class);

    private final static String STORE_NAME = "test";
    private final static String SLOP_STORE_NAME = "slop";

    private final static int NUM_THREADS = 9;
    private final static int NUM_NODES_TOTAL = 9;
    private final static int NUM_NODES_FAILED = 4;

    private final static int REPLICATION_FACTOR = 3;
    private final static int P_READS = 1;
    private final static int R_READS = 1;
    private final static int P_WRITES = 2;
    private final static int R_WRITES = 1;

    private final static int KEY_LENGTH = 32;
    private final static int VALUE_LENGTH = 32;

    private final Class<? extends FailureDetector> failureDetectorCls = BannagePeriodFailureDetector.class;
    private final HintedHandoffStrategyType hintRoutingStrategy;

    private final Map<Integer, Store<ByteArray, byte[], byte[]>> subStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[], byte[]>>();
    private final Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores = new ConcurrentHashMap<Integer, Store<ByteArray, Slop, byte[]>>();
    private final List<StreamingSlopPusherJob> slopPusherJobs = Lists.newLinkedList();
    private final Multimap<ByteArray, Integer> keysToNodes = HashMultimap.create();
    private final Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();

    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private RoutedStoreFactory routedStoreFactory;
    private RoutingStrategy strategy;
    private RoutedStore store;

    public HintedHandoffTest(HintedHandoffStrategyType hintRoutingStrategy) {
        this.hintRoutingStrategy = hintRoutingStrategy;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { /*
                                               * {HintedHandoffStrategyType.
                                               * CONSISTENT_STRATEGY },
                                               */
        { HintedHandoffStrategyType.ANY_STRATEGY },
                { HintedHandoffStrategyType.PROXIMITY_STRATEGY } });
    }

    private StoreDefinition getStoreDef(String storeName,
                                        int replicationFactor,
                                        int preads,
                                        int rreads,
                                        int pwrites,
                                        int rwrites,
                                        String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .setHintedHandoffStrategy(hintRoutingStrategy)
                                           .build();
    }

    @Before
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        storeDef = getStoreDef(STORE_NAME,
                               REPLICATION_FACTOR,
                               P_READS,
                               R_READS,
                               P_WRITES,
                               R_WRITES,
                               RoutingStrategyType.CONSISTENT_STRATEGY);
        for(Node node: cluster.getNodes()) {
            VoldemortException e = new UnreachableStoreException("Node down");

            InMemoryStorageEngine<ByteArray, byte[], byte[]> storageEngine = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(STORE_NAME);
            LoggingStore<ByteArray, byte[], byte[]> loggingStore = new LoggingStore<ByteArray, byte[], byte[]>(storageEngine);
            subStores.put(node.getId(), new ForceFailStore<ByteArray, byte[], byte[]>(loggingStore,
                                                                                      e));
        }

        setFailureDetector(subStores);

        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(true,
                                                    routedStoreThreadPool,
                                                    new TimeoutConfig(1500L, false));
        strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        Map<Integer, NonblockingStore> nonblockingSlopStores = Maps.newHashMap();
        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = new StoreRepository();
            storeRepo.addLocalStore(subStores.get(nodeId));

            for(int i = 0; i < NUM_NODES_TOTAL; i++)
                storeRepo.addNodeStore(i, subStores.get(i));

            SlopStorageEngine slopStorageEngine = new SlopStorageEngine(new InMemoryStorageEngine<ByteArray, byte[], byte[]>(SLOP_STORE_NAME),
                                                                        cluster);
            StorageEngine<ByteArray, Slop, byte[]> storageEngine = slopStorageEngine.asSlopStore();
            storeRepo.setSlopStore(slopStorageEngine);
            nonblockingSlopStores.put(nodeId,
                                      routedStoreFactory.toNonblockingStore(slopStorageEngine));
            slopStores.put(nodeId, storageEngine);

            MetadataStore metadataStore = ServerTestUtils.createMetadataStore(cluster,
                                                                              Lists.newArrayList(storeDef));
            StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(storeRepo,
                                                                       metadataStore,
                                                                       failureDetector,
                                                                       ServerTestUtils.createServerConfigWithDefs(false,
                                                                                                                  nodeId,
                                                                                                                  TestUtils.createTempDir()
                                                                                                                           .getAbsolutePath(),
                                                                                                                  cluster,
                                                                                                                  Lists.newArrayList(storeDef),
                                                                                                                  new Properties()),
                                                                       new ScanPermitWrapper(1));
            slopPusherJobs.add(pusher);
        }

        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();
        for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: subStores.entrySet())
            nonblockingStores.put(entry.getKey(),
                                  routedStoreFactory.toNonblockingStore(entry.getValue()));

        store = routedStoreFactory.create(cluster,
                                          storeDef,
                                          subStores,
                                          nonblockingStores,
                                          slopStores,
                                          nonblockingSlopStores,
                                          false,
                                          Zone.DEFAULT_ZONE_ID,
                                          failureDetector);

        generateData();
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        if(routedStoreThreadPool != null)
            routedStoreThreadPool.shutdown();
    }

    @Test
    public void testHintedHandoff() throws Exception {
        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = populateStore(failedNodes);
        Thread.sleep(5000);

        Map<ByteArray, byte[]> dataInSlops = Maps.newHashMap();
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.PUT);
        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                dataInSlops.put(slop.getKey(), slop.getValue());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries()) {
            byte[] expected = keyValues.get(failedKey.getValue()).get();
            byte[] actual = dataInSlops.get(failedKey.getValue());

            assertNotNull("data should be stored in the slop for key = " + failedKey.getValue(),
                          actual);
            assertEquals("correct should be stored in slop", 0, ByteUtils.compare(actual, expected));
        }

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

    @Test
    @Ignore
    public void testSlopPushers() throws Exception {
        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = populateStore(failedNodes);
        Thread.sleep(5000);
        ExecutorService executor = Executors.newFixedThreadPool(slopPusherJobs.size());
        final CountDownLatch latch = new CountDownLatch(slopPusherJobs.size());
        for(final StreamingSlopPusherJob job: slopPusherJobs) {
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Started slop pusher job " + job);
                        job.run();
                        if(logger.isTraceEnabled())
                            logger.trace("Finished slop pusher job " + job);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        Thread.sleep(5000);
        for(Map.Entry<Integer, ByteArray> entry: failedKeys.entries()) {
            List<Versioned<byte[]>> values = store.get(entry.getValue(), null);

            assertTrue("slop entry should be pushed for " + entry.getValue() + ", preflist "
                       + keysToNodes.get(entry.getValue()), values.size() > 0);
            assertEquals("slop entry should be correct for " + entry.getValue(),
                         keyValues.get(entry.getValue()),
                         new ByteArray(values.get(0).getValue()));
        }
    }

    @Test
    @Ignore
    public void testDeleteHandoff() throws Exception {
        populateStore(Sets.<Integer> newHashSet());

        Map<ByteArray, Version> versions = Maps.newHashMap();
        for(ByteArray key: keyValues.keySet())
            versions.put(key, store.get(key, null).get(0).getVersion());

        Set<Integer> failedNodes = getFailedNodes();
        Multimap<Integer, ByteArray> failedKeys = ArrayListMultimap.create();

        for(ByteArray key: keysToNodes.keySet()) {
            List<Node> nodes = strategy.routeRequest(key.get());
            for(Node node: nodes) {
                if(failedNodes.contains(node.getId())) {
                    failedKeys.put(node.getId(), key);
                    break;
                }
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries()) {
            try {
                store.delete(failedKey.getValue(), versions.get(failedKey.getValue()));
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }

        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.DELETE);
        Set<ByteArray> keysInSlops = Sets.newHashSet();

        Thread.sleep(5000);

        for(Store<ByteArray, Slop, byte[]> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys, null);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                keysInSlops.add(slop.getKey());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(Map.Entry<Integer, ByteArray> failedKey: failedKeys.entries())
            assertTrue("delete operation for " + failedKey.getValue() + " should be handed off",
                       keysInSlops.contains(failedKey.getValue()));

    }

    private Set<Integer> getFailedNodes() {
        Set<Integer> failedNodes = new CopyOnWriteArraySet<Integer>();
        Random rand = new Random();
        for(int i = 0; i < NUM_NODES_FAILED; i++) {
            int n = rand.nextInt(NUM_NODES_TOTAL);
            failedNodes.add(n);
        }

        for(int node: failedNodes)
            getForceFailStore(node).setFail(true);

        if(logger.isTraceEnabled())
            logger.trace("Failing requests to " + failedNodes);

        return failedNodes;
    }

    private Multimap<Integer, ByteArray> populateStore(Set<Integer> failedNodes) {
        Multimap<Integer, ByteArray> failedKeys = ArrayListMultimap.create();
        for(ByteArray key: keysToNodes.keySet()) {
            List<Node> nodes = strategy.routeRequest(key.get());
            for(Node node: nodes) {
                if(failedNodes.contains(node.getId())) {
                    failedKeys.put(node.getId(), key);
                    break;
                }
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                store.put(key, versioned, null);
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }
        return failedKeys;
    }

    private void generateData() {
        for(int i = 0; i < 2; i++) {
            Set<Integer> nodesCovered = Sets.newHashSet();
            while(nodesCovered.size() < NUM_NODES_TOTAL) {
                ByteArray randomKey = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
                byte[] randomValue = TestUtils.randomBytes(VALUE_LENGTH);

                if(randomKey.length() > 0 && randomValue.length > 0) {
                    for(Node node: strategy.routeRequest(randomKey.get())) {
                        keysToNodes.put(randomKey, node.getId());
                        nodesCovered.add(node.getId());
                    }

                    keyValues.put(randomKey, new ByteArray(randomValue));
                }
            }
        }
    }

    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[], byte[]>> subStores)
            throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(failureDetectorCls.getName());
        failureDetectorConfig.setBannagePeriod(500);
        failureDetectorConfig.setCluster(cluster);
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    public ForceFailStore<ByteArray, byte[], byte[]> getForceFailStore(int nodeId) {
        return (ForceFailStore<ByteArray, byte[], byte[]>) subStores.get(nodeId);
    }
}

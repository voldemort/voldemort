package voldemort.store.routed;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import voldemort.MutableStoreVerifier;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.scheduler.SlopPusherJob;
import voldemort.store.ForceFailStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.logging.LoggingStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.slop.HintedHandoffStrategyType;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static voldemort.VoldemortTestConstants.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HintedHandoffTest {

    private final static Logger logger = Logger.getLogger(HintedHandoffTest.class);

    private final static String STORE_NAME = "test";
    private final static String SLOP_STORE_NAME = "slop";

    private final static int NUM_THREADS = 4;
    private final static int NUM_NODES_TOTAL = 9;
    private final static int NUM_NODES_FAILED = 4;

    private final static int REPLICATION_FACTOR = 4;
    private final static int P_READS = 1;
    private final static int R_READS = 1;
    private final static int P_WRITES = 2;
    private final static int R_WRITES = 1;

    private final static int KEY_LENGTH = 64;
    private final static int VALUE_LENGTH = 1024;

    private final Class<? extends FailureDetector> failureDetectorCls = BannagePeriodFailureDetector.class;
    private final String hintRoutingStrategy;

    private final Map<Integer, Store<ByteArray, byte[]>> subStores = new ConcurrentHashMap<Integer, Store<ByteArray, byte[]>>();
    private final Map<Integer, Store<ByteArray, Slop>> slopStores = new ConcurrentHashMap<Integer, Store<ByteArray, Slop>>();
    private final List<SlopPusherJob> slopPusherJobs = Lists.newLinkedList();
    private final Multimap<ByteArray, Integer> keysToNodes = HashMultimap.create();
    private final Map<ByteArray, ByteArray> keyValues = Maps.newHashMap();

    private Cluster cluster;
    private FailureDetector failureDetector;
    private StoreDefinition storeDef;
    private ExecutorService routedStoreThreadPool;
    private RoutedStoreFactory routedStoreFactory;
    private RoutingStrategy strategy;
    private RoutedStore store;


    public HintedHandoffTest(String hintRoutingStrategy) {
        this.hintRoutingStrategy = hintRoutingStrategy;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][]{
                       {HintedHandoffStrategyType.CONSISTENT_STRATEGY},
                       {HintedHandoffStrategyType.TO_ALL_STRATEGY}
        });
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
                       .setEnableHintedHandoff(true)
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
        Random rand = new Random();
        for(Node node : cluster.getNodes()) {
            VoldemortException e;
            if(rand.nextBoolean())
                e = new VoldemortException("Operation failed");
            else
                e = new UnreachableStoreException("Node down");

            InMemoryStorageEngine<ByteArray, byte[]> storageEngine = new InMemoryStorageEngine<ByteArray, byte[]>(STORE_NAME);
            LoggingStore<ByteArray, byte[]> loggingStore = new LoggingStore<ByteArray, byte[]>(storageEngine);
            subStores.put(node.getId(), new ForceFailStore<ByteArray, byte[]>(loggingStore, e, node.getId()));
        }

        setFailureDetector(subStores);

        for(Node node : cluster.getNodes()) {
            int nodeId = node.getId();
            StoreRepository storeRepo = new StoreRepository();
            storeRepo.addLocalStore(subStores.get(nodeId));

            for(int i = 0; i < NUM_NODES_TOTAL; i++)
                storeRepo.addNodeStore(i, subStores.get(i));

            StorageEngine<ByteArray, Slop> storageEngine = new InMemoryStorageEngine<ByteArray, Slop>(SLOP_STORE_NAME);
            storeRepo.setSlopStore(storageEngine);
            slopStores.put(nodeId, storageEngine);

            SlopPusherJob pusher = new SlopPusherJob(storeRepo);
            slopPusherJobs.add(pusher);
        }

        routedStoreThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
        routedStoreFactory = new RoutedStoreFactory(true,
                                                    routedStoreThreadPool,
                                                    1000L);
        strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);
        Map<Integer, NonblockingStore> nonblockingStores = Maps.newHashMap();

        for(Map.Entry<Integer, Store<ByteArray, byte[]>> entry: subStores.entrySet())
            nonblockingStores.put(entry.getKey(), routedStoreFactory.toNonblockingStore(entry.getValue()));

        store = routedStoreFactory.create(cluster,
                                          storeDef,
                                          subStores,
                                          nonblockingStores,
                                          slopStores,
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
        Set<ByteArray> failedKeys = populateStore(failedNodes);

        Map<ByteArray, byte[]> dataInSlops = Maps.newHashMap();
        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.PUT);

        for (Store<ByteArray, Slop> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                dataInSlops.put(slop.getKey(), slop.getValue());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(ByteArray failedKey: failedKeys) {
            byte[] expected = keyValues.get(failedKey).get();
            byte[] actual = dataInSlops.get(failedKey);

            assertNotNull("data should be stored in the slop", actual);
            assertEquals("correct should be stored in slop",
                         0,
                         ByteUtils.compare(actual, expected));
        }
            
    }

    private Set<ByteArray> makeSlopKeys(Set<ByteArray> failedKeys, Slop.Operation operation) {
        Set<ByteArray> slopKeys = Sets.newHashSet();

        for(ByteArray key: failedKeys) {
            byte[] opCode = new byte[] { operation.getOpCode() };
            byte[] spacer = new byte[] { (byte) 0 };
            byte[] storeName = ByteUtils.getBytes(STORE_NAME, "UTF-8");
            ByteArray slopKey = new ByteArray(ByteUtils.cat(opCode, spacer, storeName, spacer, key.get()));
            slopKeys.add(slopKey);
        }
        return slopKeys;
    }

    @Test
    @Ignore
    public void testSlopPushers() throws Exception {
        Set<Integer> failedNodes = getFailedNodes();
        Set<ByteArray> failedKeys = populateStore(failedNodes);
        reviveNodes(failedNodes);

        for(int i = 0; i < 5; i++) {
            for(SlopPusherJob job: slopPusherJobs) {
                if (logger.isTraceEnabled())
                    logger.trace("Started slop pusher job " + job);

                job.run();

                if (logger.isTraceEnabled())
                    logger.trace("Finished slop pusher job " + job);
            }
        }

        for(ByteArray key: failedKeys) {
            List<Versioned<byte[]>> values = store.get(key);

            assertTrue("slop entry should be pushed for " +
                       key + ", preflist " + keysToNodes.get(key),
                       values.size() > 0);
            assertEquals("slop entry should be correct for " + key,
                         keyValues.get(key),
                         new ByteArray(values.get(0).getValue()));
        }
    }

    private void reviveNodes(Set<Integer> failedNodes) {
        for(int node: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(node);
            forceFailStore.setFail(false);

            if(logger.isTraceEnabled())
                logger.trace("Stopped failing requests to " + node);
        }

        while(!failedNodes.isEmpty()) {
            for(int node: failedNodes)
                if(failureDetector.isAvailable(cluster.getNodeById(node)))
                    failedNodes.remove(node);
        }
    }

    @Test
    public void testDeleteHandoff() throws Exception {
        populateStore(Sets.<Integer>newHashSet());

        Map<ByteArray, Version> versions = Maps.newHashMap();
        for(ByteArray key: keyValues.keySet())
            versions.put(key, store.get(key).get(0).getVersion());

        Set<Integer> failedNodes = getFailedNodes();
        Set<ByteArray> failedKeys = Sets.newHashSet();

        for(ByteArray key: keysToNodes.keySet()) {
            Iterable<Integer> nodes = keysToNodes.get(key);

            for(int i=0; i < REPLICATION_FACTOR; i++) {
                int node = Iterables.get(nodes, i);
                if(failedNodes.contains(node)) {
                    failedKeys.add(key);
                    break;
                }
            }
        }

        for(ByteArray failedKey: failedKeys) {
            try {
                store.delete(failedKey, versions.get(failedKey));
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }

        Set<ByteArray> slopKeys = makeSlopKeys(failedKeys, Slop.Operation.DELETE);
        Set<ByteArray> keysInSlops = Sets.newHashSet();

        for (Store<ByteArray, Slop> slopStore: slopStores.values()) {
            Map<ByteArray, List<Versioned<Slop>>> res = slopStore.getAll(slopKeys);
            for(Map.Entry<ByteArray, List<Versioned<Slop>>> entry: res.entrySet()) {
                Slop slop = entry.getValue().get(0).getValue();
                keysInSlops.add(slop.getKey());

                if(logger.isTraceEnabled())
                    logger.trace(slop);
            }
        }

        for(ByteArray failedKey: failedKeys)
            assertTrue("delete operation for " + failedKey + " should be handed off",
                       keysInSlops.contains(failedKey));

    }

    private Set<Integer> getFailedNodes() {
        Set<Integer> failedNodes = new CopyOnWriteArraySet<Integer>();
        Random rand = new Random();
        int offset = rand.nextInt(NUM_NODES_TOTAL);

        for(int i=0; i < NUM_NODES_FAILED; i++)
            failedNodes.add((offset + i) % NUM_NODES_TOTAL);

        for(int node: failedNodes) {
            ForceFailStore forceFailStore = getForceFailStore(node);
            forceFailStore.setFail(true);

            if(logger.isTraceEnabled())
                logger.trace("Started failing requests to " + node);
        }
        return failedNodes;
    }

    private Set<ByteArray> populateStore(Set<Integer> failedNodes) {
        Set<ByteArray> failedKeys = Sets.newHashSet();
        for(ByteArray key: keysToNodes.keySet()) {
            Iterable<Integer> nodes = keysToNodes.get(key);

            for(int i=0; i < REPLICATION_FACTOR; i++) {
                int node = Iterables.get(nodes, i);
                if(failedNodes.contains(node)) {
                    failedKeys.add(key);
                    break;
                }
            }

            try {
                Versioned<byte[]> versioned = new Versioned<byte[]>(keyValues.get(key).get());
                store.put(key, versioned);
            } catch(Exception e) {
                if(logger.isTraceEnabled())
                    logger.trace(e, e);
            }
        }
        return failedKeys;
    }

    private void generateData() {
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

    private void setFailureDetector(Map<Integer, Store<ByteArray, byte[]>> subStores)
                   throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig();
        failureDetectorConfig.setImplementationClassName(failureDetectorCls.getName());
        failureDetectorConfig.setBannagePeriod(30);
        failureDetectorConfig.setNodes(cluster.getNodes());
        failureDetectorConfig.setStoreVerifier(MutableStoreVerifier.create(subStores));

        failureDetector = FailureDetectorUtils.create(failureDetectorConfig, false);
    }

    public ForceFailStore<ByteArray, byte[]> getForceFailStore(int nodeId) {
        return (ForceFailStore<ByteArray, byte[]>) subStores.get(nodeId);
    }
}

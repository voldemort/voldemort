package voldemort.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.cleanupCluster;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.AssertionFailedError;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.client.rebalance.RebalanceController;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStore;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * @author afeinberg
 */
public class Ec2RebalancingTest {

    private static Ec2RebalancingTestConfig ec2RebalancingTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;

    private static final Logger logger = Logger.getLogger(Ec2RebalancingTest.class);

    private Cluster originalCluster;

    private Map<String, String> testEntries;
    private Map<String, Integer> nodeIds;
    private int[][] partitionMap;
    private boolean spareNode;

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2RebalancingTestConfig = new Ec2RebalancingTestConfig();
        hostNamePairs = createInstances(ec2RebalancingTestConfig);
        hostNames = toHostNames(hostNamePairs);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(hostNames != null)
            destroyInstances(hostNames, ec2RebalancingTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        Thread.sleep(5000);
        logger.info("Before()");
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        spareNode = ec2RebalancingTestConfig.addNodes == 0;
        partitionMap = getPartitionMap(clusterSize,
                                       ec2RebalancingTestConfig.partitionsPerNode,
                                       spareNode);

        if(logger.isInfoEnabled())
            logPartitionMap(partitionMap, "Original");

        originalCluster = ServerTestUtils.getLocalCluster(clusterSize,
                                                          getPorts(clusterSize),
                                                          partitionMap);
        nodeIds = generateClusterDescriptor(hostNamePairs,
                                            originalCluster,
                                            ec2RebalancingTestConfig);

        deploy(hostNames, ec2RebalancingTestConfig);
        startClusterAsync(hostNames, ec2RebalancingTestConfig, nodeIds);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to let the Voldemort cluster start");

        Thread.sleep(15000);

        testEntries = ServerTestUtils.createRandomKeyValueString(ec2RebalancingTestConfig.numKeys);
        originalCluster = updateCluster(originalCluster, nodeIds);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2RebalancingTestConfig);
        cleanupCluster(hostNames, ec2RebalancingTestConfig);
    }

    @Test
    public void testGracefulRecovery() throws Exception {
        assert (ec2RebalancingTestConfig.getInstanceCount() == 2);
        final Cluster targetCluster = expandCluster(0,
                                                    ServerTestUtils.getLocalCluster(2,
                                                                                    getPorts(2),
                                                                                    new int[][]{
                                                                                                   {},
                                                                                                   {0, 1, 2, 3}}));
        final SocketStoreClientFactory factory = getStoreClientFactory();
        final StoreClient<String,String> storeClient = getStoreClient(factory);
        final CountDownLatch nodeKilled = new CountDownLatch(1);
        final CountDownLatch rebalanceStarted = new CountDownLatch(1);
        final CountDownLatch testsDone = new CountDownLatch(1);

        final AtomicBoolean restartedRebalancing = new AtomicBoolean(false);
        final AtomicBoolean entriesCorrect = new AtomicBoolean(false);

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            populateData(originalCluster, Arrays.asList(0));

            if (logger.isInfoEnabled())
                logger.info("Populated data, start the test.");
            
            executorService.submit(new Runnable() {
                // Initiate rebalancing
                public void run() {
                    AdminClient adminClient = new AdminClient(getBootstrapUrl(originalCluster, 0), new AdminClientConfig());
                    RebalancePartitionsInfo rebalancePartitionsInfo = new RebalancePartitionsInfo(1, 0,
                                                                                                  Arrays.asList(0, 1, 2, 3),
                                                                                                  Arrays.asList(ec2RebalancingTestConfig.testStoreName),
                                                                                                  false,
                                                                                                  0
                                                                                                  );
                    int reqId = adminClient.rebalanceNode(rebalancePartitionsInfo);
                    if (logger.isInfoEnabled())
                        logger.info("Starting rebalancing: async request id = " + reqId);

                    rebalanceStarted.countDown(); // signal that we started rebalancing
                }
            });

            executorService.submit(new Runnable() {
                // Stop a node, bring it back up
                public void run() {
                    try {
                        String hostname = originalCluster.getNodeById(1).getHost();
                        if (logger.isInfoEnabled())
                            logger.info("Waiting to get rebalancing into a half-way state");

                        rebalanceStarted.await();
                        Thread.sleep(25);

                        stopClusterNode(hostname,ec2RebalancingTestConfig);
                        
                        nodeKilled.countDown();  // signal that we shot the node
                        if (logger.isInfoEnabled())
                            logger.info("Killed node 1");

                        Thread.sleep(250);

                        startClusterNode(hostname, ec2RebalancingTestConfig, 1);
                        if (logger.isInfoEnabled())
                            logger.info("Brought node 1 back up");
                    } catch (InterruptedException ie) {
                        logger.error(ie);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        throw new IllegalStateException("unable to stop/start the host!", e);
                    }
                }
            });

            executorService.submit(new Runnable() {
                // After a node is brought down, verify that it finishes rebalancing
                public void run() {
                    try {
                        try {
                            if (logger.isInfoEnabled())
                                logger.info("Waiting for the node to be brought down");

                            nodeKilled.await();
                            if (logger.isInfoEnabled())
                                logger.info("Waiting for five seconds for rebalancing to retry");

                            Thread.sleep(10000);

                            AdminClient adminClient = new AdminClient(getBootstrapUrl(originalCluster, 0),
                                                                      new AdminClientConfig());
                            Versioned<MetadataStore.VoldemortState> serverState = adminClient.getRemoteServerState(1);

                            long start = System.currentTimeMillis();
                            long delay = 5000;
                            long delayMax = 1000 * 30; 
                            long timeout = 5 * 1000 * 60;

                            while (System.currentTimeMillis() < start + timeout &&
                                   serverState.getValue() != MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER) {
                                Thread.sleep(delay);
                                if (delay < delayMax)
                                    delay *= 2;
                                serverState = adminClient.getRemoteServerState(1);
                                if (logger.isInfoEnabled())
                                    logger.info("Server state: " + serverState.getValue());
                            }

                            if (serverState.getValue() == MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER) {

                                restartedRebalancing.set(true);
                                if (logger.isInfoEnabled())
                                    logger.info("Recovered node began rebalancing, continuing with the test.");

                                Thread.sleep(60 * 1000);
                                try {
                                    for(int nodeId: Arrays.asList(0,1)) {
                                        List<Integer> availablePartitions = targetCluster.getNodeById(nodeId).getPartitionIds();
                                        List<Integer> unavailablePartitions = getUnavailablePartitions(targetCluster,
                                                                                                       availablePartitions);

                                        checkGetEntries(originalCluster.getNodeById(nodeId),
                                                        targetCluster,
                                                        unavailablePartitions,
                                                        availablePartitions);
                                    }
                                    entriesCorrect.set(true);
                                } catch (AssertionFailedError afe) {
                                    logger.error(afe);
                                }
                                
                            }
                        } catch (InterruptedException ie) {
                            logger.error(ie);
                            Thread.currentThread().interrupt();
                        }
                    } finally {
                        factory.close();
                        testsDone.countDown();
                    }
                }
            });

            testsDone.await();

            assertTrue("node restarted rebalancing after recovering", restartedRebalancing.get());
            assertTrue("entries got set correctly", entriesCorrect.get());

            executorService.awaitTermination(15 * 60, TimeUnit.SECONDS);
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    @Test
    public void testSingleRebalancing() throws Exception {
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        int[][] targetLayout;

        if(spareNode)
            targetLayout = splitLastPartition(partitionMap,
                                              partitionMap[clusterSize - 2].length - 2);
        else
            targetLayout = insertNode(partitionMap, partitionMap[clusterSize - 1].length - 2);

        if(logger.isInfoEnabled())
            logPartitionMap(targetLayout, "Target");

        Cluster targetCluster = ServerTestUtils.getLocalCluster(targetLayout.length,
                                                                getPorts(targetLayout.length),
                                                                targetLayout);

        List<Integer> originalNodes = new ArrayList<Integer>();
        for(Node node: originalCluster.getNodes()) {
            if(node.getId() == (clusterSize - 1) && spareNode)
                break;
            originalNodes.add(node.getId());
        }

        targetCluster = expandCluster(targetCluster.getNumberOfNodes() - clusterSize,
                                      targetCluster);

        // Start common code
        RebalanceController RebalanceController = new RebalanceController(getBootstrapUrl(originalCluster,
                                                                                          0),
                                                                          new RebalanceClientConfig());
        try {
            populateData(originalCluster, originalNodes);
            rebalanceAndCheck(originalCluster, targetCluster, RebalanceController, originalNodes);
            // end common code
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    @Test
    public void testProxyGetDuringRebalancing() throws Exception {
        assert (ec2RebalancingTestConfig.getInstanceCount() == 2);
        final Cluster targetCluster = expandCluster(0,
                                                    ServerTestUtils.getLocalCluster(2,
                                                                                    getPorts(2),
                                                                                    new int[][] {
                                                                                            {},
                                                                                            {
                                                                                                    0,
                                                                                                    1,
                                                                                                    2,
                                                                                                    3 } }));
        try {
            final List<Integer> serverList = Arrays.asList(0, 1);
            ExecutorService executors = Executors.newFixedThreadPool(2);
            final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            // populate data now.
            populateData(originalCluster, Arrays.asList(0));

            final SocketStoreClientFactory factory = getStoreClientFactory();
            final StoreClient<String, String> storeClient = getStoreClient(factory);

            final boolean[] masterNodeResponded = { false, false };

            // start get operation.
            executors.execute(new Runnable() {

                public void run() {
                    try {
                        List<String> keys = new ArrayList<String>(testEntries.keySet());

                        boolean caughtException=false;
                        int nRequests = 0;
                        while(!rebalancingToken.get()) {
                            // should always able to get values.
                            int index = (int) (Math.random() * keys.size());

                            // should get a valid value
                            try {
                                nRequests++;
                                Versioned<String> value = storeClient.get(keys.get(index));
                                assertNotSame("StoreClient get() should not return null.", null, value);
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             value);
                                int masterNode = storeClient.getResponsibleNodes(keys.get(index))
                                               .get(0)
                                               .getId();
                                masterNodeResponded[masterNode] = true;

                            } catch (InsufficientOperationalNodesException ison) {
                                if (!caughtException) {
                                    ison.printStackTrace();
                                    exceptions.add(ison);
                                    caughtException = true;
                                }
                            } catch(Exception e) {
                                e.printStackTrace();
                                exceptions.add(e);
                            }
                        }

                    } catch(Exception e) {
                        exceptions.add(e);
                    } finally {
                        factory.close();
                    }
                }

            });

            executors.execute(new Runnable() {

                public void run() {
                    try {

                        Thread.sleep(100);

                        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(originalCluster,
                                                                                                      0),
                                                                                      new RebalanceClientConfig());
                        rebalanceAndCheck(originalCluster,
                                          targetCluster,
                                          rebalanceClient,
                                          Arrays.asList(1));

                        // sleep for 1 mins before stopping servers
                        Thread.sleep(60 * 1000);

                        rebalancingToken.set(true);

                    } catch(Exception e) {
                        exceptions.add(e);
                    } finally {
                        // stop servers
                    }
                }
            });

            executors.shutdown();
            executors.awaitTermination(300, TimeUnit.SECONDS);

            assertEquals("Client should see values returned master at both (0,1):("
                         + masterNodeResponded[0] + "," + masterNodeResponded[1] + ")",
                         true,
                         masterNodeResponded[0] && masterNodeResponded[1]);

            // check No Exception
            if(exceptions.size() > 0) {
                for(Exception e: exceptions) {
                    e.printStackTrace();
                }
                fail("Should not see any exceptions !!");
            }
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    private StoreClient<String, String> getStoreClient(SocketStoreClientFactory factory) {
        return new DefaultStoreClient<String, String>(ec2RebalancingTestConfig.testStoreName,
                                                      null,
                                                      factory,
                                                      3);
    }

    private SocketStoreClientFactory getStoreClientFactory() {
        return new SocketStoreClientFactory(new ClientConfig()
                       .setSocketTimeout(60, TimeUnit.SECONDS)
                       .setBootstrapUrls(getBootstrapUrl(originalCluster,
                                                         0)));
    }

    private Cluster updateCluster(Cluster templateCluster, Map<String, Integer> nodeIds) {
        List<Node> nodes = new ArrayList<Node>();
        for(Map.Entry<String, Integer> entry: nodeIds.entrySet()) {
            String hostName = entry.getKey();
            int nodeId = entry.getValue();
            Node templateNode = templateCluster.getNodeById(nodeId);
            Node node = new Node(nodeId,
                                 hostName,
                                 templateNode.getHttpPort(),
                                 templateNode.getSocketPort(),
                                 templateNode.getAdminPort(),
                                 templateNode.getPartitionIds());
            nodes.add(node);
        }
        return new Cluster(templateCluster.getName(), nodes);
    }

    private Cluster expandCluster(int newNodes, Cluster newCluster) throws Exception {
        if(newNodes > 0) {
            List<HostNamePair> newInstances = createInstances(newNodes, ec2RebalancingTestConfig);
            List<String> newHostnames = toHostNames(newInstances);

            if(logger.isInfoEnabled())
                logger.info("Sleeping for 30 seconds to let new instances startup");

            Thread.sleep(30000);

            hostNamePairs.addAll(newInstances);
            hostNames = toHostNames(hostNamePairs);

            nodeIds = generateClusterDescriptor(hostNamePairs, newCluster, ec2RebalancingTestConfig);

            deploy(newHostnames, ec2RebalancingTestConfig);
            startClusterAsync(newHostnames, ec2RebalancingTestConfig, nodeIds);

            if(logger.isInfoEnabled()) {
                logger.info("Expanded the cluster. New layout: " + nodeIds);
                logger.info("Sleeping for 10 seconds to let voldemort start");
            }

            Thread.sleep(10000);
        }

        return updateCluster(newCluster, nodeIds);
    }

    private int[][] getPartitionMap(int nodes, int perNode, boolean spareNode) {
        int limit = spareNode ? nodes - 1 : nodes;
        int[][] partitionMap = new int[nodes][];
        int i, k;

        for(i = 0, k = 0; i < limit; i++) {
            partitionMap[i] = new int[perNode];
            for(int j = 0; j < perNode; j++)
                partitionMap[i][j] = k++;
        }

        if(spareNode)
            partitionMap[limit] = new int[] {};

        return partitionMap;
    }

    private static int[][] insertNode(int[][] template, int pivot) {
        int templateLength = template.length;
        int vectorTailLength = template[templateLength - 1].length - pivot;

        int[][] layout = new int[templateLength + 1][];
        layout[templateLength - 1] = new int[pivot];
        layout[templateLength] = new int[vectorTailLength];

        System.arraycopy(template, 0, layout, 0, templateLength - 1);
        System.arraycopy(template[templateLength - 1], 0, layout[templateLength - 1], 0, pivot);
        System.arraycopy(template[templateLength - 1],
                         pivot,
                         layout[templateLength],
                         0,
                         vectorTailLength);

        return layout;
    }

    private static int[][] splitLastPartition(int[][] template, int pivot) {
        int templateLength = template.length;
        int vectorTailLength = template[templateLength - 2].length - pivot;

        int[][] layout = new int[templateLength][];
        layout[templateLength - 2] = new int[pivot];
        layout[templateLength - 1] = new int[vectorTailLength];

        System.arraycopy(template, 0, layout, 0, templateLength - 2);
        System.arraycopy(template[templateLength - 2], 0, layout[templateLength - 2], 0, pivot);
        System.arraycopy(template[templateLength - 2],
                         pivot,
                         layout[templateLength - 1],
                         0,
                         vectorTailLength);

        return layout;
    }

    private static int[] getPorts(int count) {
        int[] ports = new int[count * 3];
        for(int i = 0; i < count; i++) {
            ports[3 * i] = 6665;
            ports[3 * i + 1] = 6666;
            ports[3 * i + 2] = 6667;
        }

        return ports;
    }

    private static void logPartitionMap(int[][] map, String name) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(name);
        stringBuilder.append(" partition layout: \n");
        stringBuilder.append("------------------------\n");
        for(int i = 0; i < map.length; i++) {
            stringBuilder.append("node " + i + ": ");
            for(int j = 0; j < map[i].length; j++) {
                stringBuilder.append(map[i][j] + " ");
            }
            stringBuilder.append("\n");
        }
        stringBuilder.append("\n");
        logger.info(stringBuilder.toString());

    }

    private void populateData(Cluster cluster, List<Integer> nodeList) {
        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[]>>();
        for(int nodeId: nodeList) {
            Node node = cluster.getNodeById(nodeId);
            storeMap.put(nodeId, ServerTestUtils.getSocketStore(ec2RebalancingTestConfig.testStoreName,
                                                                node.getHost(),
                                                                node.getSocketPort(),
                                                                RequestFormatType.PROTOCOL_BUFFERS));
        }

        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);
        for(Map.Entry<String, String> entry: testEntries.entrySet()) {
            int masterNode = routing.routeRequest(ByteUtils.getBytes(entry.getKey(), "UTF-8"))
                                    .get(0)
                                    .getId();
            if(nodeList.contains(masterNode)) {
                try {
                    ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
                    storeMap.get(masterNode)
                            .put(keyBytes,
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")));
                } catch(ObsoleteVersionException e) {
                    System.out.println("Why are we seeing this at all here ?? ");
                    e.printStackTrace();
                }
            }
        }

        // close all socket stores
        for(Store<ByteArray, byte[]> store: storeMap.values()) {
            store.close();
        }
    }

    private void rebalanceAndCheck(Cluster currentCluster,
                                   Cluster targetCluster,
                                   RebalanceController rebalanceController,
                                   List<Integer> nodeCheckList) {
        rebalanceController.rebalance(targetCluster);

        for(int nodeId: nodeCheckList) {
            List<Integer> availablePartitions = targetCluster.getNodeById(nodeId).getPartitionIds();
            List<Integer> unavailablePartitions = getUnavailablePartitions(targetCluster,
                                                                           availablePartitions);

            checkGetEntries(currentCluster.getNodeById(nodeId),
                            targetCluster,
                            unavailablePartitions,
                            availablePartitions);
        }

    }

    private void checkGetEntries(Node node,
                                 Cluster cluster,
                                 List<Integer> unavailablePartitions,
                                 List<Integer> availablePartitions) {
        int matchedEntries = 0;
        RoutingStrategy routing = new ConsistentRoutingStrategy(cluster.getNodes(), 1);

        SocketStore store = ServerTestUtils.getSocketStore(ec2RebalancingTestConfig.testStoreName,
                                                           node.getHost(),
                                                           node.getSocketPort(),
                                                           RequestFormatType.PROTOCOL_BUFFERS);

        for(Map.Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));

            List<Integer> partitions = routing.getPartitionList(keyBytes.get());

            if(null != unavailablePartitions && unavailablePartitions.containsAll(partitions)) {
                try {
                    List<Versioned<byte[]>> value = store.get(keyBytes);
                    assertEquals("unavailable partitions should return zero size list.",
                                 0,
                                 value.size());
                } catch(InvalidMetadataException e) {
                    // ignore.
                }
            } else if(null != availablePartitions && availablePartitions.containsAll(partitions)) {
                List<Versioned<byte[]>> values = store.get(keyBytes);

                // expecting exactly one version
                assertEquals("Expecting exactly one version", 1, values.size());
                Versioned<byte[]> value = values.get(0);
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), value.getVersion());
                // check value matches.
                assertEquals("Value bytes should match",
                             entry.getValue(),
                             ByteUtils.getString(value.getValue(), "UTF-8"));
                matchedEntries++;
            } else {
                // dont care about these
            }
        }

        if(null != availablePartitions && availablePartitions.size() > 0)
            assertNotSame("CheckGetEntries should match some entries.", 0, matchedEntries);
    }

    private List<Integer> getUnavailablePartitions(Cluster targetCluster,
                                                   List<Integer> availablePartitions) {
        List<Integer> unavailablePartitions = new ArrayList<Integer>();

        for(Node node: targetCluster.getNodes()) {
            unavailablePartitions.addAll(node.getPartitionIds());
        }

        unavailablePartitions.removeAll(availablePartitions);
        return unavailablePartitions;
    }

    private String getBootstrapUrl(List<String> hostnames) {
        return "tcp://" + hostnames.get(0) + ":6666";
    }

    private String getBootstrapUrl(Cluster cluster, int nodeId) {
        return getBootstrapUrl(Arrays.asList(cluster.getNodeById(nodeId).getHost()));
    }

    private static class Ec2RebalancingTestConfig extends Ec2RemoteTestConfig {

        private int numKeys;
        private int partitionsPerNode;
        private int addNodes;
        private String testStoreName = "test-replication-memory";

        private static String storeDefFile = "test/common/voldemort/config/stores.xml";
        private String configDirName;

        @Override
        protected void init(Properties properties) {
            super.init(properties);
            configDirName = properties.getProperty("ec2ConfigDirName");
            numKeys = Integer.valueOf(properties.getProperty("rebalancingNumKeys", "10000"));
            partitionsPerNode = Integer.valueOf(properties.getProperty("rebalancingPartitionsPerNode",
                                                                       "4"));
            addNodes = Integer.valueOf(properties.getProperty("rebalancingAddNodes", "0"));
            try {
                FileUtils.copyFileToDirectory(new File(storeDefFile), new File(configDirName));
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected List<String> getRequiredPropertyNames() {
            List<String> requireds = super.getRequiredPropertyNames();
            requireds.add("ec2ConfigDirName");

            return requireds;
        }
    }
}

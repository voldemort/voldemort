package voldemort.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.rebalance.RebalanceClient;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.SocketStore;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.cleanupCluster;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.toHostNames;


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

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hostNames != null)
            destroyInstances(hostNames, ec2RebalancingTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        Thread.sleep(5000);
        logger.info("Before()");
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        spareNode = ec2RebalancingTestConfig.addNodes == 0;
        partitionMap = getPartitionMap(clusterSize, ec2RebalancingTestConfig.partitionsPerNode, spareNode);

        if (logger.isInfoEnabled())
            logPartitionMap(partitionMap, "Original");

        originalCluster = ServerTestUtils.getLocalCluster(clusterSize,
                                                          getPorts(clusterSize),
                                                          partitionMap);
        nodeIds = generateClusterDescriptor(hostNamePairs, originalCluster, ec2RebalancingTestConfig);

        deploy(hostNames, ec2RebalancingTestConfig);
        startClusterAsync(hostNames, ec2RebalancingTestConfig, nodeIds);

        if (logger.isInfoEnabled())
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
    public void testSingleRebalancing() throws Exception {
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        int[][] targetLayout;

        if (spareNode)
            targetLayout = splitLastPartition(partitionMap, partitionMap[clusterSize-2].length-2);
        else
            targetLayout = insertNode(partitionMap, partitionMap[clusterSize-1].length-2);


        if (logger.isInfoEnabled())
            logPartitionMap(targetLayout,"Target");

        Cluster targetCluster = ServerTestUtils.getLocalCluster(targetLayout.length,
                                                                getPorts(targetLayout.length),
                                                                targetLayout);

        List<Integer> originalNodes = new ArrayList<Integer>();
        for (Node node: originalCluster.getNodes()) {
            if (node.getId() == (clusterSize-1) && spareNode)
                break;
            originalNodes.add(node.getId());
        }

        try {
            targetCluster = expandCluster(targetCluster.getNumberOfNodes() - clusterSize, targetCluster);
            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(Arrays.asList(originalCluster
                    .getNodeById(0)
                    .getHost())),
                                                                  new RebalanceClientConfig());
            populateData(originalCluster, originalNodes);
            rebalanceAndCheck(originalCluster,
                              targetCluster,
                              rebalanceClient,
                              originalNodes);
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    @Test
    public void testProxyGetDuringRebalancing() throws Exception {
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        int[][] targetLayout;

        if (spareNode)
            targetLayout = splitLastPartition(partitionMap, partitionMap[clusterSize-2].length-2);
        else
            targetLayout = insertNode(partitionMap, partitionMap[clusterSize-1].length-2);


        if (logger.isInfoEnabled())
            logPartitionMap(targetLayout,"Target");

        final Cluster targetCluster = expandCluster(ec2RebalancingTestConfig.addNodes,
                                                    ServerTestUtils.getLocalCluster(targetLayout.length,
                                                                                    getPorts(targetLayout.length),
                                                                                    targetLayout));

        List<Integer> originalNodes = new ArrayList<Integer>();
        for (Node node: originalCluster.getNodes()) {
            if (node.getId() == (clusterSize-1) && spareNode)
                break;
            originalNodes.add(node.getId());
        }

        try {
            ExecutorService executorService = Executors.newFixedThreadPool(2);

            final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
            final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

            populateData(originalCluster, originalNodes);

            final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig()
                    .setBootstrapUrls(getBootstrapUrl(originalCluster, 0)));

            final StoreClient<String, String> storeClient = new DefaultStoreClient<String, String>(ec2RebalancingTestConfig.testStoreName,
                                                                                                   null,
                                                                                                   factory,
                                                                                                   5);
            final boolean[] masterNodeResponded = { false, false };

            // start get operation.
            executorService.execute(new Runnable() {

                public void run() {
                    try {
                        List<String> keys = new ArrayList<String>(testEntries.keySet());

                        while(!rebalancingToken.get()) {
                            // should always able to get values.
                            int index = (int) (Math.random() * keys.size());
                            // should get a valid value
                            try {
                                Versioned<String> value = storeClient.get(keys.get(index));
                                assertNotNull("StoreClient get() should not return null.", value);
                                assertEquals("Value returned should be good",
                                             new Versioned<String>(testEntries.get(keys.get(index))),
                                             value);
                                int masterNode = storeClient.getResponsibleNodes(keys.get(index))
                                                                                     .get(0)
                                                                                     .getId();
                                masterNodeResponded[masterNode] = true;

                            } catch(UnreachableStoreException e) {
                                // ignore
                            } catch(Exception e) {
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

            executorService.execute(new Runnable() {

                public void run() {
                    try {
                        Thread.sleep(100);

                        RebalanceClient rebalanceClient = new
                                        RebalanceClient(getBootstrapUrl(originalCluster, 0),
                                                        new RebalanceClientConfig());
                        
                        rebalanceAndCheck(originalCluster,
                                          targetCluster,
                                          rebalanceClient,
                                          Arrays.asList(1));

                        // sleep for 1 min before stopping servers
                        Thread.sleep(60 * 1000);

                        rebalancingToken.set(true);

                    } catch(Exception e) {
                        exceptions.add(e);
                    }
                }
            });

            executorService.shutdown();
            executorService.awaitTermination(300, TimeUnit.SECONDS);

            assertTrue("Client should see values returned master at both (0,1):("
                       + masterNodeResponded[0] + "," + masterNodeResponded[1] + ")",
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

    private Cluster updateCluster(Cluster templateCluster, Map<String, Integer> nodeIds) {
        List<Node> nodes = new ArrayList<Node>();
        for (Map.Entry<String,Integer> entry: nodeIds.entrySet()) {
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
        if (newNodes > 0) {
            List<HostNamePair> newInstances = createInstances(newNodes, ec2RebalancingTestConfig);
            List<String> newHostnames = toHostNames(newInstances);

            if (logger.isInfoEnabled())
                logger.info("Sleeping for 30 seconds to let new instances startup");

            Thread.sleep(30000);

            hostNamePairs.addAll(newInstances);
            hostNames = toHostNames(hostNamePairs);

            nodeIds = generateClusterDescriptor(hostNamePairs, newCluster, ec2RebalancingTestConfig);

            deploy(newHostnames, ec2RebalancingTestConfig);
            startClusterAsync(newHostnames, ec2RebalancingTestConfig, nodeIds);

            if (logger.isInfoEnabled()) {
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

          for (i=0, k=0; i<limit; i++) {
              partitionMap[i] = new int[perNode];
              for (int j=0; j < perNode; j++)
                  partitionMap[i][j] = k++;
          }

          if (spareNode)
              partitionMap[limit] = new int[] {};

          return partitionMap;
      }

      private static int[][] insertNode(int[][] template, int pivot) {
          int templateLength = template.length;
          int vectorTailLength = template[templateLength-1].length - pivot;

          int[][] layout = new int[templateLength+1][];
          layout[templateLength-1] = new int[pivot];
          layout[templateLength] = new int[vectorTailLength];

          System.arraycopy(template, 0, layout, 0,  templateLength-1);
          System.arraycopy(template[templateLength-1], 0, layout[templateLength-1], 0, pivot);
          System.arraycopy(template[templateLength-1], pivot, layout[templateLength], 0, vectorTailLength);

          return layout;
      }

      private static int[][] splitLastPartition(int[][] template, int pivot) {
          int templateLength = template.length;
          int vectorTailLength = template[templateLength-2].length - pivot;

          int[][] layout = new int[templateLength][];
          layout[templateLength-2] = new int[pivot];
          layout[templateLength-1] = new int[vectorTailLength];

          System.arraycopy(template, 0, layout, 0,  templateLength-2);
          System.arraycopy(template[templateLength-2], 0, layout[templateLength-2], 0, pivot);
          System.arraycopy(template[templateLength-2], pivot, layout[templateLength-1], 0, vectorTailLength);

          return layout;
      }

      private static int[] getPorts(int count) {
          int[] ports = new int[count*3];
          for (int i = 0; i < count; i++) {
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
        for (int i=0; i < map.length; i++) {
            stringBuilder.append("node " + i + ": ");
            for (int j=0; j < map[i].length; j++) {
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
        for(Store store: storeMap.values()) {
            store.close();
        }
    }

    private void rebalanceAndCheck(Cluster currentCluster,
                                   Cluster targetCluster,
                                   RebalanceClient rebalanceClient,
                                   List<Integer> nodeCheckList) {
        rebalanceClient.rebalance(currentCluster, targetCluster);

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
            partitionsPerNode = Integer.valueOf(properties.getProperty("rebalancingPartitionsPerNode", "4"));
            addNodes = Integer.valueOf(properties.getProperty("rebalancingAddNodes", "0"));
            try {
                FileUtils.copyFileToDirectory(new File(storeDefFile), new File(configDirName));
            } catch (IOException e)  {
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

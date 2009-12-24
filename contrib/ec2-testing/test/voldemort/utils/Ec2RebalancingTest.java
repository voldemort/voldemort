package voldemort.utils;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import voldemort.ServerTestUtils;
import voldemort.client.rebalance.RebalanceClient;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
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

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2RebalancingTestConfig = new Ec2RebalancingTestConfig();
        hostNamePairs = createInstances(ec2RebalancingTestConfig);
        hostNames = toHostNames(hostNamePairs);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(3000);
        
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hostNames != null)
            destroyInstances(hostNames, ec2RebalancingTestConfig);
    }

    private int[][] getPartitionMap(int nodes, int perNode) {
        int[][] partitionMap = new int[nodes][perNode];
        int i, k;

        for (i=0, k=0; i<nodes; i++)
            for (int j=0; j < perNode; j++)
                partitionMap[i][j] = k++;

        return partitionMap;
    }

    private int[][] insertNode(int[][] template, int pivot) {
        int len = template.length;

        int[][] layout = new int[len+1][];
        System.arraycopy(template, 0, layout, 0, len-1);

        layout[len-1] = new int[len-1];
        System.arraycopy(template[len-1], 0, layout[len-1], 0, pivot);

        layout[len] = new int[template[len-1].length - pivot];
        System.arraycopy(template[len-1], pivot, layout[len], 0, template[len-1].length - pivot);

        return layout;
    }

    private int[] getPorts(int count) {
        int[] ports = new int[count*3];
        for (int i = 0; i < count; i += 3) {
            ports[i] = 6665;
            ports[i+1] = 6666;
            ports[i+2] = 6667;
        }

        return ports;
    }

    @Before
    public void setUp() throws Exception {
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        partitionMap = getPartitionMap(clusterSize, ec2RebalancingTestConfig.partitionsPerNode);
        originalCluster = ServerTestUtils.getLocalCluster(clusterSize,
                                                          getPorts(clusterSize),
                                                          partitionMap);


        deploy(hostNames, ec2RebalancingTestConfig);
        startClusterAsync(hostNames, ec2RebalancingTestConfig, nodeIds);

        nodeIds = generateClusterDescriptor(hostNamePairs, originalCluster, ec2RebalancingTestConfig);
        testEntries = ServerTestUtils.createRandomKeyValueString(ec2RebalancingTestConfig.numKeys);

        originalCluster = updateCluster(originalCluster, nodeIds);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to let the Voldemort cluster start");
        
        Thread.sleep(3000);

    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2RebalancingTestConfig);
    }

    
    private Cluster updateCluster(Cluster templateCluster, Map<String, Integer> nodeIds) {
        List<Node> nodes = new LinkedList<Node>();
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
        assert(newNodes > 0);

        List<HostNamePair> newInstances = createInstances(newNodes, ec2RebalancingTestConfig);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to let new instances startup");

        Thread.sleep(15000);

        hostNamePairs.addAll(newInstances);
        hostNames = toHostNames(hostNamePairs);

        nodeIds = generateClusterDescriptor(hostNamePairs, newCluster, ec2RebalancingTestConfig);

        logger.info("Expanded the cluster. New layout: " + nodeIds);

        return updateCluster(newCluster, nodeIds);
    }

    @Test
    public void testSingleRebalancing() throws Exception {
        int clusterSize = ec2RebalancingTestConfig.getInstanceCount();
        int[][] targetLayout = insertNode(partitionMap, partitionMap[clusterSize-1].length-1);
        Cluster targetCluster = ServerTestUtils.getLocalCluster(clusterSize+1,
                                                                getPorts(clusterSize+1),
                                                                targetLayout);
        List<Integer> originalNodes = Lists.transform(Lists.<Node>newLinkedList(originalCluster.getNodes()),
                                                          new Function<Node, Integer> () {
                                                              public Integer apply(Node node) {
                                                                  return node.getId();
                                                              }
                                                          });
        targetCluster = expandCluster(targetCluster.getNumberOfNodes() - clusterSize, targetCluster);
        try {
            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(hostNames),
                                                                  new RebalanceClientConfig());
            populateData(originalCluster, originalNodes);
            rebalanceAndCheck(originalCluster, targetCluster, rebalanceClient, Arrays.asList(clusterSize));
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    private void populateData(Cluster cluster, List<Integer> nodeList) {
        // TODO: implement this
    }

    private void rebalanceAndCheck(Cluster currentCluster,
                                   Cluster targetCluster,
                                   RebalanceClient rebalanceClient,
                                   List<Integer> nodeCheckList) {
        // TODO: implement this
    }

    private String getBootstrapUrl(List<String> hostnames) {
        return "tcp://" + hostnames.get(0) + ":6666";
    }

    @Test
    public void testProxyGetDuringRebalancing() throws Exception {
        try {
            // TODO: implement this
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }


    private static class Ec2RebalancingTestConfig extends Ec2RemoteTestConfig {
        private int numKeys;
        private int partitionsPerNode;
        private static String testStoreName = "test-replication-memory";
        private static String storeDefFile = "test/common/voldemort/config/stores.xml";
        private String configDirName;

        @Override
        protected void init(Properties properties) {
            super.init(properties);
            configDirName = properties.getProperty("ec2ConfigDirName");
            numKeys = Integer.valueOf(properties.getProperty("rebalancingNumKeys", "10000"));
            partitionsPerNode = Integer.valueOf(properties.getProperty("partitionsPerNode", "3"));
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

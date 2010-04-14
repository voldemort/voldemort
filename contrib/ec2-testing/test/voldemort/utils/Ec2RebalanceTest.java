package voldemort.utils;

import static org.junit.Assert.fail;
import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.cleanupCluster;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.AbstractRebalanceTest;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.RequestRoutingType;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.ClientRequestExecutorPool;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.versioning.Versioned;

/**
 */
public class Ec2RebalanceTest extends AbstractRebalanceTest {

    private static final Logger logger = Logger.getLogger(Ec2RebalanceTest.class);
    private static Ec2RebalanceTestConfig ec2RebalanceTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;

    private Map<Integer, String> nodeIdsInv = new HashMap<Integer, String>();
    private List<String> activeHostNames = new ArrayList<String>();

    @BeforeClass
    public static void ec2Setup() throws Exception {
        ec2RebalanceTestConfig = new Ec2RebalanceTestConfig();
        NUM_KEYS = ec2RebalanceTestConfig.numKeys;

        hostNamePairs = createInstances(ec2RebalanceTestConfig);
        hostNames = toHostNames(hostNamePairs);

        logger.info("Sleeping for 30 seconds to let the instances start up.");
        Thread.sleep(30000);
    }

    @AfterClass
    public static void ec2TearDown() throws Exception {
        if(hostNames != null)
            destroyInstances(hostNames, ec2RebalanceTestConfig);
    }

    @After
    public void ec2Cleanup() throws Exception {
        if(activeHostNames.size() > 0) {
            stopClusterQuiet(activeHostNames, ec2RebalanceTestConfig);
            cleanupCluster(activeHostNames, ec2RebalanceTestConfig);
        }
    }

    @Override
    protected Cluster updateCluster(Cluster template) {
        List<Node> nodes = new ArrayList<Node>();
        for(Map.Entry<Integer, String> entry: nodeIdsInv.entrySet()) {
            int nodeId = entry.getKey();
            String hostName = entry.getValue();
            Node tmplNode = template.getNodeById(nodeId);
            Node node = new Node(nodeId,
                                 hostName,
                                 tmplNode.getHttpPort(),
                                 tmplNode.getSocketPort(),
                                 tmplNode.getAdminPort(),
                                 tmplNode.getPartitionIds());
            nodes.add(node);
        }

        return new Cluster(template.getName(), nodes);
    }

    @Override
    protected Store<ByteArray, byte[]> getSocketStore(String storeName,
                                                      String host,
                                                      int port,
                                                      boolean isRouted) {
        SocketStoreFactory storeFactory = new ClientRequestExecutorPool(2,
                                                                        60 * 1000,
                                                                        60 * 1000,
                                                                        32 * 1024);
        RequestRoutingType requestRoutingType = RequestRoutingType.getRequestRoutingType(isRouted,
                                                                                         false);
        return storeFactory.create(storeName,
                                   host,
                                   port,
                                   RequestFormatType.PROTOCOL_BUFFERS,
                                   requestRoutingType);
    }

    @Override
    protected Cluster startServers(Cluster template,
                                   String StoreDefXmlFile,
                                   List<Integer> nodeToStart,
                                   Map<String, String> configProps) throws Exception {
        if(ec2RebalanceTestConfig.getInstanceCount() < template.getNumberOfNodes())
            throw new IllegalStateException("instanceCount must be >= number of nodes in the cluster");

        Map<String, Integer> nodeIds = generateClusterDescriptor(hostNamePairs,
                                                                 template,
                                                                 ec2RebalanceTestConfig);
        List<Node> nodes = new ArrayList<Node>();
        for(Map.Entry<String, Integer> entry: nodeIds.entrySet()) {
            String hostName = entry.getKey();
            int nodeId = entry.getValue();
            Node tmplNode = template.getNodeById(nodeId);
            Node node = new Node(nodeId,
                                 hostName,
                                 tmplNode.getHttpPort(),
                                 tmplNode.getSocketPort(),
                                 tmplNode.getAdminPort(),
                                 tmplNode.getPartitionIds());
            nodes.add(node);
            nodeIdsInv.put(nodeId, hostName);
            activeHostNames.add(hostName);
        }

        Cluster cluster = new Cluster(template.getName(), nodes);

        deploy(activeHostNames, ec2RebalanceTestConfig);
        startClusterAsync(activeHostNames, ec2RebalanceTestConfig, nodeIds);

        logger.info("Sleeping for ten seconds to let Voldemort start.");
        Thread.sleep(10000);

        return cluster;
    }

    @Override
    protected void stopServer(List<Integer> nodesToStop) throws Exception {
        List<String> hostsToStop = new ArrayList<String>();
        for(int nodeId: nodesToStop) {
            hostsToStop.add(nodeIdsInv.get(nodeId));
        }
        stopCluster(hostsToStop, ec2RebalanceTestConfig);
    }

    @Test
    public void testGracefulRecovery() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 4, 5, 6, 7, 8 }, { 2, 3 } });

        List<Integer> serverList = Arrays.asList(0, 1);
        currentCluster = startServers(currentCluster, storeDefFile, serverList, null);
        targetCluster = updateCluster(targetCluster);

        populateData(currentCluster, Arrays.asList(0));

        AdminClient adminClient = new AdminClient(getBootstrapUrl(currentCluster, 0),
                                                  new AdminClientConfig());
        RebalancePartitionsInfo rebalancePartitionsInfo = new RebalancePartitionsInfo(1,
                                                                                      0,
                                                                                      Arrays.asList(2,
                                                                                                    3),
                                                                                      new ArrayList<Integer>(0),
                                                                                      Arrays.asList(testStoreName),
                                                                                      0);
        int requestId = adminClient.rebalanceNode(rebalancePartitionsInfo);
        logger.info("started rebalanceNode, request id = " + requestId);

        Thread.sleep(25);
        stopServer(Arrays.asList(1));
        logger.info("waiting ten seconds after shutting down the node");

        Thread.sleep(10000);

        String hostName = currentCluster.getNodeById(1).getHost();
        startClusterNode(hostName, ec2RebalanceTestConfig, 1);

        adminClient.stop();
        adminClient = new AdminClient(getBootstrapUrl(currentCluster, 0), new AdminClientConfig());
        Versioned<MetadataStore.VoldemortState> serverState = adminClient.getRemoteServerState(1);

        int delay = 250;
        int maxDelay = 1000 * 30;
        int timeout = 5 * 1000 * 60;
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() < start + timeout
              && serverState.getValue() != MetadataStore.VoldemortState.NORMAL_SERVER) {
            Thread.sleep(delay);
            if(delay < maxDelay)
                delay *= 2;
            serverState = adminClient.getRemoteServerState(1);
            logger.info("serverState -> " + serverState.getValue());
        }

        if(serverState.getValue() == MetadataStore.VoldemortState.NORMAL_SERVER) {
            for(int nodeId: Arrays.asList(1)) {
                List<Integer> availablePartitions = targetCluster.getNodeById(nodeId)
                                                                 .getPartitionIds();
                List<Integer> unavailablePartitions = getUnavailablePartitions(targetCluster,
                                                                               availablePartitions);

                try {
                    checkGetEntries(currentCluster.getNodeById(nodeId),
                                    targetCluster,
                                    unavailablePartitions,
                                    availablePartitions);
                } catch(InvalidMetadataException e) {
                    logger.warn(e);
                }
            }
        } else
            fail("Server state never reached NORMAL_SERVER");
    }

    private static class Ec2RebalanceTestConfig extends Ec2RemoteTestConfig {

        private String configDirName;
        private int numKeys;

        @Override
        protected void init(Properties properties) {
            super.init(properties);
            configDirName = properties.getProperty("ec2ConfigDirName");
            numKeys = getIntProperty(properties, "ec2NumKeys", 1000);

            try {
                FileUtils.copyFile(new File(storeDefFile), new File(configDirName + "/stores.xml"));
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

package voldemort.utils;

import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.cleanupCluster;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.AbstractRebalanceTest;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

/**
 */
public class Ec2RebalanceTest extends AbstractRebalanceTest {

    private static int NUM_KEYS;

    private static final Logger logger = Logger.getLogger(Ec2RebalanceTest.class);
    private static Ec2RebalanceTestConfig ec2RebalanceTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;

    private Map<Integer, String> nodeIdsInv = new HashMap<Integer, String>();
    private List<String> activeHostNames = new ArrayList<String>();
    private boolean useDonorBased = true;

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

    @Override
    protected int getNumKeys() {
        return NUM_KEYS;
    }

    @Override
    protected Cluster getCurrentCluster(int nodeId) {
        String hostName = nodeIdsInv.get(nodeId);
        if(hostName == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            AdminClient adminClient = new AdminClient(hostName, new AdminClientConfig());
            return adminClient.getAdminClientCluster();
        }
    }

    @Override
    protected VoldemortState getCurrentState(int nodeId) {
        String hostName = nodeIdsInv.get(nodeId);
        if(hostName == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            AdminClient adminClient = new AdminClient(hostName, new AdminClientConfig());
            return adminClient.getRemoteServerState(nodeId).getValue();
        }
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
    protected Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
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

    @Override
    protected boolean useDonorBased() {
        return this.useDonorBased;
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
                FileUtils.copyFile(new File(storeDefFileWithoutReplication),
                                   new File(configDirName + "/stores.xml"));
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

package voldemort.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.RebalanceClient;
import voldemort.client.rebalance.RebalanceClientConfig;
import voldemort.cluster.Cluster;
import voldemort.xml.ClusterMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
    private static Map<String, Integer> nodeIds;


    
    private static final Logger logger = Logger.getLogger(Ec2RebalancingTest.class);

    private Map<String, String> testEntries;

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2RebalancingTestConfig = new Ec2RebalancingTestConfig();
        hostNamePairs = createInstances(ec2RebalancingTestConfig);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2RebalancingTestConfig, true);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(3000);
        
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hostNames != null)
            destroyInstances(hostNames, ec2RebalancingTestConfig);

    }

    @Before
    public void setUp() throws Exception {
        deploy(hostNames, ec2RebalancingTestConfig);
        startClusterAsync(hostNames, ec2RebalancingTestConfig, nodeIds);

        testEntries = ServerTestUtils.createRandomKeyValueString(ec2RebalancingTestConfig.numKeys);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to let the Voldemort cluster start");
        
        Thread.sleep(3000);

    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2RebalancingTestConfig);
    }

    @Test
    public void testSingleRebalancing() throws Exception {
        Cluster originalCluster = getOriginalCluster();
        Cluster targetCluster = getTargetCluster(1);

        try {
            RebalanceClient rebalanceClient = new RebalanceClient(getBootstrapUrl(hostNames),
                                                                  new RebalanceClientConfig());

            
        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }

    public Cluster getOriginalCluster() {
        AdminClient adminClient = new AdminClient(getBootstrapUrl(hostNames), new AdminClientConfig());
        return adminClient.getAdminClientCluster();
    }

    public Cluster getTargetCluster(int toAdd) throws Exception {
        List<HostNamePair> addlInstances = createInstances(toAdd, ec2RebalancingTestConfig);
        List<String> addlHostNames = toHostNames(addlInstances);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to let the new instances startup");

        hostNamePairs.addAll(addlInstances);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2RebalancingTestConfig, true);
        
        deploy(addlHostNames, ec2RebalancingTestConfig);
        startClusterAsync(addlHostNames, ec2RebalancingTestConfig, nodeIds);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 seconds to start voldemort on the new nodes");

        Thread.sleep(15000);

        AdminClient adminClient = new AdminClient(getBootstrapUrl(addlHostNames), new AdminClientConfig());

        return adminClient.getAdminClientCluster();
        
    }

    public String getBootstrapUrl(List<String> hostnames) {
        return "tcp://" + hostnames.get(0) + ":6666";
    }

    @Test
    public void testProxyGetDuringRebalancing() throws Exception {
        try {

        } finally {
            stopCluster(hostNames, ec2RebalancingTestConfig);
        }
    }


    private static class Ec2RebalancingTestConfig extends Ec2RemoteTestConfig {
        private int numKeys;
        private static String testStoreName = "test-replication-memory";
        private static String storeDefFile = "test/common/voldemort/config/stores.xml";
        private String configDirName;

        @Override
        protected void init(Properties properties) {
            super.init(properties);
            configDirName = properties.getProperty("ec2ConfigDirName");
            numKeys = Integer.valueOf(properties.getProperty("rebalancingNumKeys", "10000"));

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

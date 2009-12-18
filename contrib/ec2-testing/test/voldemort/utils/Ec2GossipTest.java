package voldemort.utils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.*;
import voldemort.Attempt;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;


import java.io.StringReader;
import java.util.*;

import static voldemort.utils.Ec2RemoteTestUtils.createInstances;
import static voldemort.utils.Ec2RemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.executeRemoteTest;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.toHostNames;
import static voldemort.TestUtils.assertWithBackoff;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;



/**
 *
 * @author afeinberg
 */
public class Ec2GossipTest {
    private static Ec2GossipTestConfig ec2GossipTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(Ec2GossipTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        ec2GossipTestConfig = new Ec2GossipTestConfig();
        hostNamePairs = createInstances(ec2GossipTestConfig);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2GossipTestConfig);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hostNames != null)
            destroyInstances(hostNames, ec2GossipTestConfig);
    }

    @Before
    public void setUp() throws Exception {
        deploy(hostNames, ec2GossipTestConfig);
        startClusterAsync(hostNames, ec2GossipTestConfig, nodeIds);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, ec2GossipTestConfig);
    }

    @Test
    public void testGossip() throws Exception {
        try {
            Set<String> oldHostnames = new HashSet<String>(hostNames);
            Set<Integer> oldNodeIdSet = new HashSet<Integer>(nodeIds.values());
            Map<String,Integer> oldNodeIdMap = new HashMap<String,Integer>(nodeIds);

            logger.info("Cluster before expanding: " + nodeIds);

            /**
             * First verify that the cluster had been expanded correctly
             */

            Pair<List<Integer>, List<String>> pair = expandCluster();

            final List<Integer> newNodeIds = pair.getFirst();
            List<String> newHostnames = pair.getSecond();

            assertEquals("correct number of nodes added", newNodeIds.size(), ec2GossipTestConfig.numNewNodes);

            boolean containsOldHostnames = false;
            for (String newHostname : newHostnames) {
                if (oldHostnames.contains(newHostname)) {
                    containsOldHostnames = true;
                    break;
                }
            }

            boolean containsOldNodeIds = false;
            for (Integer newNodeId: newNodeIds) {
                if (oldNodeIdSet.contains(newNodeId)) {
                    containsOldNodeIds = true;
                    break;
                }
            }

            assertFalse("none of the new nodes is an old hostname", containsOldHostnames);
            assertFalse("none of the new nodes is an old node id", containsOldNodeIds);

            for (String oldHostname: oldHostnames) {
                assertEquals("hostname to nodeId mapping preserved for " + oldHostname,
                             oldNodeIdMap.get(oldHostname),
                             nodeIds.get(oldHostname));
            }

            /**
             * Now increment the version on the new nodes, start gossiping
             */
            int peerNodeId = Iterables.find(nodeIds.values(),
                                          new Predicate<Integer>() {
                                              public boolean apply(Integer input) {
                                                  return !newNodeIds.contains(input);
                                              }
                                          });
            for (String hostname: newHostnames) {
                int nodeId = nodeIds.get(hostname);
                AdminClient adminClient = new AdminClient("tcp:// " + hostname + ":6666", new AdminClientConfig());

                Versioned<String> versioned = adminClient.getRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY);
                Version version = versioned.getVersion();
                
                VectorClock vectorClock = (VectorClock) version;
                vectorClock.incrementVersion(nodeId,  vectorClock.getTimestamp() + 1);
                vectorClock.incrementVersion(peerNodeId, vectorClock.getTimestamp() + 1);
                
                adminClient.updateRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY, versioned);
                adminClient.updateRemoteMetadata(peerNodeId, MetadataStore.CLUSTER_KEY, versioned);
            }

            /**
             * Finally, verify that all of the nodes have been discovered
             */
            assertWithBackoff(1000, 60000, new Attempt() {
                AdminClient adminClient = new AdminClient("tcp://" + hostNames.get(0) + ":6666",
                                                          new AdminClientConfig());
                public void checkCondition() {
                    for (int testNodeId: nodeIds.values()) {
                        ClusterMapper clusterMapper = new ClusterMapper();
                        Versioned<String> clusterXml = adminClient.getRemoteMetadata(testNodeId,
                                                                                     MetadataStore.CLUSTER_KEY);
                        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml.getValue()));
                        Set<Integer> allNodeIds = new HashSet<Integer>();
                        for (Node node: cluster.getNodes()) {
                            allNodeIds.add(node.getId());
                        }
                        assertTrue("all nodes nodes discovered by node id " + testNodeId,
                                   allNodeIds.containsAll(nodeIds.values()));

                    }
                }
            });
        } finally {
            stopCluster(hostNames, ec2GossipTestConfig);
        }
    }

    private static Pair<List<Integer>,List<String>> expandCluster() throws Exception {
        List<HostNamePair> newInstances = createInstances(ec2GossipTestConfig.numNewNodes, ec2GossipTestConfig);
        List<String> newHostnames = toHostNames(newInstances);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 15 to let new instances start up");

        Thread.sleep(15000);


        hostNamePairs.addAll(newInstances);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", ec2GossipTestConfig);

        logger.info("Expanded the cluster. New layout: " + nodeIds);

        deploy(newHostnames, ec2GossipTestConfig);
        startClusterAsync(newHostnames, ec2GossipTestConfig, nodeIds);

        if (logger.isInfoEnabled())
            logger.info("Sleeping for 5 seconds to start voldemort on the new nodes");

        Thread.sleep(5000);
        
        return new Pair<List<Integer>,List<String>>(
                       Lists.transform(newHostnames,
                                       new Function<String, Integer>() {
                                           public Integer apply(String hostname) {
                                               return nodeIds.get(hostname);
                                           }
                                       }),
                       newHostnames);
    }

    private static class Ec2GossipTestConfig extends Ec2RemoteTestConfig {
        private int numNewNodes;
        
        @Override
        protected void init(Properties properties) {
            super.init(properties);

            numNewNodes = getIntProperty(properties, "gossipNumNewNodes");
        }

        @Override
        protected List<String> getRequiredPropertyNames() {
            List<String> requireds = super.getRequiredPropertyNames();
            requireds.addAll(Arrays.asList("gossipNumNewNodes"));
            return requireds;
        }
    }
}

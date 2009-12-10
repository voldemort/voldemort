package voldemort.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.Attempt;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;

import static voldemort.utils.Ec2InstanceRemoteTestUtils.createInstances;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.toHostNames;
import static voldemort.utils.Ec2InstanceRemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.TestUtils.assertWithBackoff;

import static org.junit.Assert.assertEquals;

/**
 * @author afeinberg
 */
public class Ec2GossipTest {
    private static String accessId;
    private static String secretKey;
    private static String ami;
    private static String keyPairId;
    private static String sshPrivateKeyPath;
    private static String hostUserId;
    private static File sshPrivateKey;
    private static String voldemortRootDirectory;
    private static String voldemortHomeDirectory;
    private static File sourceDirectory;
    private static String parentDirectory;
    private static File clusterXmlFile;

    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(Ec2GossipTest.class);
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Properties properties = getEc2Properties();

        accessId = properties.getProperty("ec2AccessId");
        secretKey = properties.getProperty("ec2SecretKey");
        ami = properties.getProperty("ec2Ami");
        keyPairId = properties.getProperty("ec2KeyPairId");
        sshPrivateKeyPath = properties.getProperty("ec2SshPrivateKeyPath");
        hostUserId = properties.getProperty("ec2HostUserId");

        sshPrivateKey = sshPrivateKeyPath != null ? new File(sshPrivateKeyPath) : null;
        voldemortRootDirectory = properties.getProperty("ec2VoldemortRootDirectory");
        voldemortHomeDirectory = properties.getProperty("ec2VoldemortHomeDirectory");
        sourceDirectory = new File(properties.getProperty("ec2SourceDirectory"));
        parentDirectory = properties.getProperty("ec2ParentDirectory");
        clusterXmlFile = new File(properties.getProperty("ec2ClusterXmlFile"));
        int ec2InstanceCount = Integer.parseInt(properties.getProperty("ec2InstanceCount"));

        hostNamePairs = createInstances(accessId, secretKey, ami, keyPairId, ec2InstanceCount);

        hostNames = toHostNames(hostNamePairs);

        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @Test
    public static void testGossip() throws Exception {
        // First deploy an initial cluster
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);

        try {
            startClusterAsync(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory,
                    voldemortHomeDirectory, nodeIds);
            Pair<HostNamePair, Integer> newInstance = createAndDeployNewInstance();
            final String newHostname = newInstance.getFirst().getExternalHostName();
            final int nodeId = newInstance.getSecond();

            startClusterNode(newHostname, sshPrivateKey, hostUserId, voldemortRootDirectory,
                    voldemortHomeDirectory, nodeId);

            Thread.sleep(5000);
            if (logger.isInfoEnabled())
                logger.info("Sleeping for 5 seconds to start Voldemort on the new node.");

            final AdminClient adminClient = getAdminClient(newHostname);
            Versioned<String> versioned = adminClient.getRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY);

            // Find a node that isn't the new node we added
            Integer seedNode = Iterables.find(nodeIds.values(), new Predicate<Integer> () {
                public boolean apply(Integer input) {
                    return !input.equals(nodeId);
                }
            });

            Version version = versioned.getVersion();
            ((VectorClock) version).incrementVersion(nodeId, ((VectorClock) version).getTimestamp() + 1);
            ((VectorClock) version).incrementVersion(seedNode, ((VectorClock) version).getTimestamp() + 1);

            adminClient.updateRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY, versioned);
            adminClient.updateRemoteMetadata(seedNode, MetadataStore.CLUSTER_KEY, versioned);

            assertWithBackoff(500, 10000, new Attempt() {
                public void checkCondition() {
                    int nodesAware = 0;
                    for (int testNodeId: nodeIds.values()) {
                        ClusterMapper clusterMapper = new ClusterMapper();
                        Versioned<String> clusterXml = adminClient.getRemoteMetadata(testNodeId,
                                MetadataStore.CLUSTER_KEY);
                        Cluster cluster = clusterMapper.readCluster(new StringReader(clusterXml.getValue()));
                        boolean foundNode = false;
                        for (Node node: cluster.getNodes()) {
                            if (node.getHost().equals(newHostname) &&
                                    node.getId() == nodeId) {
                                foundNode = true;
                                break;
                            }
                        }
                        if (foundNode)
                            nodesAware++;

                    }
                    assertEquals ("All nodes aware of " + nodeId, nodesAware, nodeIds.size());
                }
            });

        }  finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
        
    }

    private static Pair<HostNamePair, Integer> createAndDeployNewInstance() throws Exception {
        HostNamePair newInstance = createInstances(accessId, secretKey, ami, keyPairId, 1).get(0);
        hostNamePairs.add(newInstance);
        hostNames = toHostNames(hostNamePairs);
        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);

        Thread.sleep(15000);

        if (logger.isInfoEnabled())
            logger.info("Sleep for 15 seconds to give the new EC2 instance some time to startup");

        deploy(ImmutableList.of(newInstance.getExternalHostName()), sshPrivateKey, hostUserId, sourceDirectory,
                parentDirectory);
        
        return new Pair<HostNamePair, Integer>(newInstance, nodeIds.get(newInstance.getExternalHostName()));
    }

    private static AdminClient getAdminClient(String hostname) {
        return new ProtoBuffAdminClientRequestFormat("tcp://" + hostname + ":6666", new ClientConfig());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hostNames != null)
            destroyInstances(accessId, secretKey, hostNames);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
    }

    private static Properties getEc2Properties() throws Exception {
        String propertiesFileName = System.getProperty("ec2PropertiesFile");


        String[] requireds = { "ec2AccessId", "ec2SecretKey", "ec2Ami", "ec2KeyPairId",
                "ec2HostUserId", "ec2VoldemortRootDirectory", "ec2VoldemortHomeDirectory",
                "ec2SourceDirectory", "ec2ParentDirectory", "ec2ClusterXmlFile",
                "ec2InstanceCount", "ec2RampTime"  };

        if(propertiesFileName == null)
               throw new Exception("ec2PropertiesFile system property must be defined that "
                                   + "provides the path to file containing the following "
                                   + "required EC2 test properties: "
                                   + StringUtils.join(requireds, ", "));

        Properties properties = new Properties();
        InputStream in = null;
        try {
            in  = new FileInputStream(propertiesFileName);
            properties.load(in);
        } finally {
            IOUtils.closeQuietly(in);
        }

         for(String required: requireds) {
            // Allow system properties to override
            if(System.getProperties().containsKey(required))
                properties.put(required, System.getProperty(required));

            if(!properties.containsKey(required))
                throw new Exception("Required properties for EC2 test: "
                                    + StringUtils.join(requireds, ", ") + "; missing " + required);
        }

        return properties;
    }
}

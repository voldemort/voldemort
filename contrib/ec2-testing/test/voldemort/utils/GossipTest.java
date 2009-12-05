package voldemort.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static voldemort.utils.Ec2InstanceRemoteTestUtils.createInstances;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.toHostNames;
import static voldemort.utils.Ec2InstanceRemoteTestUtils.destroyInstances;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;

/**
 * @author afeinberg
 */
public class GossipTest {
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
    private static int rampTime;
    
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(GossipTest.class);
    
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
        rampTime = Integer.parseInt(properties.getProperty("ec2RampTime"));

        hostNamePairs = createInstances(accessId, secretKey, ami, keyPairId, ec2InstanceCount);

        hostNames = toHostNames(hostNamePairs);

        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);

    }

    @Test
    public static void testGossip() throws Exception {

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

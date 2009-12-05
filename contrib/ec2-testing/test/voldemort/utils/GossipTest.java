package voldemort.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    @BeforeClass
    public static void setUpClass() throws Exception {
        
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

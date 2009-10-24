package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SmokeTest {

    @Test
    public void test() throws Exception {
        Map<String, String> dnsNames = new HashMap<String, String>();
        dnsNames.put("ec2-174-129-150-46.compute-1.amazonaws.com",
                     "domU-12-31-39-02-21-F7.compute-1.internal");
        dnsNames.put("ec2-67-202-46-76.compute-1.amazonaws.com",
                     "domU-12-31-39-02-E4-C8.compute-1.internal");

        // dnsNames = createInstances();
        generateClusterDescriptor(dnsNames.values());

        CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(dnsNames.keySet());
        config.setHostUserId("root");
        config.setSshPrivateKey(new File("/home/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair"));
        config.setVoldemortParentDirectory("somesubdirectory");
        config.setVoldemortRootDirectory("somesubdirectory/voldemort");
        config.setVoldemortHomeDirectory("somesubdirectory/voldemort/config/single_node_cluster");
        config.setSourceDirectory(new File("/home/kirk/voldemortdev/voldemort"));

        new RsyncDeployer(config).execute();
        new SshClusterStarter(config).execute();
        Thread.sleep(15000);
        new SshClusterStopper(config).execute();
    }

    private Map<String, String> createInstances() throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        String ami = System.getProperty("ec2Ami");
        String keyPairId = System.getProperty("ec2KeyPairId");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        return ec2.createInstances(ami, keyPairId, null, 1, 360000);
    }

    private void generateClusterDescriptor(Collection<String> privateDnsNames) throws Exception {
        ClusterGenerator clusterGenerator = new ClusterGenerator();
        String clusterXml = clusterGenerator.createClusterDescriptor(new ArrayList<String>(privateDnsNames),
                                                                     3);

        // System.out.println(clusterXml); // Rad
    }

}

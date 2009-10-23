package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SmokeTest {

    private String hostUserId = "root";

    private File sshPrivateKey = new File("/home/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair");

    private String voldemortRootDirectory = "somesubdirectory";

    private String voldemortHomeDirectory = "somesubdirectory/voldemort/config/single_node_cluster";

    private File sourceDirectory = new File("/home/kirk/voldemortdev/voldemort");

    @Test
    public void test() throws Exception {
        Map<String, String> dnsNames = new HashMap<String, String>();
        dnsNames.put("ec2-75-101-226-173.compute-1.amazonaws.com", "ip-10-244-133-123.ec2.internal");
        dnsNames.put("ec2-174-129-138-76.compute-1.amazonaws.com", "ip-10-244-145-204.ec2.internal");
        dnsNames.put("ec2-75-101-199-162.compute-1.amazonaws.com", "ip-10-245-70-114.ec2.internal");

        // dnsNames = createInstances();
        generateClusterDescriptor(dnsNames.values());

        rsync(dnsNames.keySet());
        startCluster(dnsNames.keySet());

        Thread.sleep(15000);

        stopCluster(dnsNames.keySet());
    }

    private Map<String, String> createInstances() throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        String ami = System.getProperty("ec2Ami");
        String keyPairId = System.getProperty("ec2KeyPairId");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        return ec2.createInstances(ami, keyPairId, null, 3, 360000);
    }

    private void generateClusterDescriptor(Collection<String> privateDnsNames) throws Exception {
        ClusterGenerator clusterGenerator = new ClusterGenerator();
        String clusterXml = clusterGenerator.createClusterDescriptor(new ArrayList<String>(privateDnsNames),
                                                                     3);

        // System.out.println(clusterXml); // Rad
    }

    private void rsync(Collection<String> hostNames) throws Exception {
        Deployer deployer = new RsyncDeployer();
        deployer.deploy(hostNames,
                        hostUserId,
                        sshPrivateKey,
                        voldemortRootDirectory,
                        sourceDirectory,
                        600000);
    }

    private void startCluster(Collection<String> hostNames) throws Exception {
        ClusterStarter clusterStarter = new SshClusterStarter();
        clusterStarter.start(hostNames,
                             hostUserId,
                             sshPrivateKey,
                             voldemortRootDirectory + "/voldemort",
                             voldemortHomeDirectory,
                             600000);
    }

    private void stopCluster(Collection<String> hostNames) throws Exception {
        ClusterStopper clusterStopper = new SshClusterStopper();
        clusterStopper.stop(hostNames, hostUserId, sshPrivateKey, voldemortRootDirectory
                                                                  + "/voldemort", 600000);
    }

}

package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import voldemort.utils.ec2testing.Ec2Connection;
import voldemort.utils.ec2testing.TypicaEc2Connection;

public class SmokeTest {

    @Test
    public void test() throws Exception {
        Map<String, String> dnsNames = new HashMap<String, String>();
        dnsNames.put("ec2-75-101-191-205.compute-1.amazonaws.com",
                     "domU-12-31-39-00-C4-D5.compute-1.internal");

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
        return ec2.createInstances(ami, keyPairId, null, 1, 360000);
    }

    private void generateClusterDescriptor(Collection<String> privateDnsNames) throws Exception {
        ClusterDescriptorGenerator cdg = new ClusterDescriptorGenerator();
        String clusterXml = cdg.createClusterDescriptor(new ArrayList<String>(privateDnsNames), 3);

        // System.out.println(clusterXml); // Rad
    }

    private void rsync(Collection<String> hostNames) throws Exception {
        File sourceDirectory = new File("/Users/kirk/voldemortdev/voldemort");
        File sshPrivateKey = new File("/Users/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair");

        VoldemortDeployer voldemortDeployer = new RsyncVoldemortDeployer();
        voldemortDeployer.deploy(hostNames,
                                 "root",
                                 sshPrivateKey,
                                 sourceDirectory,
                                 "",
                                 "",
                                 "",
                                 "somesubdirectory");
    }

    private void startCluster(Collection<String> hostNames) throws Exception {
        File sshPrivateKey = new File("/Users/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair");

        VoldemortClusterStarter voldemortClusterStarter = new SshVoldemortClusterStarter();
        voldemortClusterStarter.start(hostNames,
                                      "root",
                                      sshPrivateKey,
                                      "somesubdirectory/voldemort",
                                      "somesubdirectory/voldemort/config/single_node_cluster");
    }

    private void stopCluster(Collection<String> hostNames) throws Exception {
        File sshPrivateKey = new File("/Users/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair");

        VoldemortClusterStopper voldemortClusterStopper = new SshVoldemortClusterStopper();
        voldemortClusterStopper.stop(hostNames, "root", sshPrivateKey, "somesubdirectory/voldemort");
    }

}

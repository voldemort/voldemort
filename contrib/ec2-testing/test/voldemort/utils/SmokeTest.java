package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class SmokeTest {

    @Test
    public void test() throws Exception {
        Map<String, String> dnsNames = new HashMap<String, String>();
        dnsNames.put("ec2-75-101-191-206.compute-1.amazonaws.com",
                     "domU-12-31-39-07-84-D2.compute-1.internal");
        dnsNames.put("ec2-67-202-36-69.compute-1.amazonaws.com",
                     "domU-12-31-39-07-A5-05.compute-1.internal");
        dnsNames.put("ec2-174-129-60-214.compute-1.amazonaws.com",
                     "domU-12-31-39-06-8C-36.compute-1.internal");

        // dnsNames = createInstances();
        Map<String, Integer> nodeIds = generateClusterDescriptor(dnsNames,
                                                                 "/home/kirk/voldemortdev/voldemort/config/single_node_cluster/config/cluster.xml");

        final CommandLineClusterConfig config = new CommandLineClusterConfig();
        config.setHostNames(dnsNames.keySet());
        config.setHostUserId("root");
        config.setSshPrivateKey(new File("/home/kirk/Dropbox/Configuration/AWS/id_rsa-mustardgrain-keypair"));
        config.setVoldemortParentDirectory("somesubdirectory");
        config.setVoldemortRootDirectory("somesubdirectory/voldemort");
        config.setVoldemortHomeDirectory("somesubdirectory/voldemort/config/single_node_cluster");
        config.setNodeIds(nodeIds);
        config.setSourceDirectory(new File("/home/kirk/voldemortdev/voldemort"));

        Map<String, String> remoteTestArguments = new HashMap<String, String>();
        final String bootstrapUrl = dnsNames.values().iterator().next();
        int startKeyIndex = 0;
        final int numRequests = 10000;
        final int iterations = 100;

        for(String publicHostName: dnsNames.keySet()) {
            remoteTestArguments.put(publicHostName, "-wd --start-key-index "
                                                    + (startKeyIndex * numRequests)
                                                    + " --iterations " + iterations + " tcp://"
                                                    + bootstrapUrl + ":6666 test " + numRequests);
            startKeyIndex++;
        }

        config.setRemoteTestArguments(remoteTestArguments);

        new RsyncDeployer(config).execute();

        new Thread(new Runnable() {

            public void run() {

                try {
                    new SshClusterStarter(config).execute();
                } catch(ClusterOperationException e) {
                    e.printStackTrace();
                }

            }

        }).start();

        Thread.sleep(5000);

        List<RemoteTestResult> remoteTestResults = new SshRemoteTest(config).execute();

        for(RemoteTestResult remoteTestResult: remoteTestResults) {
            System.out.println(remoteTestResult.getHostName() + " for writes:");

            for(RemoteTestIteration remoteTestIteration: remoteTestResult.getRemoteTestIterations()) {
                System.out.println("\t" + remoteTestIteration.getWrites());
            }
        }

        new SshClusterStopper(config).execute();
    }

    private Map<String, String> createInstances() throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        String ami = System.getProperty("ec2Ami");
        String keyPairId = System.getProperty("ec2KeyPairId");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        return ec2.createInstances(ami, keyPairId, null, 2, 360000);
    }

    private Map<String, Integer> generateClusterDescriptor(Map<String, String> dnsNames, String path)
            throws Exception {
        ClusterGenerator clusterGenerator = new ClusterGenerator();
        List<ClusterNodeDescriptor> nodes = clusterGenerator.createClusterNodeDescriptors(new ArrayList<String>(dnsNames.values()),
                                                                                          3);
        String clusterXml = clusterGenerator.createClusterDescriptor("test", nodes);
        FileUtils.writeStringToFile(new File(path), clusterXml);
        Map<String, Integer> nodeIds = new HashMap<String, Integer>();

        for(ClusterNodeDescriptor node: nodes) {
            String privateDnsName = node.getHostName();

            // OK, yeah, super-inefficient...
            for(Map.Entry<String, String> entry: dnsNames.entrySet()) {
                if(entry.getValue().equals(privateDnsName)) {
                    nodeIds.put(entry.getKey(), node.getId());
                }
            }
        }

        return nodeIds;
    }

}

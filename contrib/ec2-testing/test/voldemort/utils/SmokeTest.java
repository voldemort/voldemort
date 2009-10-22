package voldemort.utils;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import voldemort.utils.ec2testing.Ec2Connection;
import voldemort.utils.ec2testing.TypicaEc2Connection;

public class SmokeTest {

    @Test
    public void test() throws Exception {
        String accessId = System.getProperty("ec2AccessId");
        String secretKey = System.getProperty("ec2SecretKey");
        String ami = System.getProperty("ec2Ami");
        String keyPairId = System.getProperty("ec2KeyPairId");
        Ec2Connection ec2 = new TypicaEc2Connection(accessId, secretKey);
        Map<String, String> dnsNames = ec2.createInstances(ami, keyPairId, null, 2, 360000);

        ClusterDescriptorGenerator cdg = new ClusterDescriptorGenerator();
        String clusterXml = cdg.createClusterDescriptor(new ArrayList<String>(dnsNames.values()), 3);

        System.out.println(clusterXml);
    }

}

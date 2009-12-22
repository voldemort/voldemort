package voldemort.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

/**
 * @author afeinberg
 */
public class Ec2RebalancingTest {
    private static Ec2RemoteTestConfig ec2RemoteTestConfig;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;
    
    private static final Logger logger = Logger.getLogger(Ec2RebalancingTest.class);

    private static class Ec2RebalancingTestConfig extends Ec2RemoteTestConfig {
        private static final int numKeys = 1000;
        private static String testStoreName = "test-replication-memory";
        private static String storeDefFile = "test/common/voldemort/config/stores.xml";
        private String configDirName;

        @Override
        protected void init(Properties properties) {
            super.init(properties);
            configDirName = properties.getProperty("ec2ConfigDirName");

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

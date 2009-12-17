package voldemort.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author afeinberg
 */
public class Ec2GossipTest {
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

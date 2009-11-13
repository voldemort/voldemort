/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortConfig;

/**
 * Admin Client test across multiple JVMs on same localhost.
 * 
 * @author bbansal
 */
public class AdminServiceMultiJVMTest extends TestCase {

    private static int TEST_VALUES_SIZE = 1000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private VoldemortConfig config0;
    private VoldemortConfig config1;
    private List<Process> servers;

    @Override
    public void setUp() throws IOException {
        Cluster cluster = ServerTestUtils.getLocalCluster(2);

        config0 = createVoldemortConfig(0, storesXmlfile, cluster);
        config1 = createVoldemortConfig(1, storesXmlfile, cluster);

        servers.add(startServerAsNewJVM(config0));
        servers.add(startServerAsNewJVM(config1));

        // AdminClient = ServerTestUtils.getAdminClient);
    }

    private Process startServerAsNewJVM(VoldemortConfig voldemortConfig) throws IOException {
        String configDir = voldemortConfig.getVoldemortHome();
        String command = "bin/voldemort-server.sh " + configDir;

        return Runtime.getRuntime().exec(command);
    }

    @Override
    public void tearDown() throws IOException {
        for(Process server: servers) {
            StopServerJVM(server);
        }
    }

    private void StopServerJVM(Process server) {
        server.destroy();
    }

    private VoldemortConfig createVoldemortConfig(int node, String storesXmlfile, Cluster cluster)
            throws IOException {
        VoldemortConfig config = ServerTestUtils.createServerConfig(node,
                                                                    TestUtils.createTempDir()
                                                                             .getAbsolutePath(),
                                                                    null,
                                                                    storesXmlfile);
        config.setEnableMetadataChecking(true);

        return config;
    }
}

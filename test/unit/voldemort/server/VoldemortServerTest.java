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

package voldemort.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.Security;
import java.util.Properties;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;


/**
 * Unit test for  VodemortServer.
 */
public class VoldemortServerTest {

    private VoldemortServer server;

    @After
    public void tearDown() {
        if(server != null) {
            server.stop();
        }
    }

    private VoldemortServer getVoldemortServer(Properties properties) throws IOException {
        properties.setProperty(VoldemortConfig.ENABLE_NODE_ID_DETECTION, Boolean.toString(true));
        VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                    -1,
                                           TestUtils.createTempDir().getAbsolutePath(),
                                           null,
                                           null,
                                           properties);
        Cluster cluster = ServerTestUtils.getLocalCluster(1);
        return new VoldemortServer(config, cluster);
    }
    @Test
    public void testJCEProvider() throws IOException {
        Properties properties = new Properties();

        // Default configuration. Bouncy castle provider will not be used.
        server = getVoldemortServer(properties);
        assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
    }

    @Test
    public void testBouncyCastleProvider() throws IOException {
        Properties properties = new Properties();
        // Use bouncy castle as first choice of JCE provider.
        properties.setProperty("use.bouncycastle.for.ssl", "true");

        server = getVoldemortServer(properties);
        assertEquals(BouncyCastleProvider.PROVIDER_NAME, Security.getProviders()[0].getName());
    }
}

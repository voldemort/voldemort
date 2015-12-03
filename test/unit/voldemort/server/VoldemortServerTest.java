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

import java.security.Security;
import java.util.Properties;
import junit.framework.TestCase;
import org.bouncycastle.jce.provider.BouncyCastleProvider;


/**
 * Unit test for  VodemortServer.
 */
public class VoldemortServerTest extends TestCase {
    public void testJCEProvider() {
        Properties properties = new Properties();
        properties.setProperty("node.id", "1");
        properties.setProperty("voldemort.home", "/test");

        // Default configuration. Bouncy castle provider will not be used.
        VoldemortConfig config = new VoldemortConfig(properties);
        try {
            VoldemortServer server = new VoldemortServer(config, null);
        } catch (Throwable e) {
            //ignore
        }
        assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));

        // Use bouncy castle as first choice of JCE provider.
        properties.setProperty("use.bouncycastle.for.ssl", "true");
        config = new VoldemortConfig(properties);
        try {
            VoldemortServer server = new VoldemortServer(config, null);
        } catch (Throwable e) {
            //ignore
        }
        assertEquals(BouncyCastleProvider.PROVIDER_NAME, Security.getProviders()[0].getName());
    }
}

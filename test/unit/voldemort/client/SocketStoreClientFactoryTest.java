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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.serialization.SerializerFactory;
import voldemort.server.AbstractSocketService;

/**
 * 
 */

@RunWith(Parameterized.class)
public class SocketStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private AbstractSocketService socketService;

    private final boolean useNio;
    private final boolean useLazy;

    public SocketStoreClientFactoryTest(boolean useNio, boolean useLazy) {
        this.useNio = useNio;
        this.useLazy = useLazy;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, true }, { true, false }, { false, true },
                { false, false } });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        socketService = ServerTestUtils.getSocketService(useNio,
                                                         getClusterXml(),
                                                         getStoreDefXml(),
                                                         getValidStoreName(),
                                                         getLocalNode().getSocketPort());
        socketService.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        socketService.stop();
    }

    @Override
    protected StoreClientFactory getFactory(String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(useLazy));
    }

    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory,
                                                          String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(useLazy)
                                                              .setSerializerFactory(factory));
    }

    @Override
    protected String getValidBootstrapUrl() throws URISyntaxException {
        return getLocalNode().getSocketUrl().toString();
    }

    @Override
    protected String getValidScheme() {
        return SocketStoreClientFactory.URL_SCHEME;
    }

    @Test
    public void testTwoFactories() throws Exception {
        /* Test that two factories can be hosted on the same jvm */
        List<StoreClientFactory> factories = new ArrayList<StoreClientFactory>();
        factories.add(getFactory(getValidBootstrapUrl()));
        factories.add(getFactory(getValidBootstrapUrl()));
    }

    @Test
    public void testStoreClientCaching() throws Exception {
        // the objects will be equal only its not a lazy client
        StoreClientFactory factory = getFactory(getValidBootstrapUrl());
        StoreClient<Object, Object> testClient1 = factory.getStoreClient("test");
        StoreClient<Object, Object> testClient2 = factory.getStoreClient("test");

        StoreClient<Object, Object> bestClient1 = factory.getStoreClient("best");
        StoreClient<Object, Object> bestClient2 = factory.getStoreClient("best");

        if(useLazy) {
            // if lazy is enabled, we need to compare the inner Store Client.
            testClient1 = ((LazyStoreClient<Object, Object>) testClient1).getStoreClient();
            testClient2 = ((LazyStoreClient<Object, Object>) testClient2).getStoreClient();
            bestClient1 = ((LazyStoreClient<Object, Object>) bestClient1).getStoreClient();
            bestClient2 = ((LazyStoreClient<Object, Object>) bestClient2).getStoreClient();
        }

        assertTrue("Client for store test should be reused", testClient1 == testClient2);
        assertTrue("Client for store best should be reused", bestClient1 == bestClient2);
        assertTrue("Clients cannot be the same for different stores", testClient1 != bestClient1);
    }

    @Test
    @Override
    public void testBootstrapServerDown() throws Exception {
        try {
            getFactory(getValidScheme() + "://localhost:58558").getStoreClient(getValidStoreName())
                                                               .get("test");
            fail("Should throw exception.");
        } catch(BootstrapFailureException e) {
            // this is good
        }
    }

    @Test
    @Override
    public void testUnknownStoreName() throws Exception {
        try {
            StoreClient<String, String> client = getFactory(getValidBootstrapUrl()).getStoreClient("12345");
            assertNotNull(client);
            if(useLazy)
                client.get("test");
            fail("Bootstrapped a bad name.");
        } catch(BootstrapFailureException e) {
            // this is good
        }
    }

    @Test
    @Override
    public void testBootstrapFailoverSucceeds() throws Exception {
        getFactory(getValidScheme() + "://localhost:58558", getValidBootstrapUrl()).getStoreClient(getValidStoreName())
                                                                                   .get("test");
    }

    protected StoreClientFactory getFactoryForZoneID(int zoneID, String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
                                                              .setEnableLazy(useLazy)
                                                              .setClientZoneId(zoneID));
    }

    @Test
    public void testInvalidZoneID() throws Exception {
        try {
            getFactoryForZoneID(345334, getValidBootstrapUrl()).getStoreClient(getValidStoreName())
                                                               .get("test");
            fail("Should throw exception.");
        } catch(VoldemortException e) {
            e.printStackTrace();
            // this is good
        }
    }
}

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
import voldemort.serialization.SerializerFactory;
import voldemort.server.AbstractSocketService;

/**
 * 
 */

@RunWith(Parameterized.class)
public class SocketStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private AbstractSocketService socketService;

    private final boolean useNio;

    public SocketStoreClientFactoryTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
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
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls));
    }

    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory,
                                                          String... bootstrapUrls) {
        return new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls)
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

}

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

import org.junit.After;
import org.junit.Before;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.SerializerFactory;
import voldemort.store.http.HttpStore;

/**
 * 
 */
public class HttpStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private HttpStore httpStore;
    private Server server;
    private Context context;
    private String url;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        context = ServerTestUtils.getJettyServer(getClusterXml(),
                                                 getStoreDefXml(),
                                                 getValidStoreName(),
                                                 RequestFormatType.VOLDEMORT_V1,
                                                 getLocalNode().getHttpPort());
        server = context.getServer();
        httpStore = ServerTestUtils.getHttpStore(getValidStoreName(),
                                                 RequestFormatType.VOLDEMORT_V1,
                                                 getLocalNode().getHttpPort());
        url = getLocalNode().getHttpUrl().toString();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        httpStore.close();
        server.stop();
        context.destroy();
    }

    @Override
    protected StoreClientFactory getFactory(String... bootstrapUrls) {
        return new HttpStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrls));
    }

    @Override
    protected StoreClientFactory getFactoryWithSerializer(SerializerFactory factory,
                                                          String... bootstrapUrls) {
        return new HttpStoreClientFactory(new ClientConfig().setSerializerFactory(factory)
                                                            .setBootstrapUrls(bootstrapUrls));
    }

    @Override
    protected String getValidBootstrapUrl() {
        return url;
    }

    @Override
    protected String getValidScheme() {
        return HttpStoreClientFactory.URL_SCHEME;
    }

}

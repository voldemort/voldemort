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

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.SerializerFactory;
import voldemort.server.AbstractSocketService;

/**
 * @author jay
 * 
 */
public class SocketStoreClientFactoryTest extends AbstractStoreClientFactoryTest {

    private AbstractSocketService socketService;

    public SocketStoreClientFactoryTest() {
        super();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        socketService = ServerTestUtils.getSocketService(getClusterXml(),
                                                         getStoreDefXml(),
                                                         getValidStoreName(),
                                                         getLocalNode().getSocketPort(),
                                                         RequestFormatType.VOLDEMORT);
        socketService.start();
    }

    @Override
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

}

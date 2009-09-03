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

package voldemort.store.http;

import org.apache.commons.httpclient.HttpClient;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * Tests of HTTP store against the HTTP server
 * 
 * @author jay
 * 
 */
public class HttpStoreTest extends AbstractByteArrayStoreTest {

    private HttpStore httpStore;
    private Server server;
    private Context context;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Cluster cluster = ServerTestUtils.getLocalCluster(1);
        Node node = cluster.getNodes().iterator().next();
        context = ServerTestUtils.getJettyServer(new ClusterMapper().writeCluster(cluster),
                                                 VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                 "users",
                                                 RequestFormatType.VOLDEMORT_V1,
                                                 node.getHttpPort());
        server = context.getServer();
        httpStore = ServerTestUtils.getHttpStore("users",
                                                 RequestFormatType.VOLDEMORT_V1,
                                                 node.getHttpPort());
    }

    public <T extends Exception> void testBadUrlOrPort(String url, int port, Class<T> expected) {
        ByteArray key = new ByteArray("test".getBytes());
        RequestFormat requestFormat = new RequestFormatFactory().getRequestFormat(RequestFormatType.VOLDEMORT_V1);
        HttpClient client = new HttpClient();
        client.setConnectionTimeout(5000);
        HttpStore badUrlHttpStore = new HttpStore("test", url, port, client, requestFormat, false);
        try {
            badUrlHttpStore.put(key, new Versioned<byte[]>("value".getBytes(), new VectorClock()));
        } catch(Exception e) {
            assertTrue(e.getClass().equals(expected));
        }
        try {
            badUrlHttpStore.get(key);
        } catch(Exception e) {
            assertTrue(e.getClass().equals(expected));
        }
        try {
            badUrlHttpStore.delete(key, new VectorClock());
        } catch(Exception e) {
            assertTrue(e.getClass().equals(expected));
        }
    }

    public void testBadUrl() {
        testBadUrlOrPort("asfgsadfsda",
                         ServerTestUtils.findFreePort(),
                         UnreachableStoreException.class);
    }

    public void testBadPort() {
        testBadUrlOrPort("localhost",
                         ServerTestUtils.findFreePort(),
                         UnreachableStoreException.class);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        httpStore.close();
        server.stop();
        context.destroy();
    }

    @Override
    public Store<ByteArray, byte[]> getStore() {
        return httpStore;
    }

}

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

package voldemort.server.socket;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;

/**
 * Tests for the socket pooling
 * 
 * @author jay
 * 
 */
@RunWith(Parameterized.class)
public class SocketPoolTest extends TestCase {

    private int port;
    private int maxConnectionsPerNode = 3;
    private SocketPool pool;
    private SocketDestination dest1;
    private AbstractSocketService server;

    private final boolean useNio;

    public SocketPoolTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() {
        this.port = ServerTestUtils.findFreePort();
        this.pool = new SocketPool(maxConnectionsPerNode, 1000, 1000, 32 * 1024);
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        RequestHandlerFactory handlerFactory = ServerTestUtils.getSocketRequestHandlerFactory(new StoreRepository());
        this.server = ServerTestUtils.getSocketService(useNio,
                                                       handlerFactory,
                                                       port,
                                                       10,
                                                       10 + 3,
                                                       10000);

        this.server.start();
    }

    @Override
    @After
    public void tearDown() {
        this.pool.close();
        this.server.stop();
    }

    @Test
    public void testTwoCheckoutsGetTheSameSocket() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 == sas2);
    }

    @Test
    public void testClosingSocketDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getSocket().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    @Test
    public void testClosingStreamDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getOutputStream().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    @Test
    public void testVariousProtocols() throws Exception {
        for(RequestFormatType type: RequestFormatType.values()) {
            SocketDestination dest = new SocketDestination("localhost", port, type);
            SocketAndStreams sas = pool.checkout(dest);
            assertEquals(type, sas.getRequestFormatType());
        }
    }

}

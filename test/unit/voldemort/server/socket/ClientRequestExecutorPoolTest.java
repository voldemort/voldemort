/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.stats.ClientSocketStats;

/**
 * Tests for the socket pooling
 * 
 * 
 */
@RunWith(Parameterized.class)
public class ClientRequestExecutorPoolTest {

    private int port;
    private int maxConnectionsPerNode = 3;
    private ClientRequestExecutorPool pool;
    private SocketDestination dest1;
    private AbstractSocketService server;

    private final boolean useNio;

    public ClientRequestExecutorPoolTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() {
        this.port = ServerTestUtils.findFreePort();
        this.pool = new ClientRequestExecutorPool(2,
                                                  maxConnectionsPerNode,
                                                  1000,
                                                  1000,
                                                  32 * 1024,
                                                  false,
                                                  true,
                                                  0);
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

    @After
    public void tearDown() {
        this.pool.close();
        this.server.stop();
    }

    @Test
    public void testTwoCheckoutsGetTheSameSocket() throws Exception {
        ClientRequestExecutor sas1 = pool.checkout(dest1);
        pool.checkin(dest1, sas1);
        ClientRequestExecutor sas2 = pool.checkout(dest1);
        assertTrue(sas1 == sas2);
    }

    @Test
    public void testClosingDeactivates() throws Exception {
        ClientRequestExecutor sas1 = pool.checkout(dest1);
        sas1.close();
        pool.checkin(dest1, sas1);
        ClientRequestExecutor sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    @Test
    public void testCloseWithInFlightSockets() throws Exception {
        List<ClientRequestExecutor> list = new ArrayList<ClientRequestExecutor>();

        for(int i = 0; i < maxConnectionsPerNode; i++)
            list.add(pool.checkout(dest1));

        assertEquals(list.size(), pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
        assertEquals(list.size(), pool.getStats().getConnectionsActive(null));

        pool.close(dest1);

        assertEquals(list.size(), pool.getStats().getConnectionsActive(null));
        assertEquals(0, pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));

        for(ClientRequestExecutor sas: list)
            pool.checkin(dest1, sas);

        assertEquals(0, pool.getStats().getConnectionsActive(null));
        assertEquals(list.size(), pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
    }

    @Test
    public void testSocketClosedWhenCheckedInAfterPoolKeyClosed() throws Exception {
        ClientRequestExecutor sas1 = pool.checkout(dest1);
        ClientRequestExecutor sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
        pool.checkin(dest1, sas1);
        pool.close(dest1);
        pool.checkin(dest1, sas2);
        pool.close(dest1);
    }

}

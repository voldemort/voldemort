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

import junit.framework.TestCase;
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
public class SocketPoolTest extends TestCase {

    private int port;
    private int maxConnectionsPerNode = 3;
    private int maxTotalConnections = 2 * maxConnectionsPerNode + 1;
    private SocketPool pool;
    private SocketDestination dest1;
    private AbstractSocketService socketService;

    @Override
    public void setUp() {
        this.port = ServerTestUtils.findFreePort();
        this.pool = new SocketPool(maxConnectionsPerNode,
                                   maxTotalConnections,
                                   1000,
                                   1000,
                                   32 * 1024);
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        RequestHandlerFactory handlerFactory = new RequestHandlerFactory(new StoreRepository(),
                                                                         null,
                                                                         ServerTestUtils.getVoldemortConfig());
        this.socketService = ServerTestUtils.getSocketService(handlerFactory,
                                                              port,
                                                              maxTotalConnections,
                                                              maxTotalConnections + 3,
                                                              10000);
        this.socketService.start();
    }

    @Override
    public void tearDown() {
        this.pool.close();
        this.socketService.stop();
    }

    public void testTwoCheckoutsGetTheSameSocket() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 == sas2);
    }

    public void testClosingSocketDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getSocket().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    public void testClosingStreamDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest1);
        sas1.getOutputStream().close();
        pool.checkin(dest1, sas1);
        SocketAndStreams sas2 = pool.checkout(dest1);
        assertTrue(sas1 != sas2);
    }

    public void testVariousProtocols() throws Exception {
        for(RequestFormatType type: RequestFormatType.values()) {
            SocketDestination dest = new SocketDestination("localhost", port, type);
            SocketAndStreams sas = pool.checkout(dest);
            assertEquals(type, sas.getRequestFormatType());
        }
    }

}

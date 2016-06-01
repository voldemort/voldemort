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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.niosocket.NonRespondingSocketService;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.socket.clientrequest.GetClientRequest;
import voldemort.store.stats.ClientSocketStats;
import voldemort.utils.ByteArray;

/**
 * Tests for the socket pooling
 *
 *
 */
@RunWith(Parameterized.class)
public class ClientRequestExecutorPoolTest {

    private int port;
    private int nonRespondingPort;
    private int maxConnectionsPerNode = 3;
    private ClientRequestExecutorPool pool;
    private SocketDestination dest1;
    private SocketDestination nonRespondingDest;
    private AbstractSocketService server;
    private NonRespondingSocketService nonRespondingServer;
    private static final int SOCKET_TIMEOUT_MS = 1000;
    private static final int CONNECTION_TIMEOUT_MS = 1500;
    private static final long IDLE_CONNECTION_TIMEOUT_MS = -1;

    private final boolean useNio;

    public ClientRequestExecutorPoolTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        this.port = ServerTestUtils.findFreePort();
        this.pool = new ClientRequestExecutorPool(2,
                                                  maxConnectionsPerNode,
                                                  CONNECTION_TIMEOUT_MS,
                                                  SOCKET_TIMEOUT_MS,
                                                  IDLE_CONNECTION_TIMEOUT_MS,
                                                  32 * 1024,
                                                  false,
                                                  true,
                                                  new String());
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        startServer();

        this.nonRespondingPort = ServerTestUtils.findFreePort();
        this.nonRespondingDest = new SocketDestination("localhost",
                                                       nonRespondingPort,
                                                       RequestFormatType.VOLDEMORT_V1);

        this.nonRespondingServer = new NonRespondingSocketService(nonRespondingPort);
        this.nonRespondingServer.start();

    }

    @After
    public void tearDown() throws IOException {
        this.pool.close();
        this.server.stop();
        this.nonRespondingServer.stop();
    }

    private void stopServer() {
        this.server.stop();
    }

    private void startServer() {
        RequestHandlerFactory handlerFactory = ServerTestUtils.getSocketRequestHandlerFactory(new StoreRepository());
        this.server = ServerTestUtils.getSocketService(useNio,
                                                       handlerFactory,
                                                       port,
                                                       10,
                                                       10 + 3,
                                                       10000);
        this.server.start();
    }

    private void validateResourceCount(ClientRequestExecutorPool pool, String message, int expected) {
        int numConnections = pool.internalGetQueuedPool().getCheckedInResourcesCount(dest1);
        assertEquals(message, expected, numConnections);
        int totalResourceCount = pool.internalGetQueuedPool().getTotalResourceCount(dest1);
        assertEquals(message, expected, totalResourceCount);
    }

    @Test
    public void testInFlightServerBounce() throws Exception {
        // Create 2 resources
        ClientRequestExecutor sas1 = pool.checkout(dest1);
        ClientRequestExecutor sas2 = pool.checkout(dest1);

        // Stop the Server
        stopServer();

        // It takes few milliseconds for the selector to wake-up and clear
        // connections
        Thread.sleep(5);

        pool.checkin(dest1, sas1);
        pool.checkin(dest1, sas2);

        validateResourceCount(pool, "Dead connections should have been cleared on checkin", 0);
    }

    @Test
    public void testAtRestServerBounce() throws Exception {
        // Create 2 resources
        ClientRequestExecutor sas1 = pool.checkout(dest1);
        ClientRequestExecutor sas2 = pool.checkout(dest1);
        pool.checkin(dest1, sas1);
        pool.checkin(dest1, sas2);

        validateResourceCount(pool, "two connections are created ", 2);

        stopServer();

        validateResourceCount(pool, "cache should have 2 dead connections ", 2);

        // It takes few milliseconds for the selector to wake-up and clear
        // connections
        Thread.sleep(5);
        testConnectionFailure(pool, dest1, ConnectException.class);

        validateResourceCount(pool, "next checkout should have cleared all dead connections", 0);

        startServer();

        sas1 = pool.checkout(dest1);
        sas2 = pool.checkout(dest1);

        pool.checkin(dest1, sas1);
        pool.checkin(dest1, sas2);

        validateResourceCount(pool, "Back to normal 2 connections expected ", 2);
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

        assertEquals(list.size(),
                     pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
        assertEquals(list.size(), pool.getStats().getConnectionsActive(null));

        pool.close(dest1);

        assertEquals(list.size(), pool.getStats().getConnectionsActive(null));
        assertEquals(0,
                     pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));

        for(ClientRequestExecutor sas: list)
            pool.checkin(dest1, sas);

        assertEquals(0, pool.getStats().getConnectionsActive(null));
        assertEquals(list.size(),
                     pool.getStats().getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
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

    private void testCheckoutConnectionFailure(ClientRequestExecutorPool execPool,
                                               SocketDestination dest,
                                               Class<?> expectedExceptionClass) {
        try {
            execPool.checkout(dest);
            fail("should have thrown an connection exception");
        } catch(UnreachableStoreException e) {
            assertEquals("inner exception should be of type connect exception",
                         expectedExceptionClass,
                         e.getCause().getClass());
        }
    }

    private void testNonBlockingCheckoutConnectionFailure(ClientRequestExecutorPool execPool,
                                                          SocketDestination dest,
                                                          Class<?> expectedExceptionClass)
            throws Exception {

        try {
            ClientRequestExecutor resource = execPool.internalGetQueuedPool()
                                                     .internalNonBlockingGet(dest);
            // First time you call non blocking get, it triggers an async
            // operation and returns null most likely.
            if(resource == null) {
                Thread.sleep(execPool.getFactory().getTimeout() + 5);
                execPool.internalGetQueuedPool().internalNonBlockingGet(dest);
            }
            fail("should have thrown an connection exception");
        } catch(UnreachableStoreException e) {
            assertEquals("inner exception should be of type connect exception",
                         expectedExceptionClass,
                         e.getCause().getClass());
        }

    }

    private void testConnectionFailure(ClientRequestExecutorPool execPool,
                                       SocketDestination dest,
                                       Class<?> expectedExceptionClass)
            throws Exception {
        testCheckoutConnectionFailure(execPool, dest, expectedExceptionClass);
        testNonBlockingCheckoutConnectionFailure(execPool, dest, expectedExceptionClass);
    }

    @Test
    public void testNonExistentHost() throws Exception {
        SocketDestination nonExistentHost = new SocketDestination("unknown.invalid",
                                                                  port,
                                                                  RequestFormatType.VOLDEMORT_V1);
        testConnectionFailure(pool, nonExistentHost, UnresolvedAddressException.class);
    }

    @Test
    public void testUnresponsiveServerThrows() throws Exception {
        testConnectionFailure(pool, nonRespondingDest, ConnectException.class);
    }

    @Test
    public void testMachinUpProcessDownThrows() throws Exception {
        int processDownPort = ServerTestUtils.findFreePort();
        SocketDestination processDownDest = new SocketDestination("localhost",
                                                                  processDownPort,
                                                       RequestFormatType.VOLDEMORT_V1);

        testConnectionFailure(pool, processDownDest, ConnectException.class);

    }

    @Test
    public void testConnectionTimeoutThrows() throws Exception {


        ClientRequestExecutorPool timeoutPool = new ClientRequestExecutorPool(2,
                                                                          maxConnectionsPerNode,
                                                                          50,   //Connection timeout
                                                                          0,    // Socket timeout, 0 milliseconds :)
                                                                          IDLE_CONNECTION_TIMEOUT_MS,
                                                                          32 * 1024,
                                                                          false,
                                                                              true,
                                                                          new String());
        testConnectionFailure(timeoutPool, dest1, ConnectException.class);
    }

    @Test
    public void testIdleConnectionTimeout() throws Exception {
        long idleConnectionTimeoutMs = 1000;
        ClientRequestExecutorPool execPool = new ClientRequestExecutorPool(2,
                                                                           maxConnectionsPerNode,
                                                                           CONNECTION_TIMEOUT_MS,
                                                                           SOCKET_TIMEOUT_MS,
                                                                           idleConnectionTimeoutMs,
                                                                           32 * 1024,
                                                                           false,
                                                                           true,
                                                                           new String());

        List<ClientRequestExecutor> executors = new ArrayList<ClientRequestExecutor>();
        for (int i = 0; i < maxConnectionsPerNode; i++) {
            executors.add(execPool.checkout(dest1));
        }

        for (ClientRequestExecutor executor : executors) {
            execPool.checkin(dest1, executor);
        }

        validateResourceCount(execPool, " checkout should have created " + maxConnectionsPerNode, maxConnectionsPerNode);
        // Selector only wakes up every few seconds
        Thread.sleep(idleConnectionTimeoutMs + 1000);

        // All existing connections are marked as invalid by selector
        // This call will create new connection.
        ClientRequestExecutor exec1 = execPool.checkout(dest1);

        for (ClientRequestExecutor executor : executors) {
            assertNotSame("Connections should have been destroyed and new one expected", exec1, executor);
        }
        execPool.checkin(dest1, exec1);
        validateResourceCount(execPool, " all idle connections should have been destroyed ", 1);
    }

    @Test
    public void testIsValidConnectionIdleTimeout() throws Exception {
        long idleConnectionTimeoutMs = 200;
        ClientRequestExecutorPool execPool = new ClientRequestExecutorPool(2,
                                                                           maxConnectionsPerNode,
                                                                           CONNECTION_TIMEOUT_MS,
                                                                           SOCKET_TIMEOUT_MS,
                                                                           idleConnectionTimeoutMs,
                                                                           32 * 1024,
                                                                           false,
                                                                           true,
                                                                           new String());

        ClientRequestExecutor clientRequest = execPool.checkout(dest1);

        assertTrue("Connection checked out is valid", execPool.getFactory().validate(dest1, clientRequest));

        Thread.sleep(idleConnectionTimeoutMs);

        assertFalse("Idle connection will expire", execPool.getFactory().validate(dest1, clientRequest));

    }

    @Test
    public void testInUseConnectionIsNeverIdle() throws Exception {

    }

    @Test
    public void testCloseWithOutstandingQueue() {
        final int MAX_CONNECTIONS = 2;
        ClientRequestExecutorPool execPool = new ClientRequestExecutorPool(2,
                                                                              MAX_CONNECTIONS,
                                                                              CONNECTION_TIMEOUT_MS,
                                                                              SOCKET_TIMEOUT_MS,
                                                                              IDLE_CONNECTION_TIMEOUT_MS,
                                                                              32 * 1024,
                                                                              false,
                                                                              true,
                                                                              new String());

        // Once connections are checked out, calling close don't destroy them.
        // They are destroyed, when they are checked in to the pool.
        // In practice, all checked out connections are returned, so this is not
        // a problem.
        // Exhaust the connection pool here, so that all the subsequent requests
        // will always wait in the queue.
        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            execPool.checkout(dest1);
        }

        for(int j = 0; j < 2; j++) {

            GetClientRequest clientRequest = new GetClientRequest("sampleStore",
                                                                  new RequestFormatFactory().getRequestFormat(dest1.getRequestFormatType()),
                                                                  RequestRoutingType.ROUTED,
                                                                  new ByteArray(new byte[] { 1, 2,
                                                                          3 }),
                                                                  null);

            final AtomicInteger cancelledEvents = new AtomicInteger(0);

            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                @Override
                public void requestComplete(Object result, long requestTime) {
                    if(result instanceof UnreachableStoreException)
                        cancelledEvents.incrementAndGet();
                    else
                        fail("The request must have failed with UnreachableException" + result);
                }
            };

            int queuedRequestCount = 20;
            for(int i = 0; i < queuedRequestCount; i++) {
                execPool.submitAsync(dest1, clientRequest, callback, 5000, "get");
            }

            int outstandingQueue = execPool.internalGetQueuedPool()
                                           .getRegisteredResourceRequestCount(dest1);
            assertEquals("Queued request count should match", queuedRequestCount, outstandingQueue);

            // Now reset the queue, the outstanding requests should fail with
            // UnreachableStoreException
            execPool.close(dest1);

            outstandingQueue = execPool.internalGetQueuedPool()
                                       .getRegisteredResourceRequestCount(dest1);

            assertEquals("Queued request should have been cleared", 0, outstandingQueue);

            // CancelledEvents should be
            assertEquals("All Queuedrequest must have been cancelled.",
                         queuedRequestCount,
                         cancelledEvents.get());
        }

    }

    @Test
    public void testRememberedExceptions() {

        ConnectException connectEx = new ConnectException("Connect exception");
        UnreachableStoreException unreachableEx = new UnreachableStoreException("test Exception", connectEx);
        final int COUNT = 10;
        for(int i = 0; i < COUNT; i++) {
            this.pool.internalGetQueuedPool().reportException(dest1, unreachableEx);
        }
        
        for(int i = 0; i < COUNT; i ++) {
            try {
                this.pool.internalGetQueuedPool().checkout(dest1);
                fail("should have thrown an exception");
            } catch(Exception ex) {
                assertEquals("Expected Unreachable Store Exception",
                             unreachableEx.getClass(),
                             ex.getClass());
                assertEquals("Expected Unreachable Store Exception",
                             unreachableEx.getMessage(),
                             ex.getMessage());
                assertEquals("InnerException is connect Exception",
                             connectEx.getClass(),
                             ex.getCause().getClass());
                assertEquals("InnerException is connect Exception",
                             connectEx.getMessage(),
                             ex.getCause().getMessage());
            }
        }

        // should not fail
        this.pool.checkout(dest1);
    }

    @Test
    public void testRememberedExceptionsBeyondTime() throws Exception {

        final int CURRENT_CONNECTION_TIMEOUT = 50;
        ClientRequestExecutorPool timeoutPool = new ClientRequestExecutorPool(2,
                                                                              maxConnectionsPerNode,
                                                                              CURRENT_CONNECTION_TIMEOUT,
                                                                              CURRENT_CONNECTION_TIMEOUT,
                                                                              IDLE_CONNECTION_TIMEOUT_MS,
                                                                              32 * 1024,
                                                                              false,
                                                                              true,
                                                                              new String());

        ConnectException connectEx = new ConnectException("Connect exception");
        UnreachableStoreException unreachableEx = new UnreachableStoreException("test Exception",
                                                                                connectEx);
        final int COUNT = 10;
        for(int i = 0; i < COUNT; i++) {
            timeoutPool.internalGetQueuedPool().reportException(dest1, unreachableEx);
        }

        Thread.sleep(CURRENT_CONNECTION_TIMEOUT);

        // Get all exceptions but 1.
        for(int i = 0; i < COUNT - 1; i++) {
            try {
                timeoutPool.internalGetQueuedPool().checkout(dest1);
                fail("should have thrown an exception");
            } catch(Exception ex) {

            }
        }
        Thread.sleep(CURRENT_CONNECTION_TIMEOUT + 1);

        // should not fail
        timeoutPool.checkout(dest1);

    }

}

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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;

/**
 * @author jay
 * 
 */
public class SocketPoolTest extends TestCase {

    private int port = 8567;
    private int maxConnections = 3;
    private SocketPool pool;
    private SocketDestination dest;
    private SocketServer server;

    public void setUp() {
        this.pool = new SocketPool(maxConnections, maxConnections, 2000);
        this.dest = new SocketDestination("localhost", port);
        this.server = new SocketServer(new ConcurrentHashMap(), port, 10, 10);
        this.server.start();
        this.server.awaitStartupCompletion();
    }

    public void tearDown() {
        this.pool.close();
        this.server.shutdown();
    }

    public void testTwoCheckoutsGetTheSameSocket() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest);
        pool.checkin(dest, sas1);
        SocketAndStreams sas2 = pool.checkout(dest);
        assertTrue(sas1 == sas2);
    }

    public void testClosingSocketDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest);
        sas1.getSocket().close();
        pool.checkin(dest, sas1);
        SocketAndStreams sas2 = pool.checkout(dest);
        assertTrue(sas1 != sas2);
    }

    public void testClosingStreamDeactivates() throws Exception {
        SocketAndStreams sas1 = pool.checkout(dest);
        sas1.getOutputStream().close();
        pool.checkin(dest, sas1);
        SocketAndStreams sas2 = pool.checkout(dest);
        assertTrue(sas1 != sas2);
    }

    public void testNoChurn() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(10);
        int numRequests = 1000;
        final CountDownLatch latch = new CountDownLatch(numRequests);
        for(int i = 0; i < numRequests; i++) {
            service.execute(new Runnable() {

                public void run() {
                    SocketAndStreams sas = pool.checkout(dest);
                    pool.checkin(dest, sas);
                    latch.countDown();
                }
            });
        }
        latch.await();
        assertEquals(maxConnections, pool.getNumberSocketsCreated());
    }
}

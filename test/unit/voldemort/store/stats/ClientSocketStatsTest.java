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

package voldemort.store.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.RequestFormatType;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.utils.Time;
import voldemort.utils.pool.QueuedKeyedResourcePool;

public class ClientSocketStatsTest {

    private ClientSocketStats masterStats;
    private int port;
    private SocketDestination dest1;
    private SocketDestination dest2;
    private QueuedKeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;

    @Before
    public void setUp() throws Exception {
        this.port = ServerTestUtils.findFreePort();
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        this.dest2 = new SocketDestination("localhost", port + 1, RequestFormatType.VOLDEMORT_V1);
        this.masterStats = new ClientSocketStats(0);
        pool = null;
    }

    @Test
    public void testNewNodeStatsObject() {
        ClientSocketStats stats = new ClientSocketStats(masterStats, dest1, pool, 0);
        assertNotNull(stats);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNewAggrNodeStatsObject() {
        ClientSocketStats stats = masterStats;
        assertNotNull(stats);
        assertEquals(0, stats.getConnectionsCreated());
        assertEquals(0, stats.getConnectionsDestroyed());
        assertEquals(0, stats.getCheckoutCount());
        assertEquals(0, (int) stats.getAvgCheckoutWaitMs() * Time.US_PER_MS);
    }

    @Test
    public void testConnectionCreate() {
        ClientSocketStats stats = masterStats;
        stats.connectionCreate(dest1);
        stats.connectionCreate(dest2);
        stats.connectionCreate(dest1);
        assertEquals(3, stats.getConnectionsCreated());
        assertEquals(2, stats.getStatsMap().get(dest1).getConnectionsCreated());
        assertEquals(1, stats.getStatsMap().get(dest2).getConnectionsCreated());
    }

    @Test
    public void testConnectionDestroy() {
        ClientSocketStats stats = masterStats;
        stats.connectionDestroy(dest1);
        stats.connectionDestroy(dest2);
        stats.connectionDestroy(dest1);
        assertEquals(3, stats.getConnectionsDestroyed());
        assertEquals(2, stats.getStatsMap().get(dest1).getConnectionsDestroyed());
        assertEquals(1, stats.getStatsMap().get(dest2).getConnectionsDestroyed());
    }

    @Test
    public void testRecordCheckoutTimeOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutCount());

        stats.recordCheckoutTimeUs(dest1, 100);
        // check parent
        assertEquals(1, stats.getCheckoutCount());
        assertEquals(100, (int) (stats.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(1, child.getCheckoutCount());
        assertEquals(100, (int) (child.getCheckoutTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordCheckoutTimeMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutCount());

        stats.recordCheckoutTimeUs(dest1, 100);
        stats.recordCheckoutTimeUs(dest1, 200);
        stats.recordCheckoutTimeUs(dest1, 300);
        stats.recordCheckoutTimeUs(dest1, 400);
        stats.recordCheckoutTimeUs(dest2, 500);
        stats.recordCheckoutTimeUs(dest2, 600);
        stats.recordCheckoutTimeUs(dest1, 700);
        stats.recordCheckoutTimeUs(dest1, 800);
        stats.recordCheckoutTimeUs(dest2, 900);

        // check parent
        assertEquals(9, stats.getCheckoutCount());
        assertEquals(900, (int) (stats.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(6, child1.getCheckoutCount());
        assertEquals(100, (int) (child1.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEquals(300, (int) (child1.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEquals(800, (int) (child1.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(3, child2.getCheckoutCount());
        assertEquals(500, (int) (child2.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEquals(600, (int) (child2.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEquals(900, (int) (child2.getCheckoutTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordResourceRequestTimeOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.resourceRequestCount());

        stats.recordResourceRequestTimeUs(dest1, 100);
        // check parent
        assertEquals(1, stats.resourceRequestCount());
        assertEquals(100, (int) (stats.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(1, child.resourceRequestCount());
        assertEquals(100, (int) (child.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordResourceRequestTimeMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.resourceRequestCount());

        stats.recordResourceRequestTimeUs(dest1, 100);
        stats.recordResourceRequestTimeUs(dest1, 200);
        stats.recordResourceRequestTimeUs(dest1, 300);
        stats.recordResourceRequestTimeUs(dest1, 400);
        stats.recordResourceRequestTimeUs(dest2, 500);
        stats.recordResourceRequestTimeUs(dest2, 600);
        stats.recordResourceRequestTimeUs(dest1, 700);
        stats.recordResourceRequestTimeUs(dest1, 800);
        stats.recordResourceRequestTimeUs(dest2, 900);

        // check parent
        assertEquals(9, stats.resourceRequestCount());
        assertEquals(900, (int) (stats.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));

        // check child1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(6, child1.resourceRequestCount());
        assertEquals(100, (int) (child1.getResourceRequestTimeMsQ10th() * Time.US_PER_MS));
        assertEquals(300, (int) (child1.getResourceRequestTimeMsQ50th() * Time.US_PER_MS));
        assertEquals(800, (int) (child1.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));

        // check child2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(3, child2.resourceRequestCount());
        assertEquals(500, (int) (child2.getResourceRequestTimeMsQ10th() * Time.US_PER_MS));
        assertEquals(600, (int) (child2.getResourceRequestTimeMsQ50th() * Time.US_PER_MS));
        assertEquals(900, (int) (child2.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordCheckoutQueueLengthOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutQueueLengthHistogram().getQuantile(0.99));

        stats.recordCheckoutQueueLength(dest1, 50);
        // check parent
        assertEquals(50, stats.getCheckoutQueueLengthHistogram().getQuantile(0.99));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(50, child.getCheckoutQueueLengthHistogram().getQuantile(0.99));
    }

    @Test
    public void testRecordCheckoutQueueLengthMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutQueueLengthHistogram().getQuantile(0.99));

        stats.recordCheckoutQueueLength(dest1, 50);
        stats.recordCheckoutQueueLength(dest2, 50);
        stats.recordCheckoutQueueLength(dest1, 100);
        stats.recordCheckoutQueueLength(dest2, 100);
        stats.recordCheckoutQueueLength(dest2, 100);
        stats.recordCheckoutQueueLength(dest1, 50);
        // check parent
        assertEquals(50, stats.getCheckoutQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, stats.getCheckoutQueueLengthHistogram().getQuantile(0.50));
        assertEquals(100, stats.getCheckoutQueueLengthHistogram().getQuantile(0.51));
        assertEquals(100, stats.getCheckoutQueueLengthHistogram().getQuantile(0.99));

        // check child 1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(50, child1.getCheckoutQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, child1.getCheckoutQueueLengthHistogram().getQuantile(0.66));
        assertEquals(100, child1.getCheckoutQueueLengthHistogram().getQuantile(0.67));
        assertEquals(100, child1.getCheckoutQueueLengthHistogram().getQuantile(0.99));
        // check child 2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(50, child2.getCheckoutQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, child2.getCheckoutQueueLengthHistogram().getQuantile(0.33));
        assertEquals(100, child2.getCheckoutQueueLengthHistogram().getQuantile(0.34));
        assertEquals(100, child2.getCheckoutQueueLengthHistogram().getQuantile(0.99));
    }

    @Test
    public void testRecordResourceRequestQueueLengthOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.99));

        stats.recordResourceRequestQueueLength(dest1, 50);
        // check parent
        assertEquals(50, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.99));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(50, child.getResourceRequestQueueLengthHistogram().getQuantile(0.99));
    }

    @Test
    public void testRecordResourceRequestQueueLengthMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.99));

        stats.recordResourceRequestQueueLength(dest1, 50);
        stats.recordResourceRequestQueueLength(dest2, 50);
        stats.recordResourceRequestQueueLength(dest1, 100);
        stats.recordResourceRequestQueueLength(dest2, 100);
        stats.recordResourceRequestQueueLength(dest2, 100);
        stats.recordResourceRequestQueueLength(dest1, 50);
        // check parent
        assertEquals(50, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.50));
        assertEquals(100, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.51));
        assertEquals(100, stats.getResourceRequestQueueLengthHistogram().getQuantile(0.99));

        // check child 1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(50, child1.getResourceRequestQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, child1.getResourceRequestQueueLengthHistogram().getQuantile(0.66));
        assertEquals(100, child1.getResourceRequestQueueLengthHistogram().getQuantile(0.67));
        assertEquals(100, child1.getResourceRequestQueueLengthHistogram().getQuantile(0.99));
        // check child 2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(50, child2.getResourceRequestQueueLengthHistogram().getQuantile(0.01));
        assertEquals(50, child2.getResourceRequestQueueLengthHistogram().getQuantile(0.33));
        assertEquals(100, child2.getResourceRequestQueueLengthHistogram().getQuantile(0.34));
        assertEquals(100, child2.getResourceRequestQueueLengthHistogram().getQuantile(0.99));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSetMonitoringInterval() {
        ClientSocketStats stats = masterStats;
        stats.setMonitoringInterval(9);
        stats.recordCheckoutTimeUs(dest1, 100);
        stats.recordCheckoutTimeUs(dest1, 200);
        stats.recordCheckoutTimeUs(dest1, 300);
        stats.recordCheckoutTimeUs(dest1, 400);
        stats.recordCheckoutTimeUs(dest1, 500);
        stats.recordCheckoutTimeUs(dest1, 600);
        stats.recordCheckoutTimeUs(dest2, 700);
        stats.recordCheckoutTimeUs(dest2, 800);
        // before interval based reset
        // check parent
        assertEquals(8, stats.getCheckoutCount());
        assertEquals(450, (int) (stats.getAvgCheckoutWaitMs() * Time.US_PER_MS));
        // check child
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertEquals(6, child1.getCheckoutCount());
        assertEquals(2, child2.getCheckoutCount());
        assertEquals(350, (int) (child1.getAvgCheckoutWaitMs() * Time.US_PER_MS));
        assertEquals(750, (int) (child2.getAvgCheckoutWaitMs() * Time.US_PER_MS));

        // after interval based reset
        stats.recordCheckoutTimeUs(dest2, 900000);
        // check parent
        assertEquals(0, (int) (stats.getAvgCheckoutWaitMs() * Time.US_PER_MS));
        assertEquals(0, stats.getCheckoutCount());
        // check child
        assertEquals(0, (int) (child1.getAvgCheckoutWaitMs() * Time.US_PER_MS));
        assertEquals(0, child1.getCheckoutCount());
        assertEquals(0, (int) (child2.getAvgCheckoutWaitMs() * Time.US_PER_MS));
        assertEquals(0, child2.getCheckoutCount());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void concurrentTest() {
        class TestThread implements Runnable {

            SocketDestination dest;

            public TestThread(SocketDestination dest) {
                this.dest = dest;
            }

            @Override
            public void run() {
                masterStats.recordCheckoutTimeUs(dest, 1000);
                masterStats.recordCheckoutTimeUs(dest, 2000);
                masterStats.recordCheckoutTimeUs(dest, 3000);
                masterStats.recordCheckoutTimeUs(dest, 4000);
                masterStats.recordCheckoutTimeUs(dest, 5000);
                masterStats.recordCheckoutTimeUs(dest, 6000);
                masterStats.recordCheckoutTimeUs(dest, 7000);
                masterStats.recordCheckoutTimeUs(dest, 8000);
                masterStats.recordCheckoutTimeUs(dest, 9000);
            }
        }
        Thread t1 = new Thread(new TestThread(dest1));
        Thread t1_1 = new Thread(new TestThread(dest1));
        Thread t1_2 = new Thread(new TestThread(dest1));
        Thread t1_3 = new Thread(new TestThread(dest1));
        Thread t2 = new Thread(new TestThread(dest2));
        Thread t2_1 = new Thread(new TestThread(dest2));
        t1.start();
        t2.start();
        t2_1.start();
        t1_1.start();
        t1_2.start();
        t1_3.start();
        try {
            t1.join();
            t2.join();
            t2_1.join();
            t1_1.join();
            t1_2.join();
            t1_3.join();
        } catch(Exception e) {}
        assertEquals((int) (masterStats.getCheckoutTimeMsQ10th() * Time.US_PER_MS), 1000);
        assertEquals((int) (masterStats.getCheckoutTimeMsQ50th() * Time.US_PER_MS), 5000);

        assertEquals((int) (masterStats.getStatsMap()
                                .get(dest1).getCheckoutTimeMsQ10th() * Time.US_PER_MS),
                                1000);
        assertEquals((int) (masterStats.getStatsMap()
                                .get(dest1).getCheckoutTimeMsQ50th() * Time.US_PER_MS),
                                 5000);
        assertEquals((int) (masterStats.getStatsMap()
                                .get(dest2).getCheckoutTimeMsQ10th() * Time.US_PER_MS),
                                 1000);
        assertEquals((int) (masterStats.getStatsMap()
                                .get(dest2).getCheckoutTimeMsQ50th() * Time.US_PER_MS),
                                 5000);
    }
}

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

    private double errorBound = 0.03;

    private void assertEqualsWithErrorBound(double expected, double actual) {
        assertEquals(expected, actual, expected * errorBound);
    }

    @Before
    public void setUp() throws Exception {
        this.port = ServerTestUtils.findFreePort();
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        this.dest2 = new SocketDestination("localhost", port + 1, RequestFormatType.VOLDEMORT_V1);
        this.masterStats = new ClientSocketStats("");
        pool = null;
    }

    @Test
    public void testNewNodeStatsObject() {
        ClientSocketStats stats = new ClientSocketStats(masterStats, dest1, pool, "");
        assertNotNull(stats);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNewAggrNodeStatsObject() {
        ClientSocketStats stats = masterStats;
        assertNotNull(stats);
        assertEquals(0, stats.getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
        assertEquals(0, stats.getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));
        assertEquals(0, stats.getCheckoutCount());
        assertEquals(0, (int) stats.getAvgCheckoutWaitMs() * Time.US_PER_MS);
    }

    @Test
    public void testConnectionCreate() {
        ClientSocketStats stats = masterStats;
        stats.incrementCount(dest1, ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT);
        stats.incrementCount(dest2, ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT);
        stats.incrementCount(dest1, ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT);
        assertEquals(3, stats.getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
        assertEquals(2,
                     stats.getStatsMap()
                          .get(dest1)
                          .getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
        assertEquals(1,
                     stats.getStatsMap()
                          .get(dest2)
                          .getCount(ClientSocketStats.Tracked.CONNECTION_CREATED_EVENT));
    }

    @Test
    public void testConnectionDestroy() {
        ClientSocketStats stats = masterStats;
        stats.incrementCount(dest1, ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT);
        stats.incrementCount(dest2, ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT);
        stats.incrementCount(dest1, ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT);
        assertEquals(3, stats.getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));
        assertEquals(2,
                     stats.getStatsMap()
                          .get(dest1)
                          .getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));
        assertEquals(1,
                     stats.getStatsMap()
                          .get(dest2)
                          .getCount(ClientSocketStats.Tracked.CONNECTION_DESTROYED_EVENT));
    }

    @Test
    public void testRecordCheckoutTimeOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutCount());

        stats.recordCheckoutTimeUs(dest1, 100);
        // check parent
        assertEquals(1, stats.getCheckoutCount());
        assertEqualsWithErrorBound(100, (int) (stats.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(1, child.getCheckoutCount());
        assertEqualsWithErrorBound(100, (int) (child.getCheckoutTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordCheckoutTimeMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getCheckoutCount());

        // FIXME: 10 values is not enough to have a statistically-significant sample size to measure percentiles off of.
        stats.recordCheckoutTimeUs(dest1, 100);
        stats.recordCheckoutTimeUs(dest1, 200);
        stats.recordCheckoutTimeUs(dest1, 300);
        stats.recordCheckoutTimeUs(dest1, 400);
        stats.recordCheckoutTimeUs(dest2, 500);
        stats.recordCheckoutTimeUs(dest2, 600);
        stats.recordCheckoutTimeUs(dest1, 700);
        stats.recordCheckoutTimeUs(dest1, 800);
        stats.recordCheckoutTimeUs(dest2, 900);
        stats.recordCheckoutTimeUs(dest1, 1000);

        // check parent
        assertEquals(10, stats.getCheckoutCount());
        assertEqualsWithErrorBound(1000, (int) (stats.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(7, child1.getCheckoutCount());
        assertEqualsWithErrorBound(100, (int) (child1.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(400, (int) (child1.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(1000, (int) (child1.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(3, child2.getCheckoutCount());
        assertEqualsWithErrorBound(500, (int) (child2.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(600, (int) (child2.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(900, (int) (child2.getCheckoutTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordResourceRequestTimeOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.resourceRequestCount());

        stats.recordResourceRequestTimeUs(dest1, 100);
        // check parent
        assertEquals(1, stats.resourceRequestCount());
        assertEqualsWithErrorBound(100, (int) (stats.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(1, child.resourceRequestCount());
        assertEqualsWithErrorBound(100, (int) (child.getResourceRequestTimeMsQ99th() * Time.US_PER_MS));
    }

    @Test
    public void testRecordResourceRequestTimeMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.resourceRequestCount());

        // FIXME: 10 values is not enough to have a statistically-significant sample size to measure percentiles off of.
        stats.recordCheckoutTimeUs(dest1, 100);
        stats.recordCheckoutTimeUs(dest1, 200);
        stats.recordCheckoutTimeUs(dest1, 300);
        stats.recordCheckoutTimeUs(dest1, 400);
        stats.recordCheckoutTimeUs(dest2, 500);
        stats.recordCheckoutTimeUs(dest2, 600);
        stats.recordCheckoutTimeUs(dest1, 700);
        stats.recordCheckoutTimeUs(dest1, 800);
        stats.recordCheckoutTimeUs(dest2, 900);
        stats.recordCheckoutTimeUs(dest1, 1000);

        // check parent
        assertEquals(10, stats.getCheckoutCount());
        assertEqualsWithErrorBound(1000, (int) (stats.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(7, child1.getCheckoutCount());
        assertEqualsWithErrorBound(100, (int) (child1.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(400, (int) (child1.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(1000, (int) (child1.getCheckoutTimeMsQ99th() * Time.US_PER_MS));

        // check child2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(3, child2.getCheckoutCount());
        assertEqualsWithErrorBound(500, (int) (child2.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(600, (int) (child2.getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(900, (int) (child2.getCheckoutTimeMsQ99th() * Time.US_PER_MS));
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
        stats.setMonitoringInterval(500);
        stats.recordCheckoutQueueLength(dest1, 1);
        stats.recordCheckoutQueueLength(dest1, 2);
        stats.recordCheckoutQueueLength(dest1, 3);
        stats.recordCheckoutQueueLength(dest1, 4);
        stats.recordCheckoutQueueLength(dest1, 5);
        stats.recordCheckoutQueueLength(dest1, 6);
        stats.recordCheckoutQueueLength(dest2, 7);
        stats.recordCheckoutQueueLength(dest2, 8);
        // before reset
        // check parent
        assertEquals(4.5, (stats.getCheckoutQueueLengthHistogram().getAverage()), 0);
        // check child
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertEquals(3.5, child1.getCheckoutQueueLengthHistogram().getAverage(), 0);
        assertEquals(7.5, child2.getCheckoutQueueLengthHistogram().getAverage(), 0);

        // Make it sleep for a second
        try {
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        // The histogram should be reset by now
        assertEquals(0, (stats.getCheckoutQueueLengthHistogram().getAverage()), 0);

        stats.recordCheckoutQueueLength(dest1, 1);
        stats.recordCheckoutQueueLength(dest1, 1);
        stats.recordCheckoutQueueLength(dest2, 4);
        // after reset
        // check parent
        assertEquals(2, (stats.getCheckoutQueueLengthHistogram().getAverage()), 0);
        // check child
        assertEquals(1, (child1.getCheckoutQueueLengthHistogram().getAverage()), 0);
        assertEquals(4, (child2.getCheckoutQueueLengthHistogram().getAverage()), 0);
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
        assertEqualsWithErrorBound(1000,
                (int) (masterStats.getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(5000,
                (int) (masterStats.getCheckoutTimeMsQ50th() * Time.US_PER_MS));

        assertEqualsWithErrorBound(1000,
                (int) (masterStats.getStatsMap().get(dest1).getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(5000,
                (int) (masterStats.getStatsMap().get(dest1).getCheckoutTimeMsQ50th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(1000,
                (int) (masterStats.getStatsMap().get(dest2).getCheckoutTimeMsQ10th() * Time.US_PER_MS));
        assertEqualsWithErrorBound(5000,
                (int) (masterStats.getStatsMap().get(dest2).getCheckoutTimeMsQ50th() * Time.US_PER_MS));
    }
}

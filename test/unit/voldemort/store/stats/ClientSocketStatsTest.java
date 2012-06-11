/*
 * Copyright 2008-2011 LinkedIn, Inc
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
import voldemort.utils.pool.KeyedResourcePool;

public class ClientSocketStatsTest {

    private ClientSocketStats masterStats;
    private int port;
    private SocketDestination dest1;
    private SocketDestination dest2;
    private KeyedResourcePool<SocketDestination, ClientRequestExecutor> pool;

    @Before
    public void setUp() throws Exception {
        this.port = ServerTestUtils.findFreePort();
        this.dest1 = new SocketDestination("localhost", port, RequestFormatType.VOLDEMORT_V1);
        this.dest2 = new SocketDestination("localhost", port + 1, RequestFormatType.VOLDEMORT_V1);
        this.masterStats = new ClientSocketStats();
        pool = null;
    }

    @Test
    public void testNewNodeStatsObject() {
        ClientSocketStats stats = new ClientSocketStats(masterStats, dest1, pool);
        assertNotNull(stats);
    }

    @Test
    public void testNewAggrNodeStatsObject() {
        ClientSocketStats stats = masterStats;
        assertNotNull(stats);
        assertEquals(0, stats.getConnectionsCreated());
        assertEquals(0, stats.getConnectionsDestroyed());
        assertEquals(0, stats.getConnectionsCheckedout());
        assertEquals(-1, stats.getAveWaitUs());
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
    public void testRecordCheckoutTimeNsOnce() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getConnectionsCheckedout());

        stats.recordCheckoutTimeUs(dest1, 100);
        // check parent
        assertEquals(1, stats.getConnectionsCheckedout());
        assertEquals(100, stats.getWaitHistogram().getQuantile(0.99));

        // check child
        ClientSocketStats child = stats.getStatsMap().get(dest1);
        assertNotNull(child);
        assertEquals(1, child.getConnectionsCheckedout());
        assertEquals(100, child.getWaitHistogram().getQuantile(0.99));
    }

    @Test
    public void testRecordCheckoutTimeNsMultiple() {
        ClientSocketStats stats = masterStats;
        assertEquals(0, stats.getConnectionsCheckedout());

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
        assertEquals(9, stats.getConnectionsCheckedout());
        assertEquals(900, stats.getWaitHistogram().getQuantile(0.99));

        // check child1
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        assertNotNull(child1);
        assertEquals(6, child1.getConnectionsCheckedout());
        assertEquals(100, child1.getWaitHistogram().getQuantile(0.1));
        assertEquals(300, child1.getWaitHistogram().getQuantile(0.5));
        assertEquals(800, child1.getWaitHistogram().getQuantile(0.99));

        // check child2
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertNotNull(child2);
        assertEquals(3, child2.getConnectionsCheckedout());
        assertEquals(500, child2.getWaitHistogram().getQuantile(0.1));
        assertEquals(600, child2.getWaitHistogram().getQuantile(0.5));
        assertEquals(900, child2.getWaitHistogram().getQuantile(0.99));
    }

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
        assertEquals(8, stats.getConnectionsCheckedout());
        assertEquals(450, stats.getAveWaitUs());
        // check child
        ClientSocketStats child1 = stats.getStatsMap().get(dest1);
        ClientSocketStats child2 = stats.getStatsMap().get(dest2);
        assertEquals(6, child1.getConnectionsCheckedout());
        assertEquals(2, child2.getConnectionsCheckedout());
        assertEquals(350, child1.getAveWaitUs());
        assertEquals(750, child2.getAveWaitUs());

        // after interval based reset
        stats.recordCheckoutTimeUs(dest2, 900000);
        // check parent
        assertEquals(-1, stats.getAveWaitUs());
        assertEquals(0, stats.getConnectionsCheckedout());
        // check child
        assertEquals(-1, child1.getAveWaitUs());
        assertEquals(0, child1.getConnectionsCheckedout());
        assertEquals(-1, child2.getAveWaitUs());
        assertEquals(0, child2.getConnectionsCheckedout());
    }
}

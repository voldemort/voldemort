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
package voldemort.store.stats;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static voldemort.store.stats.Tracked.*;
import static voldemort.utils.Time.NS_PER_MS;

public class StoreStatsJmxTest {
    @Test
    public void getThroughPutShouldBeSumOfThroughputOperations() {
        StoreStats stats = mock(StoreStats.class);

        when(stats.getThroughput(eq(Tracked.GET))    ).thenReturn(   1.0f);
        when(stats.getThroughput(eq(Tracked.GET_ALL))).thenReturn(  20.0f);
        when(stats.getThroughput(eq(Tracked.DELETE)) ).thenReturn( 300.0f);
        when(stats.getThroughput(eq(Tracked.PUT))    ).thenReturn(4000.0f);

        StoreStatsJmx storeStatsJmx = new StoreStatsJmx(stats);

        assertEquals(4321.0f, storeStatsJmx.getOperationThroughput(), 0.1f);

        for(Tracked op : EnumSet.of(Tracked.GET, Tracked.GET_ALL, Tracked.DELETE, Tracked.PUT)) {
            verify(stats, times(1)).getThroughput(op);
        }
    }

    // Verify that the various classes are wired correctly between the JMX class and the stats tracking classes
    @Test
    public void getMaxLatenciesWork() {
        // Max latency should be tracked across calls and one Op's should not affect the others.
        for(Tracked op : EnumSet.of(GET, GET_ALL, DELETE, PUT)) {
            StoreStats stats = new StoreStats();
            StoreStatsJmx jmx = new StoreStatsJmx(stats);

            stats.recordTime(op, 5 * NS_PER_MS);
            stats.recordTime(op, 15 * NS_PER_MS);
            stats.recordTime(op, 7 * NS_PER_MS);

            assertEquals(op == PUT ?     15 : 0, jmx.getMaxPutLatency());
            assertEquals(op == GET ?     15 : 0, jmx.getMaxGetLatency());
            assertEquals(op == GET_ALL ? 15 : 0, jmx.getMaxGetAllLatency());
            assertEquals(op == DELETE ?  15 : 0, jmx.getMaxDeleteLatency());
        }
    }

    // Verify that the logic for determing maximum value returned and average value return has been wired up correctly.
    @Test
    public void maxAndAvgSizeOfValuesAreCalculatedCorrectly() {
        for(Tracked op : EnumSet.of(GET, GET_ALL, PUT)) {
            StoreStats stats = new StoreStats();
            StoreStatsJmx jmx = new StoreStatsJmx(stats);

            long [] valueSizes = new long [] {100, 450, 200, 300};
            long sum = 0l;
            long max = valueSizes[0];
            final long timeNS = 1 * NS_PER_MS;

            for(long v : valueSizes) {
                if(op == GET)
                    stats.recordGetTime(timeNS, false, v);
                else if (op == GET_ALL)
                    stats.recordGetAllTime(timeNS, 1, 1, v);
                else // PUT
                    stats.recordPutTimeAndSize(timeNS, v);

                sum += v;
                max = Math.max(max, v);
            }
            double average = sum / (double)valueSizes.length;

            assertEquals(op == PUT ? max : 0,         jmx.getMaxPutSizeInBytes());
            assertEquals(op == PUT ? average : 0,     jmx.getAveragePutSizeInBytes(),  0.0);
            assertEquals(op == GET ? max : 0,         jmx.getMaxGetSizeInBytes());
            assertEquals(op == GET ? average : 0,     jmx.getAverageGetSizeInBytes(), 0.0);
            assertEquals(op == GET_ALL ? max : 0,     jmx.getMaxGetAllSizeInBytes());
            assertEquals(op == GET_ALL ? average : 0, jmx.getAverageGetAllSizeInBytes(), 0.0);
        }
    }

    @Test
    public void testGetPercentageGetEmptyResponses() {
        StoreStats stats = new StoreStats();
        StoreStatsJmx jmx = new StoreStatsJmx(stats);

        stats.recordGetTime(100, false, 1000);
        assertEquals(0, jmx.getPercentGetReturningEmptyResponse(), 0.0);
        stats.recordGetTime(200, true,  1001);
        assertEquals(0.5, jmx.getPercentGetReturningEmptyResponse(), 0.0);
        stats.recordGetTime(300, false, 1002);
        assertEquals(0.33, jmx.getPercentGetReturningEmptyResponse(), 0.05);
    }

    @Test
    public void testGetPercentageGetAllEmptyResponses() {
        StoreStats stats = new StoreStats();
        StoreStatsJmx jmx = new StoreStatsJmx(stats);

        stats.recordGetAllTime(100, 2, 2, 1000);  // requested values for two keys, got both of them
        assertEquals(0.0, jmx.getPercentGetAllReturningEmptyResponse(), 0.0);
        stats.recordGetAllTime(200, 2, 0, 1001);  // requested 2, got 0
        assertEquals(0.5, jmx.getPercentGetAllReturningEmptyResponse(), 0.0);
    }
}

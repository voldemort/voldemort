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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static voldemort.store.stats.Tracked.DELETE;
import static voldemort.store.stats.Tracked.GET;
import static voldemort.store.stats.Tracked.GET_ALL;
import static voldemort.store.stats.Tracked.GET_VERSIONS;
import static voldemort.store.stats.Tracked.PUT;
import static voldemort.utils.Time.NS_PER_MS;

import java.util.EnumSet;

import org.junit.Test;

public class StoreStatsJmxTest {

    @Test
    public void getThroughPutShouldBeSumOfThroughputOperations() {
        StoreStats stats = mock(StoreStats.class);

        when(stats.getThroughput(eq(Tracked.GET))).thenReturn(1.0f);
        when(stats.getThroughput(eq(Tracked.GET_ALL))).thenReturn(20.0f);
        when(stats.getThroughput(eq(Tracked.DELETE))).thenReturn(300.0f);
        when(stats.getThroughput(eq(Tracked.GET_VERSIONS))).thenReturn(4000.0f);
        when(stats.getThroughput(eq(Tracked.PUT))).thenReturn(50000.0f);

        StoreStatsJmx storeStatsJmx = new StoreStatsJmx(stats);

        assertEquals(54321.0f, storeStatsJmx.getOperationThroughput(), 0.1f);

        for(Tracked op: EnumSet.of(Tracked.GET,
                                   Tracked.GET_ALL,
                                   Tracked.DELETE,
                                   Tracked.GET_VERSIONS,
                                   Tracked.PUT)) {
            verify(stats, times(1)).getThroughput(op);
        }
    }

    // Verify that the various classes are wired correctly between the JMX class
    // and the stats tracking classes
    @Test
    public void getMaxLatenciesWork() {
        // Max latency should be tracked across calls and one Op's should not
        // affect the others.
        for(Tracked op: EnumSet.of(GET, GET_VERSIONS, GET_ALL, DELETE, PUT)) {
            StoreStats stats = new StoreStats("tests.getMaxLatenciesWork");
            StoreStatsJmx jmx = new StoreStatsJmx(stats);

            stats.recordTime(op, 5 * NS_PER_MS);
            stats.recordTime(op, 15 * NS_PER_MS);
            stats.recordTime(op, 7 * NS_PER_MS);

            assertEquals(op == PUT ? 15 : 0, jmx.getMaxPutLatency());
            assertEquals(op == GET ? 15 : 0, jmx.getMaxGetLatency());
            assertEquals(op == GET_VERSIONS ? 15 : 0, jmx.getMaxGetVersionsLatency());
            assertEquals(op == GET_ALL ? 15 : 0, jmx.getMaxGetAllLatency());
            assertEquals(op == DELETE ? 15 : 0, jmx.getMaxDeleteLatency());
        }
    }

    // Verify that the logic for determing maximum value returned and average
    // value return has been wired up correctly.
    @Test
    public void maxAndAvgSizeOfValuesAreCalculatedCorrectly() {
        for(Tracked op: EnumSet.of(GET, GET_ALL, PUT)) {
            StoreStats stats = new StoreStats("tests.maxAndAvgSizeOfValuesAreCalculatedCorrectly");
            StoreStatsJmx jmx = new StoreStatsJmx(stats);

            long[] valueSizes = new long[] { 100, 450, 200, 300 };
            long sum = 0l;
            long max = valueSizes[0];
            final long timeNS = 1 * NS_PER_MS;

            for(long v: valueSizes) {
                if(op == GET)
                    stats.recordGetTime(timeNS, false, v, 0);
                else if(op == GET_ALL)
                    stats.recordGetAllTime(timeNS, 1, 1, v, 0);
                else
                    // PUT
                    stats.recordPutTimeAndSize(timeNS, v, 0);

                sum += v;
                max = Math.max(max, v);
            }
            double average = sum / (double) valueSizes.length;

            assertEquals(op == PUT ? max : 0, jmx.getMaxPutSizeInBytes());
            assertEquals(op == PUT ? average : Double.NaN, jmx.getAveragePutSizeInBytes(), 0.0);
            assertEquals(op == GET ? max : 0, jmx.getMaxGetSizeInBytes());
            assertEquals(op == GET ? average : Double.NaN, jmx.getAverageGetSizeInBytes(), 0.0);
            assertEquals(op == GET_ALL ? max : 0, jmx.getMaxGetAllSizeInBytes());
            assertEquals(op == GET_ALL ? average : Double.NaN, jmx.getAverageGetAllSizeInBytes(), 0.0);
        }
    }

    // Verify that the logic for determing maximum value returned and average
    // value return has been wired up correctly.
    @Test
    public void maxAndAvgSizeOfKeysAreCalculatedCorrectly() {
        for(Tracked op: EnumSet.of(GET, GET_ALL, PUT, DELETE)) {
            StoreStats stats = new StoreStats("tests.maxAndAvgSizeOfKeysAreCalculatedCorrectly");
            StoreStatsJmx jmx = new StoreStatsJmx(stats);

            long[] keySizes = new long[] { 100, 450, 200, 300 };
            long sum = 0l;
            long max = keySizes[0];
            final long timeNS = 1 * NS_PER_MS;

            for(long k: keySizes) {
                if(op == GET)
                    stats.recordGetTime(timeNS, false, 0, k);
                else if(op == GET_ALL)
                    stats.recordGetAllTime(timeNS, 1, 1, 0, k);
                else if(op == DELETE)
                    stats.recordDeleteTime(timeNS, k);
                else
                    // PUT
                    stats.recordPutTimeAndSize(timeNS, 0, k);

                sum += k;
                max = Math.max(max, k);
            }
            double average = sum / (double) keySizes.length;

            assertEquals(op == PUT ? max : 0, jmx.getMaxPutKeySizeInBytes());
            assertEquals(op == PUT ? average : Double.NaN, jmx.getAveragePutKeySizeInBytes(), 0.0);
            assertEquals(op == GET ? max : 0, jmx.getMaxGetKeySizeInBytes());
            assertEquals(op == GET ? average : Double.NaN, jmx.getAverageGetKeySizeInBytes(), 0.0);
            assertEquals(op == GET_ALL ? max : 0, jmx.getMaxGetAllKeySizeInBytes());
            assertEquals(op == GET_ALL ? average : Double.NaN, jmx.getAverageGetAllKeySizeInBytes(), 0.0);
            assertEquals(op == DELETE ? max : 0, jmx.getMaxDeleteKeySizeInBytes());
            assertEquals(op == DELETE ? average : Double.NaN, jmx.getAverageDeleteKeySizeInBytes(), 0.0);

        }
    }

    @Test
    public void testGetPercentageGetEmptyResponses() {
        StoreStats stats = new StoreStats("tests.testGetPercentageGetEmptyResponses");
        StoreStatsJmx jmx = new StoreStatsJmx(stats);

        stats.recordGetTime(100, false, 1000, 0);
        assertEquals(0, jmx.getPercentGetReturningEmptyResponse(), 0.0);
        stats.recordGetTime(200, true, 1001, 0);
        assertEquals(0.5, jmx.getPercentGetReturningEmptyResponse(), 0.0);
        stats.recordGetTime(300, false, 1002, 0);
        assertEquals(0.33, jmx.getPercentGetReturningEmptyResponse(), 0.05);
    }

    @Test
    public void testGetPercentageGetAllEmptyResponses() {
        StoreStats stats = new StoreStats("tests.testGetPercentageGetAllEmptyResponses");
        StoreStatsJmx jmx = new StoreStatsJmx(stats);

        stats.recordGetAllTime(100, 2, 2, 1000, 0); // requested values for two
                                                    // keys, got both of them
        assertEquals(0.0, jmx.getPercentGetAllReturningEmptyResponse(), 0.0);
        stats.recordGetAllTime(200, 2, 0, 1001, 0); // requested 2, got 0
        assertEquals(0.5, jmx.getPercentGetAllReturningEmptyResponse(), 0.0);
    }

    @Test
    public void testAverageGetAllCount() {
        StoreStats stats = new StoreStats("tests.testAverageGetAllCount");
        StoreStatsJmx jmx = new StoreStatsJmx(stats);
        stats.recordGetAllTime(100, 2, 2, 1000, 0);
        assertEquals(2.0, jmx.getAverageGetAllCount(), 0.0);
        stats.recordGetAllTime(100, 4, 4, 1000, 0);
        assertEquals(3.0, jmx.getAverageGetAllCount(), 0.0);
    }
}

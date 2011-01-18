package voldemort.store.stats;

import org.junit.Test;
import voldemort.utils.Time;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static voldemort.utils.Time.NS_PER_MS;


public class StatsTest  {
    private RequestCounter getTestRequestCounter() {
        return new RequestCounter(50000000);
    }

    @Test
    public void emptyResponseCountsAccumulateCorrectly() {
        RequestCounter rc = getTestRequestCounter();
        assertEquals(0l, rc.getNumEmptyResponses());
        rc.addRequest(40, 1, 0, 0);
        assertEquals(1l, rc.getNumEmptyResponses());
        rc.addRequest(40, 0, 0, 0);
        assertEquals(1l, rc.getNumEmptyResponses());
        rc.addRequest(40, 1, 0, 0);
        assertEquals(2l, rc.getNumEmptyResponses());
    }

    @Test
    public void maxLatencyIsAccurate() {
        RequestCounter rc = getTestRequestCounter();
        assertEquals(0, rc.getMaxLatencyInMs());

        // Stats go in as ns, but come out as ms
        for(long duration : new long [] { 22, 99, 33, 0 }) {
            rc.addRequest(duration * NS_PER_MS);
        }
        assertEquals(99, rc.getMaxLatencyInMs());
        rc.addRequest(523 * NS_PER_MS, 1, 0, 0);
        assertEquals(523, rc.getMaxLatencyInMs());
    }

    @Test
    public void averageValueIsAccurate() {
        RequestCounter rc = getTestRequestCounter();
        for(long i = 0, sum = 0; i < 100000; i++) {
            sum += i;
            rc.addRequest(42, 1, i, 0);
            assertEquals(sum / ((float) i + 1), rc.getAverageSizeInBytes(), 0.05f);
        }
    }

    @Test
    public void maxValueIsAccurate() {
        RequestCounter rc = getTestRequestCounter();

        assertEquals(0, rc.getMaxSizeInBytes());
        for(long requestSize : new long [] {42l, 923423l, 334l, 99}) {
            rc.addRequest(1, 1, requestSize, 0);
        }
        assertEquals(923423l, rc.getMaxSizeInBytes());
        rc.addRequest(5, 0, 1414232l, 0);
        assertEquals(1414232l, rc.getMaxSizeInBytes());
    }

    @Test
    public void statsExpireOnTime() throws InterruptedException {
        final long startTime = 1445468640; // Oct 21, 2015
        final int delay = 1000;
        Time mockTime = mock(Time.class);

        when(mockTime.getMilliseconds()).thenReturn(startTime);

        RequestCounter rc = new RequestCounter(delay, mockTime);

        // Add some new stats and verify they were calculated correctly
        rc.addRequest(100 * NS_PER_MS, 1, 200, 1);
        rc.addRequest(50 * NS_PER_MS, 0, 1000, 2);
        assertEquals(1, rc.getNumEmptyResponses());
        assertEquals(100, rc.getMaxLatencyInMs());
        assertEquals(75d, rc.getAverageTimeInMs(), 0.0f);
        assertEquals(1000, rc.getMaxSizeInBytes());
        assertEquals(3, rc.getGetAllAggregatedCount());

        // Jump into the future after the counter should have expired
        when(mockTime.getMilliseconds()).thenReturn(startTime + delay + 1);

        // Now verify that the counter has aged out the previous values
        assertEquals(0, rc.getNumEmptyResponses());
        assertEquals(0, rc.getMaxLatencyInMs());
        assertEquals(0, rc.getAverageTimeInMs(), 0.0f);
        assertEquals(0, rc.getMaxSizeInBytes());
        assertEquals(0, rc.getGetAllAggregatedCount());
    }
}

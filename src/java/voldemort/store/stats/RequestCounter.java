package voldemort.store.stats;

import java.util.concurrent.atomic.AtomicReference;

import voldemort.utils.Time;

/**
 * A thread-safe request counter that calculates throughput for a specified
 * duration of time.
 * 
 * @author elias, gmj
 * 
 */
public class RequestCounter {

    private final AtomicReference<Pair> values;
    private final int durationMS;

    /**
     * @param durationMS specifies for how long you want to maintain this counter
     *        (in milliseconds).
     */
    public RequestCounter(int durationMS) {
        this.values = new AtomicReference<Pair>(new Pair(System.currentTimeMillis(), 0, 0));
        this.durationMS = durationMS;
    }

    public long getCount() {
        return values.get().count;
    }

    public float getThroughput() {
        Pair oldv = values.get();
        float elapsed = (System.currentTimeMillis() - oldv.startTimeMS) / 1000f;
        if(elapsed > 0f) {
            return oldv.count / elapsed;
        } else {
            return -1f;
        }
    }

    public double getAverageTimeInMs() {
        return values.get().getAverageTimeNS() / Time.NS_PER_MS;
    }

    public int getDuration() {
        return durationMS;
    }

    /*
     * Updates the stats accumulator with another operation.  We need to make sure that the request
     * is only added to a non-expired pair. If so, start a new counter pair with recent time.  We'll
     * only try to do this 3 times - if other threads keep modifying while we're doing our own work,
     * just bail.
     *
     * @param timeNS time of operation, in nanoseconds
     */
    public void addRequest(long timeNS) {

        for(int i = 0; i < 3; i++) {

            Pair oldv = values.get();

            long startTimeMS = oldv.startTimeMS;
            long count = oldv.count + 1;
            long totalTimeNS = oldv.totalTimeNS + timeNS;

            long now = System.currentTimeMillis();

            if(now - startTimeMS > durationMS) {
                startTimeMS = now;
                count = 1;
                totalTimeNS = timeNS;
            }

            if(values.compareAndSet(oldv, new Pair(startTimeMS, count, totalTimeNS))) {
                return;
            }
        }
    }

    private static class Pair {

        final long startTimeMS;
        final long count;
        final long totalTimeNS;

        public Pair(long startTimeMS, long count, long totalTimeNS) {
            this.startTimeMS = startTimeMS;
            this.count = count;
            this.totalTimeNS = totalTimeNS;
        }

        public double getAverageTimeNS() {
            return count > 0 ?  1f * totalTimeNS / count : -0f;
        }
    }    
}
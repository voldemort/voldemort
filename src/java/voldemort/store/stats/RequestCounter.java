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

    private final AtomicReference<Accumulator> values;
    private final int durationMS;

    /**
     * @param durationMS specifies for how long you want to maintain this
     *        counter (in milliseconds).
     */
    public RequestCounter(int durationMS) {
        this.values = new AtomicReference<Accumulator>(new Accumulator());
        this.durationMS = durationMS;
    }

    public long getCount() {
        return getValidAccumulator().count;
    }

    public long getTotalCount() {
        return getValidAccumulator().total;
    }

    public float getThroughput() {
        Accumulator oldv = getValidAccumulator();
        float elapsed = (System.currentTimeMillis() - oldv.startTimeMS) / Time.MS_PER_SECOND;
        if(elapsed > 0f) {
            return oldv.count / elapsed;
        } else {
            return -1f;
        }
    }

    public String getDisplayThroughput() {
        return String.format("%.2f", getThroughput());
    }

    public double getAverageTimeInMs() {
        return getValidAccumulator().getAverageTimeNS() / Time.NS_PER_MS;
    }

    public String getDisplayAverageTimeInMs() {
        return String.format("%.4f", getAverageTimeInMs());
    }

    public int getDuration() {
        return durationMS;
    }

    private Accumulator getValidAccumulator() {
        Accumulator accum = values.get();
        long now = System.currentTimeMillis();
        if(now - accum.startTimeMS > durationMS) {
            Accumulator newWithTotal = accum.newWithTotal();
            while(true) {
                if(values.compareAndSet(accum, newWithTotal)) {
                    return newWithTotal;
                }
            }
        } else {
            return accum;
        }
    }

    /*
     * Updates the stats accumulator with another operation. We need to make
     * sure that the request is only added to a non-expired pair. If so, start a
     * new counter pair with recent time. We'll only try to do this 3 times - if
     * other threads keep modifying while we're doing our own work, just bail.
     * 
     * @param timeNS time of operation, in nanoseconds
     */
    public void addRequest(long timeNS) {

        for(int i = 0; i < 3; i++) {
            Accumulator oldv = getValidAccumulator();

            long startTimeMS = oldv.startTimeMS;
            long count = oldv.count + 1;
            long totalTimeNS = oldv.totalTimeNS + timeNS;
            long total = oldv.total + 1;

            if(values.compareAndSet(oldv, new Accumulator(startTimeMS, count, totalTimeNS, total))) {
                return;
            }
        }
    }

    private static class Accumulator {

        final long startTimeMS;
        final long count;
        final long totalTimeNS;
        final long total;

        public Accumulator() {
            this(System.currentTimeMillis(), 0, 0, 0);
        }

        public Accumulator newWithTotal() {
            return new Accumulator(System.currentTimeMillis(), 0, 0, total);
        }

        public Accumulator(long startTimeMS, long count, long totalTimeNS, long total) {
            this.startTimeMS = startTimeMS;
            this.count = count;
            this.totalTimeNS = totalTimeNS;
            this.total = total;
        }

        public double getAverageTimeNS() {
            return count > 0 ? 1f * totalTimeNS / count : -0f;
        }
    }
}
package voldemort.store.stats;

import java.util.concurrent.atomic.AtomicReference;

import voldemort.utils.Time;

/**
 * A thread-safe request counter that calculates throughput for a specified
 * duration of time.
 * 
 * @author elias
 * 
 */
public class RequestCounter {

    private static class Pair {

        final long startTime;
        final long count;
        final double averageTime;

        public Pair(long startTime, long count, double averageTime) {
            this.startTime = startTime;
            this.count = count;
            this.averageTime = averageTime;
        }
    }

    private final AtomicReference<Pair> values;
    private final int duration;

    /**
     * @param duration specifies for how long you want to maintain this counter
     *        (in milliseconds).
     */
    public RequestCounter(int duration) {
        this.values = new AtomicReference<Pair>(new Pair(System.currentTimeMillis(), 0, 0));
        this.duration = duration;
    }

    public long getCount() {
        return values.get().count;
    }

    public float getThroughput() {
        Pair oldv = values.get();
        float elapsed = (System.currentTimeMillis() - oldv.startTime) / 1000f;
        if(elapsed > 0f) {
            return oldv.count / elapsed;
        } else {
            return -1f;
        }
    }

    public double getAverageTimeInMs() {
        return values.get().averageTime / Time.NS_PER_MS;
    }

    /*
     * We need to make sure that the request is only added to a non-expired
     * pair. If so, start a new counter pair with recent time.
     */
    public void addRequest(double time) {
        for(int i = 0; i < 3; i++) {
            Pair oldv = values.get();
            long startTime = oldv.startTime;
            long count = oldv.count + 1;
            long now = System.currentTimeMillis();
            double averageTime = oldv.averageTime;
            averageTime += (time - averageTime) / count;
            if(now - startTime > duration) {
                startTime = now;
                count = 1;
                averageTime = time;
            }
            Pair newv = new Pair(startTime, count, averageTime);
            if(values.compareAndSet(oldv, newv))
                return;
        }
    }
}
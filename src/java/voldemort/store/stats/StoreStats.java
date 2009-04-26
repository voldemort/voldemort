package voldemort.store.stats;

import voldemort.utils.Time;

/**
 * Some convenient statistics to track about the store
 * 
 * @author jay
 * 
 */
public class StoreStats {

    private int[] calls;
    private double[] times;

    public StoreStats() {
        this.calls = new int[Tracked.values().length];
        this.times = new double[Tracked.values().length];
    }

    public int getCount(Tracked op) {
        return calls[op.ordinal()];
    }

    public double getAvgTimeInMs(Tracked op) {
        return times[op.ordinal()] / Time.NS_PER_MS;
    }

    public void recordTime(Tracked op, double time) {
        calls[op.ordinal()]++;
        times[op.ordinal()] += (time - times[op.ordinal()]) / calls[op.ordinal()];
    }

    public void reset() {
        for(int i = 0; i < Tracked.values().length; i++) {
            this.calls[i] = 0;
            this.times[i] = 0.0;
        }
    }

}

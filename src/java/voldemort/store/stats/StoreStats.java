package voldemort.store.stats;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Some convenient statistics to track about the store
 * 
 * @author jay
 * 
 */
public class StoreStats {

    private final StoreStats parent;
    private final Map<Tracked, RequestCounter> counters;

    public StoreStats() {
        this(null);
    }

    /**
     * @param parent An optional parent stats object that will maintain
     *        aggregate data across many stores
     */
    public StoreStats(StoreStats parent) {
        counters = new EnumMap<Tracked, RequestCounter>(Tracked.class);

        for(Tracked tracked: Tracked.values()) {
            counters.put(tracked, new RequestCounter(300000));
        }
        this.parent = parent;
    }

    public long getCount(Tracked op) {
        return counters.get(op).getCount();
    }

    public float getThroughput(Tracked op) {
        return counters.get(op).getThroughput();
    }

    public double getAvgTimeInMs(Tracked op) {
        return counters.get(op).getAverageTimeInMs();
    }

    public void recordTime(Tracked op, long timeNS) {
        counters.get(op).addRequest(timeNS);
        if(parent != null)
            parent.recordTime(op, timeNS);
    }

    public Map<Tracked, RequestCounter> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

}

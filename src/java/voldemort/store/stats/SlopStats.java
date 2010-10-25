package voldemort.store.stats;

import voldemort.cluster.Cluster;

import java.util.EnumMap;
import java.util.Map;

/**
 * Statistics for hinted handoff
 */
public class SlopStats {

    private final Map<Tracked, ClusterWideCounter> counters;

    public SlopStats(Cluster cluster) {
        counters = new EnumMap<Tracked, ClusterWideCounter>(Tracked.class);
        for(Tracked slopCounter: Tracked.values())
            counters.put(slopCounter, new ClusterWideCounter(cluster));
    }

    public Map<Integer, Long> asMap(Tracked metric) {
        return counters.get(metric).asMap();
    }

    public Map<Integer, Long> byZone(Tracked metric) {
        return counters.get(metric).byZone();
    }

    public Long getCount(Tracked metric, int nodeId) {
        return counters.get(metric).getCount(nodeId);
    }

    public Long getTotalCount(Tracked metric) {
        return counters.get(metric).getTotalCount();
    }

    public void clearCount(Tracked metric, int nodeId) {
        counters.get(metric).clearCount(nodeId);
    }

    public void incrementCount(Tracked metric, int nodeId) {
        counters.get(metric).incrementCount(nodeId);
    }

    public void setCount(Tracked metric, int nodeId, Long value) {
        counters.get(metric).setCount(nodeId, value);
    }

    public void setAll(Tracked metric, Map<Integer, Long> newValues) {
        counters.get(metric).setAll(newValues);
    }

    public static enum Tracked {
        OUTSTANDING("outstanding"),
        ADDED("added");

        private final String name;

        private Tracked(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}

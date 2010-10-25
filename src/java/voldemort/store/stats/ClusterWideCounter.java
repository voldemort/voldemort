package voldemort.store.stats;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe counter, that tracks per-node statistics and allows aggregation
 * by node and zone
 *
 */
public class ClusterWideCounter {

    private final static Logger logger = Logger.getLogger(ClusterWideCounter.class);

    private final Cluster cluster;
    private final ConcurrentMap<Integer, AtomicLong> values;

    public ClusterWideCounter(Cluster cluster) {
        this.cluster = cluster;
        this.values = new ConcurrentHashMap<Integer, AtomicLong>(cluster.getNumberOfNodes());
        for(Node node: cluster.getNodes())
            values.put(node.getId(), new AtomicLong(0L));
    }

    public void incrementCount(int nodeId) {
        AtomicLong counter = values.get(nodeId);
        if(counter == null) {
            counter = new AtomicLong(0L);
            values.putIfAbsent(nodeId, counter);
        }
        counter.incrementAndGet();
    }

    public void clearCount() {
        for(AtomicLong counter: values.values())
            counter.set(0L);
    }

    public void clearCount(int nodeId) {
        AtomicLong counter = values.get(nodeId);
        if(counter == null) {
            counter = new AtomicLong(0L);
            values.putIfAbsent(nodeId, counter);
        }
        counter.set(0L);
    }

    public void setCount(int nodeId, Long newValue) {
        AtomicLong counter = values.get(nodeId);
        if(counter == null) {
            counter = new AtomicLong(0L);
            values.putIfAbsent(nodeId, counter);
        }
        counter.set(newValue);
    }

    public void setAll(Map<Integer, Long> newValues) {
        for(Map.Entry<Integer, Long> entry: newValues.entrySet())
            setCount(entry.getKey(), entry.getValue());
    }

    public Map<Integer, Long> asMap() {
        ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
        for(Map.Entry<Integer, AtomicLong> entry: values.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().get());
        }
        return builder.build();
    }

    public Map<Integer, Long> byZone() {
        Map<Integer, Long> map = Maps.newHashMapWithExpectedSize(cluster.getNumberOfNodes());
        for(Map.Entry<Integer, AtomicLong> entry: values.entrySet()) {
            try {
                Node node = cluster.getNodeById(entry.getKey());
                int zoneId = node.getZoneId();
                Long count = map.get(zoneId);
                if(count == null)
                    count = 0L;
                count += entry.getValue().get();
                map.put(zoneId, count);
            } catch(VoldemortException e) {
                logger.warn("Can't get zone information for node id " + entry.getKey(), e);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    public Long getCount(int nodeId) {
        AtomicLong counter = values.get(nodeId);
        if(counter == null)
            throw new IllegalArgumentException("no value for " + nodeId);
        return counter.get();
    }

    public Long getTotalCount() {
        long total = 0L;
        for(AtomicLong value: values.values())
            total += value.get();
        return total;
    }
}

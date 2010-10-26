/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.Collections;
import java.util.Map;
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
            values.put(nodeId, counter);
        } else
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
        Map<Integer, Long> map = Maps.newHashMapWithExpectedSize(values.size());
        for(Map.Entry<Integer, AtomicLong> entry: values.entrySet())
            map.put(entry.getKey(), entry.getValue().get());
        return Collections.unmodifiableMap(map);
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

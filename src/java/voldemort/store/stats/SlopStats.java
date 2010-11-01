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

import java.util.EnumMap;
import java.util.Map;

import voldemort.cluster.Cluster;

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

    public void clearCount(Tracked metric) {
        counters.get(metric).clearCount();
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
        OUTSTANDING("outstanding");

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

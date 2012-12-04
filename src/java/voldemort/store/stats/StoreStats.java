/*
 * Copyright 2012 LinkedIn, Inc
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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Some convenient statistics to track about the store
 * 
 * 
 */
public class StoreStats {

    private final StoreStats parent;
    private final Map<Tracked, RequestCounter> counters;

    private static final Logger logger = Logger.getLogger(StoreStats.class.getName());

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
            counters.put(tracked, new RequestCounter(300000, true));
        }
        this.parent = parent;

        if(logger.isDebugEnabled()) {
            logger.debug("Constructed StoreStats object (" + System.identityHashCode(this)
                         + ") with parent object (" + System.identityHashCode(parent) + ")");
        }
    }

    /**
     * Record the duration of specified op. For PUT, GET and GET_ALL use
     * specific methods for those ops.
     */
    public void recordTime(Tracked op, long timeNS) {
        recordTime(op, timeNS, 0, 0, 0);
    }

    /**
     * Record the duration of a put operation, along with the size of the values
     * returned.
     */
    public void recordPutTimeAndSize(long timeNS, long size) {
        recordTime(Tracked.PUT, timeNS, 0, size, 0);
    }

    /**
     * Record the duration of a get operation, along with whether or not an
     * empty response (ie no values matched) and the size of the values
     * returned.
     */
    public void recordGetTime(long timeNS, boolean emptyResponse, long totalBytes) {
        recordTime(Tracked.GET, timeNS, emptyResponse ? 1 : 0, totalBytes, 0);
    }

    /**
     * Record the duration of a get_all operation, along with how many values
     * were requested, how may were actually returned and the size of the values
     * returned.
     */
    public void recordGetAllTime(long timeNS, int requested, int returned, long totalBytes) {
        recordTime(Tracked.GET_ALL, timeNS, requested - returned, totalBytes, requested);
    }

    /**
     * Method to service public recording APIs
     * 
     * @param op Operation being tracked
     * @param timeNS Duration of operation
     * @param numEmptyResponses GET and GET_ALL: number of empty responses being
     *        sent back, ie requested keys for which there were no values
     * @param size Total size of response payload, ie sum of lengths of bytes in
     *        all versions of values
     * @param getAllAggregateRequests Total of key-values requested in
     *        aggregatee from get_all operations
     */
    private void recordTime(Tracked op,
                            long timeNS,
                            long numEmptyResponses,
                            long size,
                            long getAllAggregateRequests) {
        counters.get(op).addRequest(timeNS, numEmptyResponses, size, getAllAggregateRequests);
        if(parent != null)
            parent.recordTime(op, timeNS, numEmptyResponses, size, getAllAggregateRequests);
    }

    public long getCount(Tracked op) {
        return counters.get(op).getCount();
    }

    public float getThroughput(Tracked op) {
        return counters.get(op).getThroughput();
    }

    public float getThroughputInBytes(Tracked op) {
        return counters.get(op).getThroughputInBytes();
    }

    public double getAvgTimeInMs(Tracked op) {
        return counters.get(op).getAverageTimeInMs();
    }

    public long getNumEmptyResponses(Tracked op) {
        return counters.get(op).getNumEmptyResponses();
    }

    public long getMaxLatencyInMs(Tracked op) {
        return counters.get(op).getMaxLatencyInMs();
    }

    public long getQ95LatencyInMs(Tracked op) {
        return counters.get(op).getQ95LatencyMs();
    }

    public long getQ99LatencyInMs(Tracked op) {
        return counters.get(op).getQ99LatencyMs();
    }

    public Map<Tracked, RequestCounter> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

    public long getMaxSizeInBytes(Tracked op) {
        return counters.get(op).getMaxSizeInBytes();
    }

    public double getAvgSizeinBytes(Tracked op) {
        return counters.get(op).getAverageSizeInBytes();
    }

    public double getGetAllAverageCount() {
        long total = getGetAllAggregatedCount();
        return total == 0 ? 0.0d : total / counters.get(Tracked.GET_ALL).getCount();
    }

    public long getGetAllAggregatedCount() {
        return counters.get(Tracked.GET_ALL).getGetAllAggregatedCount();
    }

    public long getGetAllMaxCount() {
        return counters.get(Tracked.GET_ALL).getGetAllMaxCount();
    }
}

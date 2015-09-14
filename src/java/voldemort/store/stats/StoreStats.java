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

import javax.management.ObjectName;

import org.apache.log4j.Logger;

import voldemort.utils.JmxUtils;

/**
 * Some convenient statistics to track about the store
 * 
 * 
 */
public class StoreStats {

    private final Map<Tracked, RequestCounter> counters;

    private static final Logger logger = Logger.getLogger(StoreStats.class.getName());

    private String storeName;

    // RequestCounter config
    private static final boolean useHistogram = true;
    private static final long timeWindow = 60000;

    public StoreStats(String storeName) {
        this(storeName, null);
    }

    /**
     * @param parent An optional parent stats object that will maintain
     *        aggregate data across many stores
     */
    public StoreStats(String storeName, StoreStats parent) {
        if (storeName == null) {
            throw new IllegalArgumentException("StoreStats.storeName cannot be null !!");
        }

        this.storeName = storeName;

        counters = new EnumMap<Tracked, RequestCounter>(Tracked.class);

        for(Tracked tracked: Tracked.values()) {
            String requestCounterName = "store-stats" + "." + storeName + "." + tracked.toString();
            RequestCounter requestCounter;

            if (parent == null) {
                requestCounter = new RequestCounter(requestCounterName, timeWindow, useHistogram);
            } else {
                requestCounterName = parent.storeName + "." + requestCounterName;
                requestCounter =
                        new RequestCounter(requestCounterName, timeWindow, useHistogram, parent.counters.get(tracked));
            }

            counters.put(tracked, requestCounter);
        }

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
        recordTime(op, timeNS, 0, 0, 0, 0);
    }

    /**
     * Record the duration of specified op. For PUT, GET and GET_ALL use
     * specific methods for those ops.
     */
    public void recordDeleteTime(long timeNS, long keySize) {
        recordTime(Tracked.DELETE, timeNS, 0, 0, keySize, 0);
    }

    /**
     * Record the duration of a put operation, along with the size of the values
     * returned.
     */
    public void recordPutTimeAndSize(long timeNS, long valueSize, long keySize) {
        recordTime(Tracked.PUT, timeNS, 0, valueSize, keySize, 0);
    }

    /**
     * Record the duration of a get operation, along with whether or not an
     * empty response (ie no values matched) and the size of the values
     * returned.
     */
    public void recordGetTime(long timeNS, boolean emptyResponse, long totalBytes, long keyBytes) {
        recordTime(Tracked.GET, timeNS, emptyResponse ? 1 : 0, totalBytes, keyBytes, 0);
    }

    /**
     * Record the duration of a get versions operation, along with whether or
     * not an empty response (ie no values matched) was returned.
     */
    public void recordGetVersionsTime(long timeNS, boolean emptyResponse) {
        recordTime(Tracked.GET_VERSIONS, timeNS, emptyResponse ? 1 : 0, 0, 0, 0);
    }

    /**
     * Record the duration of a get_all operation, along with how many values
     * were requested, how may were actually returned and the size of the values
     * returned.
     */
    public void recordGetAllTime(long timeNS,
                                 int requested,
                                 int returned,
                                 long totalValueBytes,
                                 long totalKeyBytes) {
        recordTime(Tracked.GET_ALL,
                   timeNS,
                   requested - returned,
                   totalValueBytes,
                   totalKeyBytes,
                   requested);
    }

    /**
     * Method to service public recording APIs
     * 
     * @param op Operation being tracked
     * @param timeNS Duration of operation
     * @param numEmptyResponses Number of empty responses being sent back,
     *                          i.e.: requested keys for which there were no values (GET and GET_ALL only)
     * @param valueSize Size in bytes of the value
     * @param keySize Size in bytes of the key
     * @param getAllAggregateRequests Total of amount of keys requested in the operation (GET_ALL only)
     */
    private void recordTime(Tracked op,
                            long timeNS,
                            long numEmptyResponses,
                            long valueSize,
                            long keySize,
                            long getAllAggregateRequests) {
        counters.get(op).addRequest(timeNS,
                                    numEmptyResponses,
                                    valueSize,
                                    keySize,
                                    getAllAggregateRequests);

        if (logger.isTraceEnabled() && !storeName.contains("aggregate") && !storeName.contains("voldsys$"))
            logger.trace("Store '" + storeName + "' logged a " + op.toString() + " request taking " +
                    ((double) timeNS / voldemort.utils.Time.NS_PER_MS) + " ms");
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

    public double getQ95LatencyInMs(Tracked op) {
        return counters.get(op).getQ95LatencyMs();
    }

    public double getQ99LatencyInMs(Tracked op) {
        return counters.get(op).getQ99LatencyMs();
    }

    public Map<Tracked, RequestCounter> getCounters() {
        return Collections.unmodifiableMap(counters);
    }

    public long getMaxValueSizeInBytes(Tracked op) {
        return counters.get(op).getMaxValueSizeInBytes();
    }

    public long getMaxKeySizeInBytes(Tracked op) {
        return counters.get(op).getMaxKeySizeInBytes();
    }

    public double getAvgValueSizeinBytes(Tracked op) {
        return counters.get(op).getAverageValueSizeInBytes();
    }

    public double getAvgKeySizeinBytes(Tracked op) {
        return counters.get(op).getAverageKeySizeInBytes();
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

    /**
     * @return The rate of keys per seconds that were queried through GetAll requests.
     */
    public float getGetAllKeysThroughput() { return counters.get(Tracked.GET_ALL).getGetAllKeysThroughput(); }

    private volatile boolean isJmxRegistered = false;
    private volatile ObjectName jmxObjectName;
    public synchronized void registerJmx(String identifierString) {
        if(isJmxRegistered == false) {
            StoreStatsJmx statsJmx = new StoreStatsJmx(this);
            String domain = JmxUtils.getPackageName(this.getClass());
            String type = this.storeName + identifierString;
            this.jmxObjectName = JmxUtils.createObjectName(domain, type);
            JmxUtils.registerMbean(statsJmx, jmxObjectName);

            isJmxRegistered = true;
        }
    }

    public synchronized void unregisterJmx() {
        if(isJmxRegistered == true && jmxObjectName != null) {
            JmxUtils.unregisterMbean(jmxObjectName);
            isJmxRegistered = false;
        }
    }
}

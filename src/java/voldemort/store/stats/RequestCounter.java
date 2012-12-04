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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import voldemort.utils.SystemTime;
import voldemort.utils.Time;

/**
 * A thread-safe request counter that calculates throughput for a specified
 * duration of time.
 * 
 * 
 */
public class RequestCounter {

    private final AtomicReference<Accumulator> values;
    private final long durationMs;
    private final Time time;
    private final Histogram histogram;
    private volatile long q95LatencyMs;
    private volatile long q99LatencyMs;
    private boolean useHistogram;

    private static final Logger logger = Logger.getLogger(RequestCounter.class.getName());

    /**
     * @param durationMs specifies for how long you want to maintain this
     *        counter (in milliseconds).
     */
    public RequestCounter(long durationMs) {
        this(durationMs, SystemTime.INSTANCE, false);
    }

    /**
     * @param durationMs specifies for how long you want to maintain this
     *        counter (in milliseconds). useHistogram indicates that this
     *        counter should also use a histogram.
     */
    public RequestCounter(long durationMs, boolean useHistogram) {
        this(durationMs, SystemTime.INSTANCE, useHistogram);
    }

    /**
     * For testing request expiration via an injected time provider
     */
    RequestCounter(long durationMs, Time time) {
        this(durationMs, time, false);
    }

    RequestCounter(long durationMs, Time time, boolean useHistogram) {
        this.time = time;
        this.values = new AtomicReference<Accumulator>(new Accumulator());
        this.durationMs = durationMs;
        this.q95LatencyMs = 0;
        this.q99LatencyMs = 0;
        this.useHistogram = useHistogram;
        if(this.useHistogram)
            this.histogram = new Histogram(10000, 1);
        else
            this.histogram = null;
    }

    public long getCount() {
        return getValidAccumulator().count;
    }

    public long getTotalCount() {
        return getValidAccumulator().total;
    }

    public float getThroughput() {
        Accumulator oldv = getValidAccumulator();
        double elapsed = (time.getMilliseconds() - oldv.startTimeMS) / (double) Time.MS_PER_SECOND;
        if(elapsed > 0f) {
            return (float) (oldv.count / elapsed);
        } else {
            return 0f;
        }
    }

    public float getThroughputInBytes() {
        Accumulator oldv = getValidAccumulator();
        double elapsed = (time.getMilliseconds() - oldv.startTimeMS) / (double) Time.MS_PER_SECOND;
        if(elapsed > 0f) {
            return (float) (oldv.totalBytes / elapsed);
        } else {
            return 0f;
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

    public long getDuration() {
        return durationMs;
    }

    public long getMaxLatencyInMs() {
        return getValidAccumulator().maxLatencyNS / Time.NS_PER_MS;
    }

    private void maybeResetHistogram() {
        if(!this.useHistogram)
            return;
        Accumulator accum = values.get();
        long now = time.getMilliseconds();
        if(now - accum.startTimeMS > durationMs) {
            // timing instrumentation (debug only)
            long startTimeNs = 0;
            if(logger.isDebugEnabled()) {
                startTimeNs = System.nanoTime();
            }

            // Reset the histogram
            q95LatencyMs = histogram.getQuantile(0.95);
            q99LatencyMs = histogram.getQuantile(0.99);
            histogram.reset();

            // timing instrumentation (debug only)
            if(logger.isDebugEnabled()) {
                logger.debug("Histogram (" + System.identityHashCode(histogram)
                             + ") : reset, Q95, & Q99 took " + (System.nanoTime() - startTimeNs)
                             + " ns.");
            }
        }
    }

    private Accumulator getValidAccumulator() {

        Accumulator accum = values.get();
        long now = time.getMilliseconds();

        /*
         * if still in the window, just return it
         */
        if(now - accum.startTimeMS <= durationMs) {
            return accum;
        }

        /*
         * try to set. if we fail, then someone else set it, so just return that
         * new one
         */

        Accumulator newWithTotal = accum.newWithTotal();

        if(values.compareAndSet(accum, newWithTotal)) {
            return newWithTotal;
        }

        return values.get();
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
        addRequest(timeNS, 0, 0, 0);
    }

    /**
     * @see #addRequest(long) Detailed request to track additionald data about
     *      PUT, GET and GET_ALL
     * 
     * @param numEmptyResponses For GET and GET_ALL, how many keys were no
     *        values found
     * @param bytes Total number of bytes across all versions of values' bytes
     * @param getAllAggregatedCount Total number of keys returned for getAll
     *        calls
     */
    public void addRequest(long timeNS,
                           long numEmptyResponses,
                           long bytes,
                           long getAllAggregatedCount) {
        // timing instrumentation (trace only)
        long startTimeNs = 0;
        if(logger.isTraceEnabled()) {
            startTimeNs = System.nanoTime();
        }

        long timeMs = timeNS / Time.NS_PER_MS;
        if(this.useHistogram) {
            histogram.insert(timeMs);
            maybeResetHistogram();
        }
        for(int i = 0; i < 3; i++) {
            Accumulator oldv = getValidAccumulator();
            Accumulator newv = new Accumulator(oldv.startTimeMS,
                                               oldv.count + 1,
                                               oldv.totalTimeNS + timeNS,
                                               oldv.total + 1,
                                               oldv.numEmptyResponses + numEmptyResponses,
                                               Math.max(timeNS, oldv.maxLatencyNS),
                                               oldv.totalBytes + bytes,
                                               Math.max(oldv.maxBytes, bytes),
                                               oldv.getAllAggregatedCount + getAllAggregatedCount,
                                               getAllAggregatedCount > oldv.getAllMaxCount ? getAllAggregatedCount
                                                                                          : oldv.getAllMaxCount);
            if(values.compareAndSet(oldv, newv)) {
                // timing instrumentation (trace only)
                if(logger.isTraceEnabled()) {
                    logger.trace("addRequest (histogram.insert and accumulator update) took "
                                 + (System.nanoTime() - startTimeNs) + " ns.");
                }
                // Return since data has been accumulated
                return;
            }
        }
        logger.info("addRequest lost timing instrumentation data because three retries was insufficient to update the accumulator.");

        // timing instrumentation (trace only)
        if(logger.isTraceEnabled()) {
            logger.trace("addRequest (histogram.insert and accumulator update) took "
                         + (System.nanoTime() - startTimeNs) + " ns.");
        }
    }

    /**
     * Return the number of requests that have returned returned no value for
     * the requested key. Tracked only for GET.
     */
    public long getNumEmptyResponses() {
        return getValidAccumulator().numEmptyResponses;
    }

    /**
     * Return the size of the largest response or request in bytes returned.
     * Tracked only for GET, GET_ALL and PUT.
     */
    public long getMaxSizeInBytes() {
        return getValidAccumulator().maxBytes;
    }

    /**
     * Return the average size of all the versioned values returned. Tracked
     * only for GET, GET_ALL and PUT.
     */
    public double getAverageSizeInBytes() {
        return getValidAccumulator().getAverageBytes();
    }

    /**
     * Return the aggregated number of keys returned across all getAll calls,
     * taking into account multiple values returned per call.
     */
    public long getGetAllAggregatedCount() {
        return getValidAccumulator().getAllAggregatedCount;
    }

    /**
     * Return the maximum number of keys returned across all getAll calls.
     */
    public long getGetAllMaxCount() {
        return getValidAccumulator().getAllMaxCount;
    }

    public long getQ95LatencyMs() {
        return q95LatencyMs;
    }

    public long getQ99LatencyMs() {
        return q99LatencyMs;
    }

    private class Accumulator {

        final long startTimeMS;
        final long count;
        final long totalTimeNS;
        final long total;
        final long numEmptyResponses; // GET and GET_ALL: number of empty
                                      // responses that have been returned
        final long getAllAggregatedCount; // GET_ALL: a single call to GET_ALL
                                          // can return multiple k-v pairs.
                                          // Track total requested.
        final long getAllMaxCount; // GET_ALL : track max number of keys
                                   // requesed
        final long maxLatencyNS;
        final long maxBytes; // Maximum single value
        final long totalBytes; // Sum of all the values

        public Accumulator() {
            this(RequestCounter.this.time.getMilliseconds(), 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        /**
         * This method resets startTimeMS.
         * 
         * @return
         */
        public Accumulator newWithTotal() {
            return new Accumulator(RequestCounter.this.time.getMilliseconds(),
                                   0,
                                   0,
                                   total,
                                   0,
                                   0,
                                   0,
                                   0,
                                   0,
                                   0);
        }

        public Accumulator(long startTimeMS,
                           long count,
                           long totalTimeNS,
                           long total,
                           long numEmptyResponses,
                           long maxLatencyNS,
                           long totalBytes,
                           long maxBytes,
                           long getAllAggregatedCount,
                           long getAllMaxCount) {
            this.startTimeMS = startTimeMS;
            this.count = count;
            this.totalTimeNS = totalTimeNS;
            this.total = total;
            this.numEmptyResponses = numEmptyResponses;
            this.maxLatencyNS = maxLatencyNS;
            this.totalBytes = totalBytes;
            this.maxBytes = maxBytes;
            this.getAllAggregatedCount = getAllAggregatedCount;
            this.getAllMaxCount = getAllMaxCount;
        }

        public double getAverageTimeNS() {
            return count > 0 ? 1f * totalTimeNS / count : 0f;
        }

        public double getAverageBytes() {
            return count > 0 ? 1f * totalBytes / count : -0f;
        }
    }
}

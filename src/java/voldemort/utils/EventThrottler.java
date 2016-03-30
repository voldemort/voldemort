/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.utils;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Quota;
import io.tehuti.metrics.QuotaViolationException;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import org.apache.log4j.Logger;
import voldemort.annotations.concurrency.NotThreadsafe;

import java.util.concurrent.TimeUnit;

/**
 * A class to throttle Events to a certain rate
 *
 * This class takes a maximum rate in events/sec and a minimum interval over
 * which to check the rate. The rate is measured over two rolling windows: one
 * full window, and one in-flight window. Each window is bounded to the provided
 * interval in ms, therefore, the total interval measured over is up to twice
 * the provided interval parameter. If the current event rate exceeds the maximum,
 * the call to {@link #maybeThrottle(int)} will block long enough to equalize it.
 *
 * This is a generalized IoThrottler as it existed before, which can be used to
 * throttle Bytes read or written, number of entries scanned, etc.
 */
@NotThreadsafe
public class EventThrottler {

    private static final Logger logger = Logger.getLogger(EventThrottler.class);
    private static final long DEFAULT_CHECK_INTERVAL_MS = 1000;
    private static final String THROTTLER_NAME = "event-throttler";

    private final long maxRatePerSecond;

    // Tehuti stuff
    private final io.tehuti.utils.Time time;
    private static final MetricsRepository sharedMetricsRepository = new MetricsRepository();
    private final MetricsRepository metricsRepository;
    private final Rate rate;
    private final Sensor rateSensor;
    private final MetricConfig rateConfig;

    /**
     * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
     */
    public EventThrottler(long maxRatePerSecond) {
        this(maxRatePerSecond, DEFAULT_CHECK_INTERVAL_MS, null);
    }

    /**
     * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
     * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
     * @param throttlerName if specified, the throttler will share its limit with others named the same
     *                      if null, the throttler will be independent of the others
     */
    public EventThrottler(long maxRatePerSecond, long intervalMs, String throttlerName) {
        this(new io.tehuti.utils.SystemTime(), maxRatePerSecond, intervalMs, throttlerName);
    }

    /**
     * @param time Used to inject a {@link io.tehuti.utils.Time} in tests
     * @param maxRatePerSecond Maximum rate that this throttler should allow (0 is unlimited)
     * @param intervalMs Minimum interval over which the rate is measured (maximum is twice that)
     * @param throttlerName if specified, the throttler will share its limit with others named the same
     *                      if null, the throttler will be independent of the others
     */
    public EventThrottler(io.tehuti.utils.Time time,
                          long maxRatePerSecond,
                          long intervalMs,
                          String throttlerName) {
        this.maxRatePerSecond = maxRatePerSecond;
        if (maxRatePerSecond > 0) {
            this.time = Utils.notNull(time);
            if (intervalMs <= 0) {
                throw new IllegalArgumentException("intervalMs must be a positive number.");
            }
            this.rateConfig = new MetricConfig()
                    .timeWindow(intervalMs, TimeUnit.MILLISECONDS)
                    .quota(Quota.lessThan(maxRatePerSecond));
            this.rate = new Rate(TimeUnit.SECONDS);
            if (throttlerName == null) {
                // Then we want this EventThrottler to be independent.
                this.metricsRepository = new MetricsRepository(time);
                this.rateSensor = metricsRepository.sensor(THROTTLER_NAME, rateConfig);
                rateSensor.add(THROTTLER_NAME + ".rate", rate, rateConfig);
            } else {
                // Then we want to share the EventThrottler's limit with other instances having the same name.
                this.metricsRepository = sharedMetricsRepository;
                synchronized (sharedMetricsRepository) {
                    Sensor existingSensor = sharedMetricsRepository.getSensor(throttlerName);
                    if (existingSensor != null) {
                        this.rateSensor = existingSensor;
                    } else {
                        // Create it once for all EventThrottlers sharing that name
                        this.rateSensor = sharedMetricsRepository.sensor(throttlerName);
                        this.rateSensor.add(throttlerName + ".rate", rate, rateConfig);
                    }
                }
            }
        } else {
            // DISABLED, no point in allocating anything...
            this.time = null;
            this.metricsRepository = null;
            this.rate = null;
            this.rateSensor = null;
            this.rateConfig = null;
        }

        if(logger.isDebugEnabled())
            logger.debug("EventThrottler constructed with maxRatePerSecond = " + maxRatePerSecond);

    }

    /**
     * Sleeps if necessary to slow down the caller.
     *
     * @param eventsSeen Number of events seen since last invocation. Basis for
     *        determining whether its necessary to sleep.
     */
    public synchronized void maybeThrottle(int eventsSeen) {
        if (maxRatePerSecond > 0) {
            long now = time.milliseconds();
            try {
                rateSensor.record(eventsSeen, now);
            } catch (QuotaViolationException e) {
                // If we're over quota, we calculate how long to sleep to compensate.
                double currentRate = e.getValue();
                if (currentRate > this.maxRatePerSecond) {
                    double excessRate = currentRate - this.maxRatePerSecond;
                    long sleepTimeMs = Math.round(excessRate / this.maxRatePerSecond * voldemort.utils.Time.MS_PER_SECOND);
                    if(logger.isDebugEnabled()) {
                        logger.debug("Throttler quota exceeded:\n" +
                                "eventsSeen \t= " + eventsSeen + " in this call of maybeThrotte(),\n" +
                                "currentRate \t= " + currentRate + " events/sec,\n" +
                                "maxRatePerSecond \t= " + this.maxRatePerSecond + " events/sec,\n" +
                                "excessRate \t= " + excessRate + " events/sec,\n" +
                                "sleeping for \t" + sleepTimeMs + " ms to compensate.\n" +
                                "rateConfig.timeWindowMs() = " + rateConfig.timeWindowMs());
                    }
                    if (sleepTimeMs > rateConfig.timeWindowMs()) {
                        logger.warn("Throttler sleep time (" + sleepTimeMs + " ms) exceeds " +
                                "window size (" + rateConfig.timeWindowMs() + " ms). This will likely " +
                                "result in not being able to honor the rate limit accurately.");
                        // When using the HDFS Fetcher, setting the hdfs.fetcher.buffer.size
                        // too high could cause this problem.
                    }
                    time.sleep(sleepTimeMs);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Weird. Got QuotaValidationException but measured rate not over rateLimit: " +
                            "currentRate = " + currentRate + " , rateLimit = " + this.maxRatePerSecond);
                }
            }
        }
    }
}

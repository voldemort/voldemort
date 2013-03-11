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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;

/**
 * A class to throttle Events to a certain rate
 * 
 * This class takes a maximum rate in events/sec and a minimum interval in ms at
 * which to check the rate. The rate is checked every time the interval
 * ellapses, and if the events rate exceeds the maximum, the call will block
 * long enough to equalize it.
 * 
 * This is generalized IoThrottler as it existed before, you can use it to
 * throttle on Bytes read/write,number of entries scanned etc.
 * 
 * 
 */
@NotThreadsafe
public class EventThrottler {

    private final static Logger logger = Logger.getLogger(EventThrottler.class);
    private final static long DEFAULT_CHECK_INTERVAL_MS = 50;

    private final Time time;
    private final long ratesPerSecond;
    private final long intervalMs;
    private long startTime;
    private long eventsSeenInLastInterval;

    public EventThrottler(long ratesPerSecond) {
        this(SystemTime.INSTANCE, ratesPerSecond, DEFAULT_CHECK_INTERVAL_MS);
    }

    public long getRate() {
        return this.ratesPerSecond;
    }

    public EventThrottler(Time time, long ratePerSecond, long intervalMs) {
        this.time = time;
        this.intervalMs = intervalMs;
        this.ratesPerSecond = ratePerSecond;
        this.eventsSeenInLastInterval = 0L;
        this.startTime = 0L;
    }

    /**
     * Sleeps if necessary to slow down the caller.
     * 
     * @param eventsSeen Number of events seen since last invocation. Basis for
     *        determining whether its necessary to sleep.
     */
    public synchronized void maybeThrottle(int eventsSeen) {
        // TODO: This implements "bang bang" control. This is OK. But, this
        // permits unbounded bursts of activity within the intervalMs. A
        // controller that has more memory and explicitly bounds peak activity
        // within the intervalMs may be better.
        long rateLimit = getRate();

        if(logger.isDebugEnabled())
            logger.debug("Rate = " + rateLimit);

        eventsSeenInLastInterval += eventsSeen;
        long now = time.getNanoseconds();
        long ellapsedNs = now - startTime;
        // if we have completed an interval AND we have seen some events, maybe
        // we should take a little nap
        if(ellapsedNs > intervalMs * Time.NS_PER_MS && eventsSeenInLastInterval > 0) {
            long eventsPerSec = (eventsSeenInLastInterval * Time.NS_PER_SECOND) / ellapsedNs;
            if(eventsPerSec > rateLimit) {
                // solve for the amount of time to sleep to make us hit the
                // correct i/o rate
                double maxEventsPerMs = rateLimit / (double) Time.MS_PER_SECOND;
                long ellapsedMs = ellapsedNs / Time.NS_PER_MS;
                long sleepTime = Math.round(eventsSeenInLastInterval / maxEventsPerMs - ellapsedMs);

                if(logger.isDebugEnabled())
                    logger.debug("Natural rate is " + eventsPerSec
                                 + " events/sec max allowed rate is " + rateLimit
                                 + " events/sec, sleeping for " + sleepTime + " ms to compensate.");
                if(sleepTime > 0) {
                    try {
                        time.sleep(sleepTime);
                    } catch(InterruptedException e) {
                        throw new VoldemortException(e);
                    }
                }
            }
            startTime = now;
            eventsSeenInLastInterval = 0;
        }
    }
}

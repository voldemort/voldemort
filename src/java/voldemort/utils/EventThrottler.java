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
 * @author jay
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

    public EventThrottler(Time time, long ratePerSecond, long intervalMs) {
        this.time = time;
        this.intervalMs = intervalMs;
        this.ratesPerSecond = ratePerSecond;
        this.eventsSeenInLastInterval = 0L;
        this.startTime = 0L;
    }

    public void maybeThrottle(int eventsSeen) {
        eventsSeenInLastInterval += eventsSeen;
        long now = time.getNanoseconds();
        long ellapsedNs = now - startTime;
        // if we have completed an interval AND we have seen some events, maybe
        // we should take a little nap
        if(ellapsedNs > intervalMs * Time.NS_PER_MS && eventsSeenInLastInterval > 0) {
            long eventsPerSec = (eventsSeenInLastInterval * Time.NS_PER_SECOND) / ellapsedNs;
            if(eventsPerSec > ratesPerSecond) {
                // solve for the amount of time to sleep to make us hit the
                // correct i/o rate
                double maxEventsPerMs = ratesPerSecond / (double) Time.MS_PER_SECOND;
                long ellapsedMs = ellapsedNs / Time.NS_PER_MS;
                long sleepTime = Math.round(eventsSeenInLastInterval / maxEventsPerMs - ellapsedMs);
                if(logger.isDebugEnabled())
                    logger.debug("Natural rate is " + eventsPerSec
                                 + " events/sec max allowed rate is " + ratesPerSecond
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

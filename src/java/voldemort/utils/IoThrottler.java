package voldemort.utils;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;

/**
 * A class to throttle IO to a certain rate
 * 
 * This class takes a maximum rate in bytes/sec and a minimum interval in ms at
 * which to check the rate. The rate is checked every time the interval
 * ellapses, and if the IO rate exceeds the maximum, the call will block long
 * enough to equalize it.
 * 
 * @author jay
 * 
 */
@NotThreadsafe
public class IoThrottler {

    private final static Logger logger = Logger.getLogger(IoThrottler.class);
    private final static long DEFAULT_CHECK_INTERVAL_MS = 50;

    private final Time time;
    private final long maxBytesPerSecond;
    private final long intervalMs;
    private long startTime;
    private long bytesReadInInterval;

    public IoThrottler(long maxBytesPerSecond) {
        this(SystemTime.INSTANCE, maxBytesPerSecond, DEFAULT_CHECK_INTERVAL_MS);
    }

    public IoThrottler(Time time, long maxBytesPerSecond, long intervalMs) {
        this.time = time;
        this.intervalMs = intervalMs;
        this.maxBytesPerSecond = maxBytesPerSecond;
        this.bytesReadInInterval = 0L;
        this.startTime = 0L;
    }

    public void maybeThrottle(int bytesRead) {
        bytesReadInInterval += bytesRead;
        long now = time.getNanoseconds();
        long ellapsedNs = now - startTime;
        // if we have completed an interval AND we have read some bytes, maybe
        // we should take a little nap
        if(ellapsedNs > intervalMs * Time.NS_PER_MS && bytesReadInInterval > 0) {
            long bytesPerSec = (bytesReadInInterval * Time.NS_PER_SECOND) / ellapsedNs;
            if(bytesPerSec > maxBytesPerSecond) {
                // solve for the amount of time to sleep to make us hit the
                // correct i/o rate
                double maxBytesPerMs = maxBytesPerSecond / (double) Time.MS_PER_SECOND;
                long ellapsedMs = ellapsedNs / Time.NS_PER_MS;
                long sleepTime = Math.round(bytesReadInInterval / maxBytesPerMs - ellapsedMs);
                if(logger.isDebugEnabled())
                    logger.debug("Natural I/O rate is " + bytesPerSec + " bytes/sec, sleeping for "
                                 + sleepTime + " ms to compensate.");
                if(sleepTime > 0) {
                    try {
                        time.sleep(sleepTime);
                    } catch(InterruptedException e) {
                        throw new VoldemortException(e);
                    }
                }
            }
            startTime = now;
            bytesReadInInterval = 0;
        }
    }
}

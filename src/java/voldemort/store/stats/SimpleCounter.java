package voldemort.store.stats;

import java.util.concurrent.atomic.AtomicLong;

import voldemort.utils.Time;

/**
 * A simple concurrent, non-blocking event counter that resets itself every
 * interval, and provides eventRate and average event value metrics over the
 * last interval
 * 
 */
public class SimpleCounter {

    /**
     * Count of total number of events in current interval
     */
    AtomicLong eventsCounter;
    /**
     * Sum of all the event values in the current interval
     */
    AtomicLong eventsValueCounter;
    /**
     * Last time when the counter was reset
     */
    AtomicLong lastResetTimeMs;

    /**
     * Number of events that occurred in the last interval
     */
    long numEventsLastInterval;

    /**
     * Sum of all the event values in the the last interval
     */
    long totalEventValueLastInterval;

    // We need additional tracking for the end of the second last or penultimate
    // interval, since resetting the atomicLong counters would mean we might
    // miss some event updates

    /**
     * Number of events that occurred in the second last interval.
     */
    long numEventsLastLastInterval;

    /**
     * Sum of all the event values in the the second last interval.
     */
    long totalEventValueLastLastInterval;

    /**
     * The counter will be reset once this many ms
     */
    final long resetIntervalMs;

    public SimpleCounter(long resetIntervalMs) {
        if(resetIntervalMs < 1) {
            throw new IllegalArgumentException("Reset interval must be positive");
        }
        this.resetIntervalMs = resetIntervalMs;
        this.lastResetTimeMs = new AtomicLong(System.currentTimeMillis());
        this.eventsValueCounter = new AtomicLong(0);
        this.eventsCounter = new AtomicLong(0);
        this.numEventsLastInterval = 0;
        this.totalEventValueLastInterval = 0;
        this.numEventsLastLastInterval = 0;
        this.totalEventValueLastLastInterval = 0;
    }

    public void count() {
        this.count(0);
    }

    public void count(long eventValue) {
        resetIfNeeded();
        eventsCounter.incrementAndGet();
        eventsValueCounter.addAndGet(eventValue);
    }

    private void resetIfNeeded() {
        long currentLastResetTimeMs = lastResetTimeMs.longValue();
        long now = System.currentTimeMillis();

        // check if interval might have expired
        if((now - currentLastResetTimeMs) >= resetIntervalMs) {
            long numEvents = eventsCounter.longValue();
            long totalEventValue = eventsValueCounter.longValue();
            // more than one thread can get here concurrently. But exactly one
            // will pass the check below
            if(lastResetTimeMs.compareAndSet(currentLastResetTimeMs, now)) {
                // the synchronization is for any monitoring thread to read a
                // consistent state for reporting
                synchronized(this) {
                    // reseting this counters here might be problematic since
                    // another application thread can go ahead and update the
                    // counters and we will miss those data points. instead we
                    // simply update the delta from the current interval. This
                    // guarantees correctness
                    numEventsLastLastInterval = numEventsLastInterval;
                    totalEventValueLastLastInterval = totalEventValueLastInterval;
                    numEventsLastInterval = numEvents;
                    totalEventValueLastInterval = totalEventValue;
                }
            }
        }
    }

    /**
     * Returns the events per second in the current interval
     * 
     * @return
     */
    public Double getEventRate() {
        resetIfNeeded();
        synchronized(this) {
            return (numEventsLastInterval - numEventsLastLastInterval)
                   / ((1.0 * resetIntervalMs) / Time.MS_PER_SECOND);
        }
    }

    /**
     * Returns the average event value in the current interval
     */
    public Double getAvgEventValue() {
        resetIfNeeded();
        synchronized(this) {
            long eventsLastInterval = numEventsLastInterval - numEventsLastLastInterval;
            if(eventsLastInterval > 0)
                return ((totalEventValueLastInterval - totalEventValueLastLastInterval) * 1.0)
                       / eventsLastInterval;
            else
                return 0.0;
        }
    }
}

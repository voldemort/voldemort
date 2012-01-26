package voldemort.utils;

public class DynamicEventThrottler extends EventThrottler {

    private long dynamicRatePerSecond = 0l;
    private DynamicThrottleLimit dynThrottleLimit;

    public DynamicEventThrottler(long ratesPerSecond) {
        super(ratesPerSecond);
        this.dynamicRatePerSecond = ratesPerSecond;
        this.dynThrottleLimit = null;
    }

    public DynamicEventThrottler(DynamicThrottleLimit dynLimit) {
        super(dynLimit.getRate());
        this.dynThrottleLimit = dynLimit;
    }

    public DynamicEventThrottler(Time time, long ratePerSecond, long intervalMs) {
        super(time, ratePerSecond, intervalMs);
    }

    public void updateRate(long l) {
        this.dynamicRatePerSecond = l;
    }

    public void incrementNumJobs() {
        this.dynThrottleLimit.incrementNumJobs();
    }

    public void decrementNumJobs() {
        this.dynThrottleLimit.decrementNumJobs();
    }

    @Override
    public long getRate() {
        if(this.dynThrottleLimit != null)
            return dynThrottleLimit.getRate();
        else
            return this.dynamicRatePerSecond;
    }
}

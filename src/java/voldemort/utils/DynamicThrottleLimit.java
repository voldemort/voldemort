package voldemort.utils;

import org.apache.log4j.Logger;

public class DynamicThrottleLimit {

    private long perNodeRate = 0l;
    private long dynamicRatePerSecond = 0l;
    private int numJobs = 0;
    private final static Logger logger = Logger.getLogger(DynamicThrottleLimit.class);

    public DynamicThrottleLimit(long val) {
        this.dynamicRatePerSecond = this.perNodeRate = val;
    }

    synchronized public long getRate() {
        return this.dynamicRatePerSecond;
    }

    synchronized public void incrementNumJobs() {
        this.numJobs++;
        this.dynamicRatePerSecond = numJobs > 0 ? this.perNodeRate / numJobs : this.perNodeRate;
        logger.debug("# Jobs = " + numJobs + ". Updating throttling rate to : "
                     + this.dynamicRatePerSecond + " bytes / sec");
    }

    synchronized public void decrementNumJobs() {
        if(this.numJobs > 0)
            this.numJobs--;
        this.dynamicRatePerSecond = numJobs > 0 ? this.perNodeRate / numJobs : this.perNodeRate;
        logger.debug("# Jobs = " + numJobs + ". Updating throttling rate to : "
                     + this.dynamicRatePerSecond + " bytes / sec");
    }

    synchronized public long getSpeculativeRate() {
        long dynamicRate = 0;
        int totalJobs = this.numJobs + 1;
        dynamicRate = totalJobs > 0 ? this.perNodeRate / totalJobs : this.perNodeRate;
        return dynamicRate;
    }
}

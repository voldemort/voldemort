package voldemort.utils.socketpool;

import java.util.concurrent.TimeUnit;

/**
 * Resource pool config class.
 * 
 * @author bbansal
 * 
 */
public class ResourcePoolConfig {

    private int defaultPoolSize = 100;
    private long borrowTimeout = 1000;
    private TimeUnit borrowTimeoutUnit = TimeUnit.MILLISECONDS;
    private long returnTimeout = 10;
    private TimeUnit returnTimeoutUnit = TimeUnit.MILLISECONDS;

    public int getDefaultPoolSize() {
        return defaultPoolSize;
    }

    public void setDefaultPoolSize(int defaultPoolSize) {
        this.defaultPoolSize = defaultPoolSize;
    }

    public long getBorrowTimeout() {
        return borrowTimeout;
    }

    public void setBorrowTimeout(long borrowTimeout) {
        this.borrowTimeout = borrowTimeout;
    }

    public TimeUnit getBorrowTimeoutUnit() {
        return borrowTimeoutUnit;
    }

    public void setBorrowTimeoutUnit(TimeUnit borrowTimeUnit) {
        this.borrowTimeoutUnit = borrowTimeUnit;
    }

    public TimeUnit getReturnTimeoutUnit() {
        return returnTimeoutUnit;
    }

    public void setReturnTimeoutUnit(TimeUnit returnTimeoutUnit) {
        this.returnTimeoutUnit = returnTimeoutUnit;
    }

    public void setReturnTimeout(long returnTimeout) {
        this.returnTimeout = returnTimeout;
    }

    public long getReturnTimeout() {
        return this.returnTimeout;
    }
}

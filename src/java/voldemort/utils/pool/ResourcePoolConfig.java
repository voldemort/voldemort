package voldemort.utils.pool;

import java.util.concurrent.TimeUnit;

/**
 * Resource pool config class.
 * 
 * 
 */
public class ResourcePoolConfig {

    /* Note: if you change the defaults you must update the javadoc as well. */
    private int poolMaxSize = 20;
    private long timeoutNs = Long.MAX_VALUE;
    private int maxInvalidResourceCreations = Integer.MAX_VALUE;
    private boolean isFair = true;

    public ResourcePoolConfig() {
        super();
    }

    /**
     * Get the size of the pool
     */
    public int getMaxPoolSize() {
        return poolMaxSize;
    }

    /**
     * The size of the pool to maintain for each key.
     * 
     * The default pool size is 20
     * 
     * @param poolSize The desired per-key pool size
     */
    public ResourcePoolConfig setMaxPoolSize(int poolSize) {
        if(poolSize <= 0)
            throw new IllegalArgumentException("Pool size must be a positive number.");
        this.poolMaxSize = poolSize;
        return this;
    }

    /**
     * Get the the pool timeout in the given units
     * 
     * @param unit The units in which to fetch the timeout
     * @return The timeout
     */
    public long getTimeout(TimeUnit unit) {
        return unit.convert(timeoutNs, TimeUnit.NANOSECONDS);
    }

    /**
     * The timeout which we block for when a resource is not available
     * 
     * @param timeout The timeout
     * @param unit The units of the timeout
     */
    public ResourcePoolConfig setTimeout(long timeout, TimeUnit unit) {
        if(timeout < 0)
            throw new IllegalArgumentException("The timeout must be a non-negative number.");
        this.timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, unit);
        return this;
    }

    /**
     * The maximum number of successive invalid resources that can be created in
     * a single checkout. The purpose of this parameter is to avoid churning the
     * created objects in the case that the created object is immediately
     * invalid.
     * 
     * Default is unlimited.
     * 
     * @param limit The desired limit
     */
    public ResourcePoolConfig setMaxInvalidAttempts(int limit) {
        if(limit <= 0)
            throw new IllegalArgumentException("Limit must be a positive number.");
        this.maxInvalidResourceCreations = limit;
        return this;
    }

    /**
     * Get the maximum number of invalid resources that can be created before
     * throwing an exception.
     */
    public int getMaximumInvalidResourceCreationLimit() {
        return this.maxInvalidResourceCreations;
    }

    /**
     * Controls whether the pool gives resources to threads in the order they
     * arrive or not
     */
    public boolean isFair() {
        return this.isFair;
    }

    /**
     * Controls whether the pool gives resources to threads in the order they
     * arrive or not. An unfair pool is approximately 10x faster, but gives not
     * guarantee on the order in which waiting threads get a resource.
     */
    public ResourcePoolConfig setIsFair(boolean isFair) {
        this.isFair = isFair;
        return this;
    }

}

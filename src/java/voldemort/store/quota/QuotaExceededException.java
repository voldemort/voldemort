package voldemort.store.quota;

import voldemort.VoldemortApplicationException;

/**
 * An exception that indicates that user has exceeded the throughput for Quota
 * allowed. The throughput is configurable per store per operation.
 * 
 * This extends VoldemortApplicationException due to its critical nature. In
 * most situations the request is dropped and operations is failed when it
 * exceeds Quota. Except, when a Serial Put to master succeeds and parallel puts
 * fail due to exceeding quota, slops are registered.
 * 
 */
public class QuotaExceededException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public QuotaExceededException(String message) {
        super(message);
    }

    public QuotaExceededException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Override to avoid the overhead of stack trace. These are thrown only from
     * QuotaLimitingStore on exceeding the Quota.
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}

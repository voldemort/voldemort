package voldemort.utils.pool;

/**
 * Interface for asynchronous requests for resources. Exactly one of
 * useResource, handleTimeout, or handleException expected to be invoked within
 * deadline specified by getDeadlineNs.
 */
public interface AsyncResourceRequest<V> {

    /**
     * To be invoked with resource to use.
     * 
     * @param resource. resource should not be null.
     */
    void useResource(V resource);

    /**
     * Invoked sometime after deadline.
     */
    void handleTimeout();

    /**
     * Invoked upon exception trying to process resource request.
     * 
     * @param e
     */
    void handleException(Exception e);

    /**
     * 
     * @return Deadline (in nanoseconds), after which handleTimeout() should be
     *         invoked.
     */
    long getDeadlineNs();

    long getStartTimeNs();

    long getStartTimeMs();
}

package voldemort.client;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.utils.DaemonThreadFactory;

/**
 * A thread pool with a more convenient constructor and some jmx monitoring
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "A voldemort client thread pool")
public class ClientThreadPool extends ThreadPoolExecutor {

    public ClientThreadPool(int maxThreads, long threadIdleMs, int maxQueuedRequests) {
        super(1,
              maxThreads,
              threadIdleMs,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(maxQueuedRequests),
              new DaemonThreadFactory("voldemort-client-thread-"),
              new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @JmxGetter(name = "numberOfActiveThreads", description = "The number of active threads.")
    public int getNumberOfActiveThreads() {
        return this.getActiveCount();
    }

    @JmxGetter(name = "numberOfThreads", description = "The total number of threads, active and idle.")
    public int getNumberOfThreads() {
        return this.getPoolSize();
    }

    @JmxGetter(name = "queuedRequests", description = "Number of requests in the queue waiting to execute.")
    public int getQueuedRequests() {
        return this.getQueue().size();
    }
}

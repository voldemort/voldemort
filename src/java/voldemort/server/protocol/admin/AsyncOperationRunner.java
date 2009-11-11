package voldemort.server.protocol.admin;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxManaged;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * @author afeinberg
 * Asynchronous job scheduler for admin service requests
 *
 */
@JmxManaged(description = "Execute asynchronous operations")
public class AsyncOperationRunner {
    private static final Logger logger = Logger.getLogger(AsyncOperationRunner.class);

    private final Map<String, AsyncOperation> requests;
    private final ScheduledThreadPoolExecutor scheduler;

    private static final ThreadFactory schedulerThreadFactory = new ThreadFactory() {

           public Thread newThread(Runnable r) {
               Thread thread = new Thread(r);
               thread.setDaemon(true);
               thread.setName(r.getClass().getName());
               thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                   public void uncaughtException(Thread t, Throwable e) {
                       logger.error("Scheduled task failed!", e);
                   }
               });

               return thread;
           }
       };


    @SuppressWarnings("unchecked") // apache commons collections aren't updated for 1.5 yet
    public AsyncOperationRunner(int poolSize, int cacheSize) {
        requests = Collections.synchronizedMap(new AsyncOperationRepository(cacheSize));
        scheduler = new ScheduledThreadPoolExecutor(poolSize, schedulerThreadFactory);
    }

    /**
     * Submit a operation. Throw a run time exception if the operation is already submitted
     * @param operation The asynchronous operation to submit
     * @param requestId Id of the request
     */
    public void startRequest(String requestId, AsyncOperation operation) {
        if (requests.containsKey(requestId)) {
            throw new VoldemortException("Request " + requestId + " already submitted to the system");
        }
        requests.put(requestId, operation);
        scheduler.submit(operation);
        logger.debug("Handling async operation " + requestId);
    }



    /**
     * Is a request complete? If so, forget the requests
     * @param requestId Id of the request
     * @return True if request is complete, false otherwise
     */
    public boolean isComplete(String requestId) {
        if (!requests.containsKey(requestId)) {
            throw new VoldemortException("No request with id " + requestId + " found");
        }

        if (requests.get(requestId).getStatus().isComplete()) {
            logger.debug("Request complete " + requestId);
            requests.remove(requestId);

            return true;
        }
        return false;
    }

    public String getRequestStatus(String requestId) {
        if (!requests.containsKey(requestId)) {
            throw new VoldemortException("No request with id " + requestId + " found");
        }

        return requests.get(requestId).getStatus().getStatus();
    }
}

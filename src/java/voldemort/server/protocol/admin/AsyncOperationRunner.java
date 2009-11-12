package voldemort.server.protocol.admin;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author afeinberg
 * Asynchronous job scheduler for admin service requests
 *
 */
public class AsyncOperationRunner {
    private final Map<Integer, AsyncOperation> requests;
    private final ExecutorService executor;
    private final Logger logger = Logger.getLogger(AsyncOperationRunner.class);

    @SuppressWarnings("unchecked") // apache commons collections aren't updated for 1.5 yet
    public AsyncOperationRunner(int poolSize, int cacheSize) {
        requests = Collections.synchronizedMap(new AsyncOperationRepository(cacheSize));
        executor = Executors.newFixedThreadPool(poolSize);
    }

    /**
     * Submit a operation. Throw a run time exception if the operation is already submitted
     * @param operation The asynchronous operation to submit
     * @param requestId Id of the request
     */
    public void startRequest(int requestId, AsyncOperation operation) {
        if (requests.containsKey(requestId)) {
            throw new VoldemortException("Request " + requestId + " already submitted to the system");
        }
        requests.put(requestId, operation);
        executor.submit(operation);
        logger.debug("Handling async operation " + requestId);
    }

    /**
     * Is a request complete? If so, forget the requests
     * @param requestId Id of the request
     * @return True if request is complete, false otherwise
     */
    public boolean isComplete(int requestId) {
        if (!requests.containsKey(requestId)) {
            throw new VoldemortException("No request with id " + requestId + " found");
        }

        if (requests.get(requestId).getComplete()) {
            logger.debug("Request complete " + requestId);
            requests.remove(requestId);

            return true;
        }
        return false;
    }

    public String getRequestStatus(int requestId) {
        if (!requests.containsKey(requestId)) {
            throw new VoldemortException("No request with id " + requestId + " found");
        }

        return requests.get(requestId).getStatus();
    }
}

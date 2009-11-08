package voldemort.client.protocol.admin;

import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author afeinberg
 * Asynchronous job scheduler for admin service requests
 *
 * TODO: purge stale requests
 */
public class AsyncRequestRunner {
    private final Map<String,AsyncRequest> requests;
    private final ExecutorService executor;
    private final Logger logger = Logger.getLogger(AsyncRequestRunner.class);

    @SuppressWarnings("unchecked") // apache commons collections aren't updated for 1.5 yet
    public AsyncRequestRunner(int poolSize, int cacheSize) {
        requests = Collections.synchronizedMap(new LRUMap(cacheSize));
        executor = Executors.newFixedThreadPool(poolSize);
    }

    /**
     * Submit a request. Throw a run time exception if the request is already submitted
     * @param request The asynchronous request to submit
     */
    public void startRequest(AsyncRequest request) {
        if (requests.containsKey(request.getId())) {
            throw new VoldemortException("Request " + request.getId() + " already submitted to the system");
        }
        requests.put(request.getId(), request);
        executor.submit(request);
        logger.debug("Handling async request " + request.getId());
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

        if (requests.get(requestId).getComplete()) {
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

        return requests.get(requestId).getStatus();
    }
}

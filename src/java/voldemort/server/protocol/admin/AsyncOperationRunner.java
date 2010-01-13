package voldemort.server.protocol.admin;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.scheduler.SchedulerService;
import voldemort.utils.SystemTime;

/**
 * @author afeinberg
 * Asynchronous job scheduler for admin service operations
 */
@JmxManaged(description = "Asynchronous operation execution")
public class AsyncOperationRunner extends AbstractService {

    private final Map<Integer, AsyncOperation> operations;
    private final AtomicInteger lastOperationId = new AtomicInteger(0);
    private final SchedulerService scheduler;

    private final static Logger logger = Logger.getLogger(AsyncOperationRunner.class);

    @SuppressWarnings("unchecked")
    // apache commons collections aren't updated for 1.5 yet
    public AsyncOperationRunner(SchedulerService scheduler, int cacheSize) {
        super(ServiceType.ASYNC_SCHEDULER);
        operations = Collections.synchronizedMap(new AsyncOperationRepository(cacheSize));
        this.scheduler = scheduler;
    }

    /**
     * Submit a operations. Throw a run time exception if the operations is
     * already submitted
     * 
     * @param operation The asynchronous operations to submit
     * @param requestId Id of the request
     */
    public synchronized void submitOperation(int requestId, AsyncOperation operation) {
        if(this.operations.containsKey(requestId))
            throw new VoldemortException("Request " + requestId
                                         + " already submitted to the system");

        this.operations.put(requestId, operation);
        scheduler.scheduleNow(operation);
        logger.debug("Handling async operation " + requestId);
    }

    /**
     * Is a request complete? If so, forget the operations
     * 
     * @param requestId Id of the request
     * @return True if request is complete, false otherwise
     */
    public synchronized boolean isComplete(int requestId) {
        if(!operations.containsKey(requestId))
            throw new VoldemortException("No operation with id " + requestId + " found");

        if(operations.get(requestId).getStatus().isComplete()) {
            logger.debug("Operation complete " + requestId);
            operations.remove(requestId);

            return true;
        }
        return false;
    }

    // Wrap getOperationStatus to avoid throwing exception over JMX
    @JmxOperation(description = "Retrieve operation status")
    public String getStatus(int id) {
        try {
            return getOperationStatus(id).toString();
        } catch(VoldemortException e) {
            return "No operation with id " + id + " found";
        }
    }

    @JmxOperation(description = "Retrieve all operations")
    public String getAllAsyncOperations() {
        String result;
        try {
            result = operations.toString();
        } catch(Exception e) {
            result = e.getMessage();
        }
        return result;
    }

    public List<Integer> getAsyncOperationList(boolean showComplete) {
        /**
         * Create a copy to avoid a concurrent modification exception
         */
        Set<Integer> keySet = ImmutableSet.copyOf(operations.keySet());

        if (showComplete)
            return new ArrayList<Integer>(keySet);

        List<Integer> keyList = new ArrayList<Integer>();
        for (int key: keySet) {
            if (!operations.get(key).getStatus().isComplete())
                keyList.add(key);
        }
        return keyList;
    }

    public AsyncOperationStatus getOperationStatus(int requestId) {
        if(!operations.containsKey(requestId))
            throw new VoldemortException("No operation with id " + requestId + " found");

        return operations.get(requestId).getStatus();
    }

    // Wrapper to avoid throwing an exception over JMX
    @JmxOperation
    public String stopAsyncOperation(int requestId) {
        try {
            stopOperation(requestId);
        } catch (VoldemortException e) {
            return e.getMessage();
        }

        return "Stopping operation " + requestId;
    }

    public void stopOperation(int requestId) {
        if(!operations.containsKey(requestId))
            throw new VoldemortException("No operation with id " + requestId + " found");

        operations.get(requestId).stop();
    }

    public int getUniqueRequestId() {
        return lastOperationId.getAndIncrement();
    }

    @Override
    protected void startInner() {
        logger.info("Starting asyncOperationRunner");
    }

    @Override
    protected void stopInner() {
        logger.info("Stopping asyncOperationRunner");
    }
}

/*
 * Copyright 2008-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.protocol.admin;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;

/**
 * Asynchronous job scheduler for admin service operations.
 *
 * TODO: requesting a unique id, then creating an operation with that id seems like a bad API design.
 *
 */
@JmxManaged(description = "Asynchronous operation execution")
public class AsyncOperationService extends AbstractService {

    private final Map<Integer, AsyncOperation> operations;
    private final AtomicInteger lastOperationId = new AtomicInteger(0);
    private final SchedulerService scheduler;

    private final static Logger logger = Logger.getLogger(AsyncOperationService.class);

    public AsyncOperationService(SchedulerService scheduler, int cacheSize) {
        super(ServiceType.ASYNC_SCHEDULER);
        operations = Collections.synchronizedMap(new AsyncOperationCache(cacheSize));
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

    /**
     * Get list of asynchronous operations on this node. By default, only the pending
     * operations are returned.
     * @param showCompleted Show completed operations
     * @return A list of operation ids.
     */
    public List<Integer> getAsyncOperationList(boolean showCompleted) {
        /**
         * Create a copy using an immutable set to avoid a {@link java.util.ConcurrentModificationException}
         */
        Set<Integer> keySet = ImmutableSet.copyOf(operations.keySet());

        if (showCompleted)
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

    /**
     * Generate a unique request id
     * @return A new, guaranteed unique, request id
     */
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

package voldemort.store.routed.action;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.cluster.Node;
import voldemort.store.routed.Response;
import voldemort.utils.ByteArray;

/**
 * The AsyncPutSynchronizer Class is used for synchronizing operations inside
 * PerformParallelPut action More specifically, it coordinate the exception
 * handling and hinted handoff responsibility between master thread and async
 * put threads
 * 
 */
public class AsyncPutSynchronizer {

    private final static Logger logger = Logger.getLogger(AsyncPutSynchronizer.class);
    private boolean asyncCallbackShouldSendhint;
    private boolean responseHandlingCutoff;
    private final ConcurrentMap<Node, Boolean> slopDestinations; // the value in
                                                                 // the map is
                                                                 // not used
    private final Queue<Response<ByteArray, Object>> responseQueue;

    public AsyncPutSynchronizer() {
        asyncCallbackShouldSendhint = false;
        responseHandlingCutoff = false;
        slopDestinations = new ConcurrentHashMap<Node, Boolean>();
        responseQueue = new LinkedList<Response<ByteArray, Object>>();
    }

    /**
     * Get list of nodes to register slop for
     * 
     * @return list of nodes to register slop for
     */
    public synchronized Set<Node> getDelegatedSlopDestinations() {
        return Collections.unmodifiableSet(slopDestinations.keySet());
    }

    /**
     * Stop accepting delegated slop responsibility by master
     */
    public synchronized void disallowDelegateSlop() {
        asyncCallbackShouldSendhint = true;
    }

    /**
     * Try to delegate the responsibility of sending slops to master
     * 
     * @param node The node that slop should eventually be pushed to
     * @return true if master accept the responsibility; false if master does
     *         not accept
     */
    public synchronized boolean tryDelegateSlop(Node node) {
        if(asyncCallbackShouldSendhint) {
            return false;
        } else {
            slopDestinations.put(node, true);
            return true;
        }
    }

    /**
     * Master Stop accepting new responses (from async callbacks)
     */
    public synchronized void cutoffHandling() {
        responseHandlingCutoff = true;
    }

    /**
     * try to delegate the master to handle the response
     * 
     * @param response
     * @return true if the master accepted the response; false if the master
     *         didn't accept
     */
    public synchronized boolean tryDelegateResponseHandling(Response<ByteArray, Object> response) {
        if(responseHandlingCutoff) {
            return false;
        } else {
            responseQueue.offer(response);
            this.notifyAll();
            return true;
        }
    }

    /**
     * poll the response queue for response
     * 
     * @param timeout timeout amount
     * @param timeUnit timeUnit of timeout
     * @return same result of BlockQueue.poll(long, TimeUnit)
     * @throws InterruptedException
     */
    public synchronized Response<ByteArray, Object> responseQueuePoll(long timeout,
                                                                      TimeUnit timeUnit)
            throws InterruptedException {
        long timeoutMs = timeUnit.toMillis(timeout);
        long timeoutWallClockMs = System.currentTimeMillis() + timeoutMs;
        while(responseQueue.isEmpty() && System.currentTimeMillis() < timeoutWallClockMs) {
            long remainingMs = Math.max(0, timeoutWallClockMs - System.currentTimeMillis());
            if(logger.isDebugEnabled()) {
                logger.debug("Start waiting for response queue with timeoutMs: " + timeoutMs);
            }
            this.wait(remainingMs);
            if(logger.isDebugEnabled()) {
                logger.debug("End waiting for response queue with timeoutMs: " + timeoutMs);
            }
        }
        return responseQueue.poll();
    }

    /**
     * to see if the response queue is empty
     * 
     * @return true is response queue is empty; false if not empty.
     */
    public synchronized boolean responseQueueIsEmpty() {
        return responseQueue.isEmpty();
    }
}

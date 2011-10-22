package voldemort.server.rebalance.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class DonorBasedRebalancePusherSlave extends AsyncOperation {

    protected final static Logger logger = Logger.getLogger(DonorBasedRebalancePusherSlave.class);

    private int nodeId;
    private BlockingQueue<Pair<ByteArray, Versioned<byte[]>>> queue;
    private String storeName;
    private AdminClient adminClient;
    private ResumableIterator<Pair<ByteArray, Versioned<byte[]>>> nodeIterator = new ResumableIterator<Pair<ByteArray, Versioned<byte[]>>>();
    private AtomicBoolean stopRequested;

    public DonorBasedRebalancePusherSlave(int id,
                                          String description,
                                          int nodeId,
                                          BlockingQueue<Pair<ByteArray, Versioned<byte[]>>> queue,
                                          String storeName,
                                          AdminClient adminClient) {
        super(id, description);
        this.nodeId = nodeId;
        this.queue = queue;
        this.storeName = storeName;
        this.adminClient = adminClient;
        this.stopRequested = new AtomicBoolean(false);
    }

    @Override
    public void operate() throws Exception {
        logger.info("DonorBasedRebalancePusherSlave begains to send partitions for store "
                    + storeName + " to node " + nodeId);
        while(!isStopRequest()) {
            try {
                adminClient.updateEntries(nodeId, storeName, nodeIterator, null);
                nodeIterator.purge();
            } catch(VoldemortException e) {
                if(e.getCause() instanceof IOException) {
                    nodeIterator.setRecoveryMode();
                    // we terminated due to remote error, keep retrying after
                    // sleeping for a bit
                    logger.error("Exception received while pushing entries for store " + storeName
                                 + " to remote node " + nodeId
                                 + ". Will retry again after 5 minutes");
                    logger.error(e.getCause());
                    Thread.sleep(30000);
                } else {
                    throw e;
                }
            }
        }
        setCompletion();
        logger.info("DonorBasedRebalancePusherSlave finished sending partitions for store "
                    + storeName + " to node " + nodeId);
    }

    public void setStopRequest() {
        stopRequested.set(true);
    }

    public boolean isStopRequest() {
        return stopRequested.get();
    }

    @Override
    public void stop() {
        requestCompletion();
    }

    /**
     * This function will set the request for stop first; Then insert 'END' into
     * the queue so slave will return from updateEntries. Noted that this order
     * shall not be changed or the slave will enter updateEntries again.
     * 
     * @param immediateTerminate
     * @param notifySlave
     */
    public synchronized void requestCompletion() {
        try {
            setStopRequest();
            queue.put(DonorBasedRebalanceAsyncOperation.END);
        } catch(InterruptedException e) {
            logger.info("Unable to send termination message to pusher slave for node " + nodeId
                        + " due to the following reason: " + e.getMessage());
        }
    }

    public synchronized void setCompletion() {
        getStatus().setComplete(true);
        notifyAll();
    }

    public synchronized void waitCompletion() {
        while(!getStatus().isComplete()) {
            try {
                logger.info("Waiting for the completion, with 10s timeout, of pusher slave for "
                            + getStatus().getDescription() + " with id=" + getStatus().getId());
                // check for status every 10 seconds
                wait(10000);
            } catch(InterruptedException e) {
                logger.info("Existing wait loop due to interrupt.");
                break;
            }
        }
    }

    // It will always Iterator through 'tentativeList' before iterating 'queue'
    class ResumableIterator<T> implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private boolean recoveryModeOn = false;
        private int recoveryPosition = 0;
        private Pair<ByteArray, Versioned<byte[]>> currentElem = null;
        private ArrayList<Pair<ByteArray, Versioned<byte[]>>> tentativeList = Lists.newArrayList();

        public void close() {}

        public void setRecoveryMode() {
            // won't need to be in recovery mode if nothing to recover
            if(tentativeList.size() > 0) {
                recoveryModeOn = true;
                recoveryPosition = 0;
            }
        }

        // only purge if we are NOT in recovery mode
        public void purge() {
            if(!recoveryModeOn) {
                logger.info("DonorBasedRebalancePusherSlave successfully sent "
                            + tentativeList.size() + " entries to node " + nodeId + "for store "
                            + storeName);
                tentativeList.clear();
            } else {
                logger.error("purge called while recovery mode is on!!!!!");
            }
        }

        // return when something is available, blocked otherwise
        public boolean hasNext() {
            boolean hasNext = false;
            if(null == currentElem) {
                try {
                    currentElem = getNextElem();
                } catch(InterruptedException e) {
                    logger.info("hasNext is interrupted while waiting for the next elem.");
                }
            }
            if(null != currentElem && !currentElem.equals(DonorBasedRebalanceAsyncOperation.END)) {
                hasNext = true;
            }
            return hasNext;
        }

        // return the element when one or more is available, blocked
        // otherwise
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(null == currentElem) {
                try {
                    currentElem = getNextElem();
                } catch(InterruptedException e) {
                    logger.info("next is interrupted while waiting for the next elem.");
                }
                if(null == currentElem || currentElem.equals(DonorBasedRebalanceAsyncOperation.END)) {
                    throw new NoSuchElementException();
                }
            }
            Pair<ByteArray, Versioned<byte[]>> returnValue = currentElem;
            currentElem = null;
            return returnValue;
        }

        // if we are in recovery mode, return the element pointed by the
        // recoveryPosition if not, return the next element from the queue.
        private Pair<ByteArray, Versioned<byte[]>> getNextElem() throws InterruptedException {
            Pair<ByteArray, Versioned<byte[]>> retValue = null;
            if(recoveryModeOn) {
                retValue = tentativeList.get(recoveryPosition);
                recoveryPosition++;
                if(recoveryPosition >= tentativeList.size()) {
                    // recovery is done
                    recoveryModeOn = false;
                }

                // some verification checks
                if(retValue == null) {
                    logger.error("No elements found in the recovery list while in the recovery mode!\n"
                                 + "  recovery list size: "
                                 + tentativeList.size()
                                 + "  recovery position: " + recoveryPosition);
                }
            } else {
                retValue = queue.take();
                tentativeList.add(retValue);
            }
            return retValue;
        }

        public void remove() {
            throw new VoldemortException("Remove not supported");
        }

    }
}

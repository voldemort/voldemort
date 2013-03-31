/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.server.rebalance.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class DonorBasedRebalancePusherSlave implements Runnable {

    protected final static Logger logger = Logger.getLogger(DonorBasedRebalancePusherSlave.class);

    private int nodeId;
    private BlockingQueue<Pair<ByteArray, Versioned<byte[]>>> queue;
    private String storeName;
    private AdminClient adminClient;
    private ResumableIterator<Pair<ByteArray, Versioned<byte[]>>> nodeIterator;

    public DonorBasedRebalancePusherSlave(int nodeId,
                                          BlockingQueue<Pair<ByteArray, Versioned<byte[]>>> queue,
                                          String storeName,
                                          AdminClient adminClient) {
        this.nodeId = nodeId;
        this.queue = queue;
        this.storeName = storeName;
        this.adminClient = adminClient;
        nodeIterator = new ResumableIterator<Pair<ByteArray, Versioned<byte[]>>>();
    }

    public void run() throws VoldemortException {
        boolean needWait = false;
        logger.info("DonorBasedRebalancePusherSlave begains to send partitions for store "
                    + storeName + " to node " + nodeId);
        while(!nodeIterator.done) {
            try {
                nodeIterator.reset();
                adminClient.streamingOps.updateEntries(nodeId, storeName, nodeIterator, null);
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
                    needWait = true;
                } else {
                    throw e;
                }
            }

            if(needWait) {
                try {
                    // sleep for 5 minutes if exception occur while communicate
                    // with remote node
                    logger.info("waiting for 5 minutes for the remote node to recover");
                    Thread.sleep(30000);
                    needWait = false;
                } catch(InterruptedException e) {
                    // continue
                }
            }
        }

        logger.info("DonorBasedRebalancePusherSlave finished sending partitions for store "
                    + storeName + " to node " + nodeId);
    }

    /**
     * This function inserts 'END' into the queue so slave will return from
     * updateEntries.
     * 
     * @param immediateTerminate
     * @param notifySlave
     */
    public void requestCompletion() {
        try {
            queue.put(DonorBasedRebalanceAsyncOperation.END);
        } catch(InterruptedException e) {
            logger.info("Unable to send termination message to pusher slave for node " + nodeId
                        + " due to the following reason: " + e.getMessage());
        }
    }

    // It will always Iterator through 'tentativeList' before iterating 'queue'
    class ResumableIterator<T> implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private boolean done = false;
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
                tentativeList.clear();
            } else {
                logger.error("purge called while recovery mode is on!!!!!");
            }
        }

        public void reset() {
            this.currentElem = null;
        }

        public boolean hasNext() {
            boolean hasNext = false;
            if(!done) {
                while(null == currentElem) {
                    try {
                        currentElem = getNextElem();
                    } catch(InterruptedException e) {
                        logger.info("hasNext is interrupted while waiting for the next elem, existing...");
                        break;
                    }
                }

                // regular event
                if(null != currentElem
                   && !currentElem.equals(DonorBasedRebalanceAsyncOperation.END)
                   && !currentElem.equals(DonorBasedRebalanceAsyncOperation.BREAK)) {
                    hasNext = true;
                }

                // this is the last element returned by this iterator
                if(currentElem != null && currentElem.equals(DonorBasedRebalanceAsyncOperation.END)) {
                    done = true;
                    hasNext = false;
                }
            }
            return hasNext;
        }

        // return the element when one or more is available, blocked
        // otherwise
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(done) {
                throw new NoSuchElementException();
            }

            while(null == currentElem) {
                try {
                    currentElem = getNextElem();
                } catch(InterruptedException e) {
                    logger.info("next is interrupted while waiting for the next elem, existing...");
                    break;
                }
                if(null == currentElem || currentElem.equals(DonorBasedRebalanceAsyncOperation.END)
                   || currentElem.equals(DonorBasedRebalanceAsyncOperation.BREAK)) {
                    throw new NoSuchElementException();
                }
            }

            // this is the last element returned by this iterator
            if(currentElem != null && currentElem.equals(DonorBasedRebalanceAsyncOperation.END)) {
                done = true;
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

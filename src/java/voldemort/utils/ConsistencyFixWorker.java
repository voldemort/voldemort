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

package voldemort.utils;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.store.routed.NodeValue;
import voldemort.utils.ConsistencyFix.BadKey;
import voldemort.utils.ConsistencyFix.BadKeyStatus;
import voldemort.utils.ConsistencyFix.Status;
import voldemort.versioning.ObsoleteVersionException;

class ConsistencyFixWorker extends AbstractConsistencyFixer implements Runnable {

    private static final Logger logger = Logger.getLogger(ConsistencyFixWorker.class);

    private final ConsistencyFix consistencyFix;
    private final BlockingQueue<BadKeyStatus> badKeyQOut;

    /**
     * Normal use case constructor.
     * 
     * @param keyInHexFormat
     * @param consistencyFix
     * @param badKeyQOut
     */
    ConsistencyFixWorker(BadKey badKey,
                         ConsistencyFix consistencyFix,
                         BlockingQueue<BadKeyStatus> badKeyQOut) {
        this(badKey, consistencyFix, badKeyQOut, null);
    }

    /**
     * Constructor for "orphaned values" use case. I.e., there are values for
     * the specific key that exist somewhere and may need to be written to the
     * nodes which actually host the key.
     * 
     * @param keyInHexFormat
     * @param consistencyFix
     * @param badKeyQOut
     * @param orphanedValues Set to null if no orphaned values to be included.
     */
    ConsistencyFixWorker(BadKey badKey,
                         ConsistencyFix consistencyFix,
                         BlockingQueue<BadKeyStatus> badKeyQOut,
                         QueryKeyResult orphanedValues) {
        super(badKey,
              consistencyFix.getStoreInstance(),
              consistencyFix.getAdminClient(),
              orphanedValues);
        this.consistencyFix = consistencyFix;
        this.badKeyQOut = badKeyQOut;
    }

    private String myName() {
        return Thread.currentThread().getName() + "-" + ConsistencyFixWorker.class.getName();
    }

    @Override
    public void run() {
        logger.trace("About to process key " + badKey + " (" + myName() + ")");
        Status status = doConsistencyFix();
        logger.trace("Finished processing key " + badKey + " (" + myName() + ")");
        consistencyFix.getStats().incrementFixCount();

        if(status != Status.SUCCESS) {
            try {
                badKeyQOut.put(new BadKeyStatus(badKey, status));
            } catch(InterruptedException ie) {
                logger.warn("Worker thread " + myName() + " interrupted.");
            }
            consistencyFix.getStats().incrementFailures(status);
        }
    }

    /**
     * 
     * @param toReadRepair Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
    @Override
    public Status doRepairPut(final List<NodeValue<ByteArray, byte[]>> toReadRepair) {
        if(this.consistencyFix.isDryRun()) {
            logger.debug("Returning success from ConsistencyFixWorker.doRepairPut because this is a dry run.");
            return Status.SUCCESS;
        }

        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            try {
                consistencyFix.maybePutThrottle(nodeKeyValue.getNodeId());
                consistencyFix.getAdminClient().storeOps.putNodeKeyValue(consistencyFix.getStoreName(),
                                                                         nodeKeyValue);
                consistencyFix.getStats().incrementPutCount();
            } catch(ObsoleteVersionException ove) {
                // Treat OVE as success.
                consistencyFix.getStats().incrementObsoleteVersionExceptions();
            } catch(VoldemortException ve) {
                allRepairsSuccessful = false;
                logger.debug("Repair of key " + nodeKeyValue.getKey() + "on node with id "
                             + nodeKeyValue.getNodeId() + " for version "
                             + nodeKeyValue.getVersion() + " failed because of exception : "
                             + ve.getMessage());
            }
        }
        if(!allRepairsSuccessful) {
            logger.info("Aborting fixKey because exceptions were encountered when repairing key-values.");
            return Status.REPAIR_EXCEPTION;
        }
        return Status.SUCCESS;
    }
}
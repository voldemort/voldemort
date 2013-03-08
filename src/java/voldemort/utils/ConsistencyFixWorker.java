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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.ReadRepairer;
import voldemort.utils.ConsistencyFix.BadKey;
import voldemort.utils.ConsistencyFix.BadKeyStatus;
import voldemort.utils.ConsistencyFix.Status;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

class ConsistencyFixWorker implements Runnable {

    private static final Logger logger = Logger.getLogger(ConsistencyFixWorker.class);
    private static final int fakeNodeID = Integer.MIN_VALUE;

    private final BadKey badKey;
    private final ConsistencyFix consistencyFix;
    private final BlockingQueue<BadKeyStatus> badKeyQOut;
    private final QueryKeyResult orphanedValues;

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
        this.badKey = badKey;
        this.consistencyFix = consistencyFix;
        this.badKeyQOut = badKeyQOut;
        this.orphanedValues = orphanedValues;
    }

    private String myName() {
        return Thread.currentThread().getName() + "-" + ConsistencyFixWorker.class.getName();
    }

    @Override
    public void run() {
        logger.trace("About to process key " + badKey + " (" + myName() + ")");
        Status status = doConsistencyFix(badKey);
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

    public Status doConsistencyFix(BadKey badKey) {
        // Initialization.
        byte[] keyInBytes;
        List<Integer> nodeIdList = null;
        int masterPartitionId = -1;
        try {
            keyInBytes = ByteUtils.fromHexString(badKey.getKeyInHexFormat());
            masterPartitionId = consistencyFix.getStoreInstance().getMasterPartitionId(keyInBytes);
            nodeIdList = consistencyFix.getStoreInstance()
                                       .getReplicationNodeList(masterPartitionId);
        } catch(Exception exception) {
            logger.info("Aborting fixKey due to bad init.");
            if(logger.isDebugEnabled()) {
                exception.printStackTrace();
            }
            return Status.BAD_INIT;
        }
        ByteArray keyAsByteArray = new ByteArray(keyInBytes);

        // Do the reads
        Map<Integer, QueryKeyResult> nodeIdToKeyValues = doReads(nodeIdList,
                                                                 keyInBytes,
                                                                 badKey.getKeyInHexFormat());

        // Process read replies (i.e., nodeIdToKeyValues)
        ProcessReadRepliesResult result = processReadReplies(nodeIdList,
                                                             keyAsByteArray,
                                                             badKey.getKeyInHexFormat(),
                                                             nodeIdToKeyValues);
        if(result.status != Status.SUCCESS) {
            return result.status;
        }

        // Resolve conflicts indicated in nodeValues
        List<NodeValue<ByteArray, byte[]>> toReadRepair = resolveReadConflicts(result.nodeValues);
        if(logger.isTraceEnabled()) {
            if(toReadRepair.size() == 0) {
                logger.trace("Nothing to repair");
            }
            for(NodeValue<ByteArray, byte[]> nodeValue: toReadRepair) {
                logger.trace(nodeValue.getNodeId() + " --- " + nodeValue.getKey().toString());
            }
        }

        // Do the repairs
        Status status = doRepairPut(toReadRepair);

        // return status of last operation (success or otherwise)
        return status;
    }

    /**
     * 
     * @param nodeIdList
     * @param keyInBytes
     * @param keyInHexFormat
     * @return
     */
    private Map<Integer, QueryKeyResult> doReads(final List<Integer> nodeIdList,
                                                 final byte[] keyInBytes,
                                                 final String keyInHexFormat) {
        Map<Integer, QueryKeyResult> nodeIdToKeyValues = new HashMap<Integer, QueryKeyResult>();

        ByteArray key = new ByteArray(keyInBytes);
        for(int nodeId: nodeIdList) {
            List<Versioned<byte[]>> values = null;
            try {
                values = consistencyFix.getAdminClient().storeOps.getNodeKey(consistencyFix.getStoreName(),
                                                                             nodeId,
                                                                             key);
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, values));
            } catch(VoldemortException ve) {
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, ve));
            }
        }

        return nodeIdToKeyValues;
    }

    /**
     * Result of an invocation of processReadReplies
     */
    private class ProcessReadRepliesResult {

        public final Status status;
        public final List<NodeValue<ByteArray, byte[]>> nodeValues;

        /**
         * Constructor for error status
         */
        ProcessReadRepliesResult(Status status) {
            this.status = status;
            this.nodeValues = null;
        }

        /**
         * Constructor for success
         */
        ProcessReadRepliesResult(List<NodeValue<ByteArray, byte[]>> nodeValues) {
            this.status = Status.SUCCESS;
            this.nodeValues = nodeValues;
        }
    }

    /**
     * 
     * @param nodeIdList
     * @param keyAsByteArray
     * @param keyInHexFormat
     * @param nodeIdToKeyValues
     * @param nodeValues Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
    private ProcessReadRepliesResult processReadReplies(final List<Integer> nodeIdList,
                                                        final ByteArray keyAsByteArray,
                                                        final String keyInHexFormat,
                                                        final Map<Integer, QueryKeyResult> nodeIdToKeyValues) {
        List<NodeValue<ByteArray, byte[]>> nodeValues = new ArrayList<NodeValue<ByteArray, byte[]>>();
        boolean exceptionsEncountered = false;
        for(int nodeId: nodeIdList) {
            QueryKeyResult keyValue;
            if(nodeIdToKeyValues.containsKey(nodeId)) {
                keyValue = nodeIdToKeyValues.get(nodeId);

                if(keyValue.hasException()) {
                    logger.debug("Exception encountered while fetching key " + keyInHexFormat
                                 + " from node with nodeId " + nodeId + " : "
                                 + keyValue.getException().getMessage());
                    exceptionsEncountered = true;
                } else {
                    if(keyValue.getValues().isEmpty()) {
                        Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                        nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                        keyValue.getKey(),
                                                                        versioned));

                    } else {
                        for(Versioned<byte[]> value: keyValue.getValues()) {
                            nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                            keyValue.getKey(),
                                                                            value));
                        }
                    }
                }
            } else {
                logger.debug("No key-value returned from node with id:" + nodeId);
                Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId, keyAsByteArray, versioned));
            }
        }
        if(exceptionsEncountered) {
            logger.info("Aborting fixKey because exceptions were encountered when fetching key-values.");
            return new ProcessReadRepliesResult(Status.FETCH_EXCEPTION);
        }

        if(logger.isDebugEnabled()) {
            for(NodeValue<ByteArray, byte[]> nkv: nodeValues) {
                logger.debug("\tRead NodeKeyValue : " + ByteUtils.toHexString(nkv.getKey().get())
                             + " on node with id " + nkv.getNodeId() + " for version "
                             + nkv.getVersion());
            }
        }

        return new ProcessReadRepliesResult(nodeValues);
    }

    /**
     * Decide on the specific key-value to write everywhere.
     * 
     * @param nodeValues
     * @return The subset of entries from nodeValues that need to be repaired.
     */
    private List<NodeValue<ByteArray, byte[]>> resolveReadConflicts(final List<NodeValue<ByteArray, byte[]>> nodeValues) {

        if(logger.isTraceEnabled()) {
            logger.trace("NodeValues passed into resolveReadConflicts.");
            if(nodeValues.size() == 0) {
                logger.trace("Empty nodeValues passed to resolveReadConflicts");
            }
            for(NodeValue<ByteArray, byte[]> nodeValue: nodeValues) {
                logger.trace("\t" + nodeValue.getNodeId() + " - " + nodeValue.getKey().toString()
                             + " - " + nodeValue.getVersion().toString());
            }
        }

        // If orphaned values exist, add them to fake nodes to be processed by
        // "getRepairs"
        int currentFakeNodeId = fakeNodeID;
        if(this.orphanedValues != null) {
            for(Versioned<byte[]> value: this.orphanedValues.getValues()) {
                nodeValues.add(new NodeValue<ByteArray, byte[]>(currentFakeNodeId,
                                                                this.orphanedValues.getKey(),
                                                                value));
                currentFakeNodeId++;
            }
        }

        // Some cut-paste-and-modify coding from
        // store/routed/action/AbstractReadRepair.java and
        // store/routed/ThreadPoolRoutedStore.java
        ReadRepairer<ByteArray, byte[]> readRepairer = new ReadRepairer<ByteArray, byte[]>();
        List<NodeValue<ByteArray, byte[]>> nodeKeyValues = readRepairer.getRepairs(nodeValues);

        if(logger.isTraceEnabled()) {
            if(nodeKeyValues.size() == 0) {
                logger.trace("\treadRepairer returned an empty list.");
            }
            for(NodeValue<ByteArray, byte[]> nodeKeyValue: nodeKeyValues) {
                logger.trace("\tNodeKeyValue result from readRepairer.getRepairs : "
                             + ByteUtils.toHexString(nodeKeyValue.getKey().get())
                             + " on node with id " + nodeKeyValue.getNodeId() + " for version "
                             + nodeKeyValue.getVersion());
            }
        }

        List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        for(NodeValue<ByteArray, byte[]> v: nodeKeyValues) {
            if(v.getNodeId() > currentFakeNodeId) {
                // Only copy repairs intended for real nodes.
                Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                              ((VectorClock) v.getVersion()).clone());
                toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(),
                                                                  v.getKey(),
                                                                  versioned));
            } else {
                if(logger.isDebugEnabled()) {
                    logger.debug("\tIgnoring repair to fake node: "
                                 + ByteUtils.toHexString(v.getKey().get()) + " on node with id "
                                 + v.getNodeId() + " for version " + v.getVersion());
                }
            }

        }

        if(logger.isTraceEnabled()) {
            if(toReadRepair.size() == 0) {
                logger.trace("\ttoReadRepair is empty.");
            }
            for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
                logger.trace("\tRepair key " + ByteUtils.toHexString(nodeKeyValue.getKey().get())
                             + " on node with id " + nodeKeyValue.getNodeId() + " for version "
                             + nodeKeyValue.getVersion());

            }
        }
        return toReadRepair;
    }

    /**
     * 
     * @param toReadRepair Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
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
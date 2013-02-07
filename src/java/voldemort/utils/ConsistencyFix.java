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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.routed.NodeValue;
import voldemort.store.routed.ReadRepairer;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class ConsistencyFix {

    private final String storeName;
    private final AdminClient adminClient;
    private final StoreInstance storeInstance;

    ConsistencyFix(String url, String storeName) {
        this.storeName = storeName;
        System.out.println("Connecting to bootstrap server: " + url);
        this.adminClient = new AdminClient(url, new AdminClientConfig(), 0);
        Cluster cluster = adminClient.getAdminClientCluster();
        System.out.println("Cluster determined to be: " + cluster.getName());

        System.out.println("Determining store definition for store: " + storeName);
        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        List<StoreDefinition> storeDefs = storeDefinitions.getValue();
        StoreDefinition storeDefinition = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                          storeName);
        System.out.println("Store definition determined.");

        storeInstance = new StoreInstance(cluster, storeDefinition);
    }

    public void stop() {
        adminClient.stop();
    }

    /**
     * Status of the repair of a specific "bad key"
     */
    public enum Status {
        SUCCESS("success"),
        BAD_INIT("bad initialization of fix key"),
        FETCH_EXCEPTION("exception during fetch"),
        REPAIR_EXCEPTION("exception during repair");

        private final String name;

        private Status(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Type with which to wrap "bad keys" read from input file. Has a "poison"
     * value to effectively signal EOF.
     */
    public class BadKeyInput {

        private final String keyInHexFormat;
        private final boolean poison;

        /**
         * Common case constructor.
         */
        BadKeyInput(String keyInHexFormat) {
            this.keyInHexFormat = keyInHexFormat;
            this.poison = false;
        }

        /**
         * Constructs a "poison" object.
         */
        BadKeyInput() {
            this.keyInHexFormat = null;
            this.poison = true;
        }

        boolean isPoison() {
            return poison;
        }

        String getKey() {
            return keyInHexFormat;
        }
    }

    /**
     * Type with which to wrap a "bad key" that could not be repaired and so
     * needs to be written to output file. Has a "poison" value to effectively
     * signal end-of-stream.
     */
    public class BadKeyResult {

        private final String keyInHexFormat;
        private final Status fixKeyResult;
        private final boolean poison;

        /**
         * Common case constructor.
         */
        BadKeyResult(String keyInHexFormat, Status fixKeyResult) {
            this.keyInHexFormat = keyInHexFormat;
            this.fixKeyResult = fixKeyResult;
            this.poison = false;
        }

        /**
         * Constructs a "poison" object.
         */
        BadKeyResult() {
            this.keyInHexFormat = null;
            this.fixKeyResult = null;
            this.poison = true;
        }

        public boolean isPoison() {
            return poison;
        }

        public String getKey() {
            return keyInHexFormat;
        }

        public Status getResult() {
            return fixKeyResult;
        }
    }

    // TODO: Should either move BadKeyReader and BadKeyWriter thread definitions
    // out of this file (as has been done with ConsistencyFixKeyGetter and
    // ConsistencyFixRepairPutter), or move those thread definitions (back) into
    // this file.
    class BadKeyReader implements Runnable {

        private final String badKeyFileIn;
        private final BlockingQueue<BadKeyInput> badKeyQIn;
        private BufferedReader fileReader;

        BadKeyReader(String badKeyFileIn, BlockingQueue<BadKeyInput> badKeyQIn) {
            this.badKeyFileIn = badKeyFileIn;
            this.badKeyQIn = badKeyQIn;
            try {
                fileReader = new BufferedReader(new FileReader(badKeyFileIn));
            } catch(IOException e) {
                Utils.croak("Failure to open input stream: " + e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                int counter = 0;
                for(String line = fileReader.readLine(); line != null; line = fileReader.readLine()) {
                    if(!line.isEmpty()) {
                        counter++;
                        System.out.println("BadKeyReader read line: key (" + line
                                           + ") and counter (" + counter + ")");
                        badKeyQIn.put(new BadKeyInput(line));
                    }
                }
                System.out.println("BadKeyReader poisoning the pipeline");
                badKeyQIn.put(new BadKeyInput());
            } catch(IOException ioe) {
                System.err.println("IO exception reading badKeyFile " + badKeyFileIn + " : "
                                   + ioe.getMessage());
            } catch(InterruptedException ie) {
                System.err.println("Interrupted exception during reading of badKeyFile "
                                   + badKeyFileIn + " : " + ie.getMessage());
            } finally {
                try {
                    fileReader.close();
                } catch(IOException ioe) {
                    System.err.println("IOException during fileReader.close in BadKeyReader thread.");
                }
            }
            System.out.println("BadKeyReader is done.");
        }
    }

    class BadKeyWriter implements Runnable {

        private final String badKeyFileOut;
        private final BlockingQueue<BadKeyResult> badKeyQOut;

        private BufferedWriter fileWriter = null;

        BadKeyWriter(String badKeyFile, BlockingQueue<BadKeyResult> badKeyQOut) {
            this.badKeyFileOut = badKeyFile;
            this.badKeyQOut = badKeyQOut;

            try {
                fileWriter = new BufferedWriter(new FileWriter(badKeyFileOut));
            } catch(IOException e) {
                Utils.croak("Failure to open output file : " + e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                BadKeyResult badKeyResult = badKeyQOut.take();
                while(!badKeyResult.isPoison()) {
                    System.out.println("BadKeyWriter write key (" + badKeyResult.keyInHexFormat);

                    fileWriter.write("BADKEY," + badKeyResult.keyInHexFormat + ","
                                     + badKeyResult.fixKeyResult.name() + "\n");
                    badKeyResult = badKeyQOut.take();
                }
            } catch(IOException ioe) {
                System.err.println("IO exception reading badKeyFile " + badKeyFileOut + " : "
                                   + ioe.getMessage());
            } catch(InterruptedException ie) {
                System.err.println("Interrupted exception during writing of badKeyFile "
                                   + badKeyFileOut + " : " + ie.getMessage());
            } finally {
                try {
                    fileWriter.close();
                } catch(IOException ioe) {
                    System.err.println("Interrupted exception during fileWriter.close:"
                                       + ioe.getMessage());
                }
            }
        }
    }

    // TODO: Make a type to handle Status + nodeIdToKeyValues so that
    // nodeIdToKeyValues is not an "out" parameter.
    /**
     * 
     * @param nodeIdList
     * @param keyInBytes
     * @param keyInHexFormat
     * @param verbose
     * @param nodeIdToKeyValues Effectively the output of this method. Must pass
     *        in a non-null object to be populated by this method.
     * @return
     */
    private ConsistencyFix.Status doRead(final List<Integer> nodeIdList,
                                         final byte[] keyInBytes,
                                         final String keyInHexFormat,
                                         boolean verbose,
                                         Map<Integer, QueryKeyResult> nodeIdToKeyValues) {
        if(nodeIdToKeyValues == null) {
            if(verbose) {
                System.out.println("Aborting doRead due to bad init.");
            }
            return Status.BAD_INIT;
        }

        if(verbose) {
            System.out.println("Reading key-values for specified key: " + keyInHexFormat);
        }
        ByteArray key = new ByteArray(keyInBytes);
        for(int nodeId: nodeIdList) {
            List<Versioned<byte[]>> values = null;
            try {
                values = adminClient.storeOps.getNodeKey(storeName, nodeId, key);
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, values));
            } catch(VoldemortException ve) {
                nodeIdToKeyValues.put(nodeId, new QueryKeyResult(key, ve));
            }
        }

        return Status.SUCCESS;
    }

    // TODO: Make a type to handle Status + nodeValues so that nodeValue is not
    // an "out" parameter.
    /**
     * 
     * @param nodeIdList
     * @param keyAsByteArray
     * @param keyInHexFormat
     * @param verbose
     * @param nodeIdToKeyValues
     * @param nodeValues Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @return
     */
    private ConsistencyFix.Status processReadReplies(final List<Integer> nodeIdList,
                                                     final ByteArray keyAsByteArray,
                                                     final String keyInHexFormat,
                                                     boolean verbose,
                                                     final Map<Integer, QueryKeyResult> nodeIdToKeyValues,
                                                     List<NodeValue<ByteArray, byte[]>> nodeValues) {
        if(nodeValues == null) {
            if(verbose) {
                System.out.println("Aborting processReadReplies due to bad init.");
            }
            return Status.BAD_INIT;
        }

        if(verbose) {
            System.out.println("Confirming all nodes (" + nodeIdList
                               + ") responded with key-values for specified key: " + keyInHexFormat);
        }
        boolean exceptionsEncountered = false;
        for(int nodeId: nodeIdList) {
            if(verbose) {
                System.out.println("\t Processing response from node with id:" + nodeId);
            }
            QueryKeyResult keyValue;
            if(nodeIdToKeyValues.containsKey(nodeId)) {
                if(verbose) {
                    System.out.println("\t... There was a key-value returned from node with id:"
                                       + nodeId);
                }
                keyValue = nodeIdToKeyValues.get(nodeId);

                if(keyValue.hasException()) {
                    if(verbose) {
                        System.out.println("\t... Exception encountered while fetching key "
                                           + keyInHexFormat + " from node with nodeId " + nodeId
                                           + " : " + keyValue.getException().getMessage());
                    }
                    exceptionsEncountered = true;
                } else {
                    if(keyValue.getValues().isEmpty()) {
                        if(verbose) {
                            System.out.println("\t... Adding null version to nodeValues");
                        }
                        Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                        nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                        keyValue.getKey(),
                                                                        versioned));

                    } else {
                        for(Versioned<byte[]> value: keyValue.getValues()) {
                            if(verbose) {
                                System.out.println("\t... Adding following version to nodeValues: "
                                                   + value.getVersion());
                            }
                            nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId,
                                                                            keyValue.getKey(),
                                                                            value));
                        }
                    }
                }
            } else {
                if(verbose) {
                    System.out.println("\t... No key-value returned from node with id:" + nodeId);
                    System.out.println("\t... Adding null version to nodeValues");
                }
                Versioned<byte[]> versioned = new Versioned<byte[]>(null);
                nodeValues.add(new NodeValue<ByteArray, byte[]>(nodeId, keyAsByteArray, versioned));
            }
        }
        if(exceptionsEncountered) {
            if(verbose) {
                System.out.println("Aborting fixKey because exceptions were encountered when fetching key-values.");
            }
            return Status.FETCH_EXCEPTION;
        }

        return Status.SUCCESS;
    }

    /**
     * Decide on the specific key-value to write everywhere.
     * 
     * @param verbose
     * @param nodeValues
     * @return The subset of entries from nodeValues that need to be repaired.
     */
    private List<NodeValue<ByteArray, byte[]>> resolveReadConflicts(boolean verbose,
                                                                    final List<NodeValue<ByteArray, byte[]>> nodeValues) {

        // Some cut-paste-and-modify coding from
        // store/routed/action/AbstractReadRepair.java and
        // store/routed/ThreadPoolRoutedStore.java
        if(verbose) {
            System.out.println("Resolving conflicts in responses.");
        }
        ReadRepairer<ByteArray, byte[]> readRepairer = new ReadRepairer<ByteArray, byte[]>();
        List<NodeValue<ByteArray, byte[]>> toReadRepair = Lists.newArrayList();
        for(NodeValue<ByteArray, byte[]> v: readRepairer.getRepairs(nodeValues)) {
            Versioned<byte[]> versioned = Versioned.value(v.getVersioned().getValue(),
                                                          ((VectorClock) v.getVersion()).clone());
            if(verbose) {
                System.out.println("\tAdding toReadRepair: key (" + v.getKey() + "), version ("
                                   + versioned.getVersion() + ")");
            }
            toReadRepair.add(new NodeValue<ByteArray, byte[]>(v.getNodeId(), v.getKey(), versioned));
        }

        if(verbose) {
            System.out.println("Repair work to be done:");
            for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
                System.out.println("\tRepair key " + nodeKeyValue.getKey() + "on node with id "
                                   + nodeKeyValue.getNodeId() + " for version "
                                   + nodeKeyValue.getVersion());
            }
        }
        return toReadRepair;
    }

    public class doKeyGetStatus {

        public final Status status;
        public final List<NodeValue<ByteArray, byte[]>> nodeValues;

        doKeyGetStatus(Status status, List<NodeValue<ByteArray, byte[]>> nodeValues) {
            this.status = status;
            this.nodeValues = nodeValues;
        }

        doKeyGetStatus(Status status) {
            this.status = status;
            this.nodeValues = null;
        }
    }

    public ConsistencyFix.doKeyGetStatus doKeyGet(String keyInHexFormat, boolean verbose) {
        if(verbose) {
            System.out.println("Performing consistency fix of key: " + keyInHexFormat);
        }

        // Initialization.
        byte[] keyInBytes;
        List<Integer> nodeIdList = null;
        int masterPartitionId = -1;
        try {
            keyInBytes = ByteUtils.fromHexString(keyInHexFormat);
            masterPartitionId = storeInstance.getMasterPartitionId(keyInBytes);
            nodeIdList = storeInstance.getReplicationNodeList(masterPartitionId);
        } catch(Exception exception) {
            if(verbose) {
                System.out.println("Aborting fixKey due to bad init.");
                exception.printStackTrace();
            }
            return new doKeyGetStatus(Status.BAD_INIT);
        }
        ByteArray keyAsByteArray = new ByteArray(keyInBytes);

        // Read
        Map<Integer, QueryKeyResult> nodeIdToKeyValues = new HashMap<Integer, QueryKeyResult>();
        Status fixKeyResult = doRead(nodeIdList,
                                     keyInBytes,
                                     keyInHexFormat,
                                     verbose,
                                     nodeIdToKeyValues);
        if(fixKeyResult != Status.SUCCESS) {
            return new doKeyGetStatus(fixKeyResult);
        }

        // Process read replies
        List<NodeValue<ByteArray, byte[]>> nodeValues = Lists.newArrayList();
        fixKeyResult = processReadReplies(nodeIdList,
                                          keyAsByteArray,
                                          keyInHexFormat,
                                          verbose,
                                          nodeIdToKeyValues,
                                          nodeValues);
        if(fixKeyResult != Status.SUCCESS) {
            return new doKeyGetStatus(fixKeyResult);
        }

        // Resolve conflicts
        List<NodeValue<ByteArray, byte[]>> toReadRepair = resolveReadConflicts(verbose, nodeValues);

        return new doKeyGetStatus(Status.SUCCESS, toReadRepair);
    }

    /**
     * 
     * @param toReadRepair Effectively the output of this method. Must pass in a
     *        non-null object to be populated by this method.
     * @param verbose
     * @param vInstance
     * @return
     */
    public Status doRepairPut(final List<NodeValue<ByteArray, byte[]>> toReadRepair, boolean verbose) {
        if(verbose) {
            System.out.println("Performing repair work:");
        }

        boolean allRepairsSuccessful = true;
        for(NodeValue<ByteArray, byte[]> nodeKeyValue: toReadRepair) {
            if(verbose) {
                System.out.println("\tDoing repair for node with id:" + nodeKeyValue.getNodeId());
            }
            try {
                adminClient.storeOps.putNodeKeyValue(storeName, nodeKeyValue);
            } catch(ObsoleteVersionException ove) {
                // NOOP. Treat OVE as success.
            } catch(VoldemortException ve) {
                allRepairsSuccessful = false;
                System.out.println("\t... Repair of key " + nodeKeyValue.getKey()
                                   + "on node with id " + nodeKeyValue.getNodeId()
                                   + " for version " + nodeKeyValue.getVersion()
                                   + " failed because of exception : " + ve.getMessage());
            }
        }
        if(!allRepairsSuccessful) {
            if(verbose) {
                System.err.println("Aborting fixKey because exceptions were encountered when reparing key-values.");
                System.out.println("Fix failed...");
            }
            return Status.REPAIR_EXCEPTION;
        }
        return Status.SUCCESS;
    }
}

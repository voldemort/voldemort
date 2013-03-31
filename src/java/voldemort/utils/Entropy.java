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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

// TODO: Remove from the code base.
// Entropy is replaced by KeySamplerCLI and KeyVersionFetcherCLI. Entropy
// never really worked as described and had a complicated interface.
@Deprecated
public class Entropy {

    private int nodeId;
    private long numKeys;
    private boolean verboseLogging;

    public static long DEFAULT_NUM_KEYS = 10000;

    /**
     * Entropy constructor. Uses DEFAULT_NUM_KEYS number of keys
     * 
     * @param nodeId Node id. If -1, goes over all of them. For negative test
     *        nodeId must be valid.
     */
    public Entropy(int nodeId) {
        this.nodeId = nodeId;
        this.numKeys = DEFAULT_NUM_KEYS;
        this.verboseLogging = false;
    }

    /**
     * Entropy constructor
     * 
     * @param nodeId Node id. If -1, goes over all of them. For negative test
     *        nodeId must be valid.
     * @param numKeys Number of keys
     */
    public Entropy(int nodeId, long numKeys, boolean verboseLogging) {
        this.nodeId = nodeId;
        this.numKeys = numKeys;
        this.verboseLogging = verboseLogging;
    }

    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("node", "Node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("cluster-xml", "[REQUIRED] Path to cluster-xml")
              .withRequiredArg()
              .describedAs("xml")
              .ofType(String.class);
        parser.accepts("stores-xml", "[REQUIRED] Path to stores-xml")
              .withRequiredArg()
              .describedAs("xml")
              .ofType(String.class);
        parser.accepts("output-dir",
                       "[REQUIRED] The output directory where we'll store / retrieve the keys. ")
              .withRequiredArg()
              .describedAs("output-dir")
              .ofType(String.class);
        parser.accepts("op-type",
                       "Operation type - false ( save keys ) [ default ], true ( run entropy calculator ) ")
              .withRequiredArg()
              .ofType(Boolean.class);
        parser.accepts("num-keys",
                       "Number of keys per store [ Default: " + Entropy.DEFAULT_NUM_KEYS + " ]")
              .withRequiredArg()
              .describedAs("keys")
              .ofType(Long.class);
        parser.accepts("negative-test",
                       "Check for keys that dont belong on the given nodeId are not present");
        parser.accepts("verbose-logging",
                       "Verbose logging such as keys found missing on specific nodes");

        OptionSet options = parser.parse(args);

        boolean negativeTest = false;
        if(options.has("negative-test")) {
            negativeTest = true;
        }

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "cluster-xml", "stores-xml", "output-dir");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        // compulsory params
        String clusterXml = (String) options.valueOf("cluster-xml");
        String storesXml = (String) options.valueOf("stores-xml");
        String outputDirPath = (String) options.valueOf("output-dir");
        long numKeys = CmdUtils.valueOf(options, "num-keys", Entropy.DEFAULT_NUM_KEYS);
        int nodeId = CmdUtils.valueOf(options, "node", 0);
        boolean opType = CmdUtils.valueOf(options, "op-type", false);
        boolean verboseLogging = options.has("verbose-logging");

        File outputDir = new File(outputDirPath);

        if(!outputDir.exists()) {
            outputDir.mkdirs();
        } else if(!(outputDir.isDirectory() && outputDir.canWrite())) {
            System.err.println("Cannot write to output directory " + outputDirPath);
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        if(!Utils.isReadableFile(clusterXml) || !Utils.isReadableFile(storesXml)) {
            System.err.println("Cannot read metadata file ");
            System.exit(1);
        }

        // Parse the metadata
        Cluster cluster = new ClusterMapper().readCluster(new File(clusterXml));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXml));

        Entropy detector = new Entropy(nodeId, numKeys, verboseLogging);

        detector.generateEntropy(cluster, storeDefs, outputDir, opType, negativeTest);
    }

    /**
     * Run the actual entropy calculation tool
     * 
     * @param cluster The cluster metadata
     * @param storeDefs The list of stores
     * @param storeDir The store directory
     * @param opType Operation type - true ( run entropy calculator ), false (
     *        save keys )
     * @throws IOException
     */
    public void generateEntropy(Cluster cluster,
                                List<StoreDefinition> storeDefs,
                                File storeDir,
                                boolean opType) throws IOException {
        generateEntropy(cluster, storeDefs, storeDir, opType, false);
    }

    /**
     * Run the actual entropy calculation tool
     * 
     * @param cluster The cluster metadata
     * @param storeDefs The list of stores
     * @param storeDir The store directory
     * @param opType Operation type - true ( run entropy calculator ), false (
     *        save keys )
     * @param negativeTest Validate that the rebalanced keys are deleted from
     *        the store
     * @throws IOException
     */
    public void generateEntropy(Cluster cluster,
                                List<StoreDefinition> storeDefs,
                                File storeDir,
                                boolean opType,
                                boolean negativeTest) throws IOException {
        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(cluster,
                                          new AdminClientConfig().setMaxConnectionsPerNode(storeDefs.size()),
                                          new ClientConfig());

            if(opType) {
                System.out.println("Running entropy calculator");
            } else {
                System.out.println("Generating keys for future entropy calculation");
                Utils.mkdirs(storeDir);
            }
            for(StoreDefinition storeDef: storeDefs) {

                File storesKeyFile = new File(storeDir, storeDef.getName());
                if(AdminClient.restoreStoreEngineBlackList.contains(storeDef.getType())) {
                    System.out.println("Ignoring store " + storeDef.getName());
                    continue;
                } else {
                    System.out.println("Working on store " + storeDef.getName());
                }

                RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                              cluster);

                if(!opType) {
                    if(storesKeyFile.exists()) {
                        System.err.println("Key files for " + storeDef.getName()
                                           + " already exists");
                        continue;
                    }
                    FileOutputStream writer = null;
                    try {
                        writer = new FileOutputStream(storesKeyFile);
                        Iterator<ByteArray> keys = null;
                        if(nodeId == -1) {

                            int numKeysPerNode = (int) Math.floor(numKeys
                                                                  / cluster.getNumberOfNodes());
                            int numKeysStored = 0;
                            for(Node node: cluster.getNodes()) {
                                System.out.println("Fetching " + numKeysPerNode
                                                   + " keys from node " + node.getHost());
                                keys = adminClient.bulkFetchOps.fetchKeys(node.getId(),
                                                                          storeDef.getName(),
                                                                          cluster.getNodeById(node.getId())
                                                                                 .getPartitionIds(),
                                                                          null,
                                                                          false,
                                                                          numKeysPerNode);
                                for(long keyId = 0; keyId < numKeysPerNode && keys.hasNext(); keyId++) {
                                    ByteArray key = keys.next();
                                    // entropy returns distinct keys from each
                                    // node - record the key only if this node
                                    // holds the primary partition of the key
                                    if(NodeUtils.getNodeIds(strategy.routeRequest(key.get())
                                                                    .subList(0, 1))
                                                .contains(node.getId())) {
                                        writer.write(key.length());
                                        writer.write(key.get());
                                        numKeysStored++;
                                    }
                                }
                            }
                            System.out.println("Fetched a total of  " + numKeysStored + " keys.");
                        } else {
                            List<Integer> partitions = cluster.getNodeById(nodeId)
                                                              .getPartitionIds();
                            Map<Integer, Integer> partitionMap = new HashMap<Integer, Integer>();
                            int numKeysPerPartition = (int) Math.floor(numKeys / partitions.size());
                            int numKeysStored = 0;

                            for(int partitionId: partitions) {
                                partitionMap.put(partitionId, 0);
                            }

                            keys = adminClient.bulkFetchOps.fetchKeys(nodeId,
                                                                      storeDef.getName(),
                                                                      partitions,
                                                                      null,
                                                                      false,
                                                                      numKeysPerPartition);
                            while(keys.hasNext() && numKeysStored < numKeys) {
                                ByteArray key = keys.next();
                                // entropy returns distinct keys from each
                                // node - record the key only if this node
                                // holds the primary partition of the key
                                if(NodeUtils.getNodeIds(strategy.routeRequest(key.get()).subList(0,
                                                                                                 1))
                                            .contains(nodeId)) {
                                    int targetPartition = strategy.getPartitionList(key.get())
                                                                  .get(0);
                                    int partitionCount = partitionMap.get(targetPartition);
                                    if(partitionCount == numKeysPerPartition)
                                        continue;
                                    writer.write(key.length());
                                    writer.write(key.get());
                                    partitionMap.put(targetPartition, partitionCount + 1);
                                    numKeysStored++;
                                }
                            }

                            System.out.println("Total partitions filled : " + partitions.size());
                            for(int partitionId: partitions) {
                                System.out.println("Count in partition #" + partitionId + " = "
                                                   + partitionMap.get(partitionId));
                            }

                        }

                    } finally {
                        if(writer != null)
                            writer.close();
                    }

                } else {

                    if(negativeTest && nodeId == -1)
                        return;

                    if(!(storesKeyFile.exists() && storesKeyFile.canRead())) {
                        System.err.println("Could not find " + storeDef.getName()
                                           + " file to check");
                        continue;
                    }
                    FileInputStream reader = null;
                    SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                          10000,
                                                                                          100000,
                                                                                          32 * 1024);

                    // Cache connections to all nodes for this store in advance
                    HashMap<Integer, Store<ByteArray, byte[], byte[]>> socketStoresPerNode = Maps.newHashMap();
                    for(Node node: cluster.getNodes()) {
                        socketStoresPerNode.put(node.getId(),
                                                socketStoreFactory.create(storeDef.getName(),
                                                                          node.getHost(),
                                                                          node.getSocketPort(),
                                                                          RequestFormatType.PROTOCOL_BUFFERS,
                                                                          RequestRoutingType.IGNORE_CHECKS));
                    }

                    long deletedKeys = 0L;
                    long foundKeys = 0L;
                    long totalKeys = 0L;
                    long keysRead = 0L;

                    try {
                        reader = new FileInputStream(storesKeyFile);
                        while(reader.available() != 0) {
                            int size = reader.read();

                            if(size <= 0) {
                                System.out.println("End of file reached.");
                                break;
                            }

                            // Read the key
                            byte[] key = new byte[size];
                            reader.read(key);
                            keysRead++;

                            List<Node> responsibleNodes = strategy.routeRequest(key);

                            if(!negativeTest) {
                                boolean missingKey = false;
                                for(Node node: responsibleNodes) {
                                    List<Versioned<byte[]>> value = socketStoresPerNode.get(node.getId())
                                                                                       .get(new ByteArray(key),
                                                                                            null);

                                    if(value == null || value.size() == 0) {
                                        missingKey = true;

                                        if(this.verboseLogging) {
                                            String stringKey = ByteUtils.getString(key, "UTF-8");
                                            System.out.println("missing key=" + stringKey
                                                               + " on node=" + node.getId());
                                            System.out.println("is value null "
                                                               + ((value == null) ? "true"
                                                                                 : "false"));
                                            System.out.println("is size zero "
                                                               + ((value.size() == 0) ? "true"
                                                                                     : "false"));
                                        }
                                    }
                                }
                                if(!missingKey)
                                    foundKeys++;
                                totalKeys++;
                            } else {
                                if(!responsibleNodes.contains(cluster.getNodeById(nodeId))) {
                                    List<Versioned<byte[]>> value = socketStoresPerNode.get(nodeId)
                                                                                       .get(new ByteArray(key),
                                                                                            null);

                                    if(value == null || value.size() == 0) {
                                        deletedKeys++;
                                    }
                                    totalKeys++;
                                }
                            }
                        }

                        if(!negativeTest) {
                            System.out.println("Found = " + foundKeys + ", Total = " + totalKeys
                                               + ", Keys read = " + keysRead);
                            if(foundKeys > 0 && totalKeys > 0) {
                                System.out.println("%age found - " + 100.0 * (double) foundKeys
                                                   / totalKeys);
                            }
                        } else {
                            System.out.println("Deleted = " + deletedKeys + " Total = " + totalKeys);
                            if(deletedKeys > 0 && totalKeys > 0) {
                                System.out.println("%age deleted - " + 100.0 * (double) deletedKeys
                                                   / totalKeys);
                            }
                        }
                    } finally {
                        if(reader != null)
                            reader.close();

                        // close all socket stores
                        for(Store<ByteArray, byte[], byte[]> store: socketStoresPerNode.values()) {
                            store.close();
                        }
                    }
                }

            }
        } finally {
            if(adminClient != null)
                adminClient.close();
        }
    }
}

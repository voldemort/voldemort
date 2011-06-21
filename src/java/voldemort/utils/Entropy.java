package voldemort.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
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

public class Entropy {

    private int numThreads;
    private int nodeId;
    private long numKeys;

    public static long DEFAULT_NUM_KEYS = 1000;

    /**
     * Entropy constructor. Uses DEFAULT_NUM_KEYS number of keys
     * 
     * @param nodeId Node id. If -1, goes over all of them
     * @param numThreads Number of threads
     */
    public Entropy(int nodeId, int numThreads) {
        this.numThreads = numThreads;
        this.nodeId = nodeId;
        this.numKeys = DEFAULT_NUM_KEYS;
    }

    /**
     * Entropy constructor
     * 
     * @param nodeId Node id. If -1, goes over all of them
     * @param numThreads Number of threads
     * @param numKeys Number of keys
     */
    public Entropy(int nodeId, int numThreads, long numKeys) {
        this.numThreads = numThreads;
        this.nodeId = nodeId;
        this.numKeys = numKeys;
    }

    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("node", "Node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("threads", "Number of threads")
              .withRequiredArg()
              .describedAs("threads")
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
        parser.accepts("num-keys", "Number of keys per store [ Default: 100 ]")
              .withRequiredArg()
              .describedAs("keys")
              .ofType(Long.class);

        OptionSet options = parser.parse(args);

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
        long numKeys = CmdUtils.valueOf(options, "num-keys", 100L);
        int nodeId = CmdUtils.valueOf(options, "node", 0);
        int numThreads = CmdUtils.valueOf(options, "threads", 10);
        boolean opType = CmdUtils.valueOf(options, "op-type", false);

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

        Entropy detector = new Entropy(nodeId, numThreads, numKeys);
        detector.generateEntropy(cluster, storeDefs, outputDir, opType);
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
        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(cluster,
                                          new AdminClientConfig().setMaxConnectionsPerNode(numThreads));

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
                            for(Node node: cluster.getNodes()) {
                                keys = adminClient.fetchKeys(node.getId(),
                                                             storeDef.getName(),
                                                             cluster.getNodeById(node.getId())
                                                                    .getPartitionIds(),
                                                             null,
                                                             false);
                                for(long keyId = 0; keyId < numKeysPerNode && keys.hasNext(); keyId++) {
                                    ByteArray key = keys.next();
                                    writer.write(key.length());
                                    writer.write(key.get());
                                }
                            }
                        } else {
                            keys = adminClient.fetchKeys(nodeId,
                                                         storeDef.getName(),
                                                         cluster.getNodeById(nodeId)
                                                                .getPartitionIds(),
                                                         null,
                                                         false);
                            for(long keyId = 0; keyId < numKeys && keys.hasNext(); keyId++) {
                                ByteArray key = keys.next();
                                writer.write(key.length());
                                writer.write(key.get());
                            }
                        }

                    } finally {
                        if(writer != null)
                            writer.close();
                    }

                } else {

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

                    RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                  cluster);
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

                    long foundKeys = 0L;
                    long totalKeys = 0L;
                    try {
                        reader = new FileInputStream(storesKeyFile);
                        while(reader.available() != 0) {
                            int size = reader.read();

                            if(size <= 0) {
                                break;
                            }

                            // Read the key
                            byte[] key = new byte[size];
                            reader.read(key);

                            List<Node> responsibleNodes = strategy.routeRequest(key);
                            boolean missingKey = false;
                            for(Node node: responsibleNodes) {
                                List<Versioned<byte[]>> value = socketStoresPerNode.get(node.getId())
                                                                                   .get(new ByteArray(key),
                                                                                        null);

                                if(value == null || value.size() == 0) {
                                    missingKey = true;
                                }
                            }
                            if(!missingKey)
                                foundKeys++;

                            totalKeys++;

                        }
                        System.out.println("Found = " + foundKeys + " Total = " + totalKeys);
                        if(foundKeys > 0 && totalKeys > 0) {
                            System.out.println("%age found - " + 100.0 * (double) foundKeys
                                               / totalKeys);
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
                adminClient.stop();
        }
    }
}

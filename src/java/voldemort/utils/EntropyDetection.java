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

public class EntropyDetection {

    @SuppressWarnings("cast")
    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("cluster-xml", "[REQUIRED] Path to cluster-xml")
              .withRequiredArg()
              .describedAs("xml")
              .ofType(String.class);
        parser.accepts("stores-xml", "[REQUIRED] Path to stores-xml")
              .withRequiredArg()
              .describedAs("xml")
              .ofType(String.class);
        parser.accepts("output-dir",
                       "[REQUIRED] The output directory where we'll store / retrieve the keys")
              .withRequiredArg()
              .describedAs("output-dir")
              .ofType(String.class);
        parser.accepts("op", "Operation type (0 - gets keys [default], 1 - checks the keys")
              .withRequiredArg()
              .describedAs("op")
              .ofType(Integer.class);
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
        int opType = CmdUtils.valueOf(options, "op", 0);
        long numKeys = CmdUtils.valueOf(options, "num-keys", 100L);

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

        AdminClient adminClient = null;
        try {

            // Parse the metadata
            Cluster cluster = new ClusterMapper().readCluster(new File(clusterXml));
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXml));

            adminClient = new AdminClient(cluster, new AdminClientConfig().setAdminConnectionTimeoutSec(60 * 60 * 2)
                                                                          .setMaxThreads(10));

            for(StoreDefinition storeDef: storeDefs) {
                File storesKeyFile = new File(outputDir, storeDef.getName());
                if(AdminClient.restoreStoreEngineBlackList.contains(storeDef.getType())) {
                    System.out.println("Ignoring store " + storeDef.getName());
                    continue;
                } else {
                    System.out.println("Working on store " + storeDef.getName());
                }
                switch(opType) {
                    case 0:
                    default:
                        if(storesKeyFile.exists()) {
                            System.err.println("Key files for " + storeDef.getName()
                                               + " already exists");
                            continue;
                        }
                        FileOutputStream writer = null;
                        try {
                            writer = new FileOutputStream(storesKeyFile);
                            Iterator<ByteArray> keys = adminClient.fetchKeys(0,
                                                                             storeDef.getName(),
                                                                             cluster.getNodeById(0)
                                                                                    .getPartitionIds(),
                                                                             null,
                                                                             false);
                            for(long keyId = 0; keyId < numKeys && keys.hasNext(); keyId++) {
                                ByteArray key = keys.next();
                                writer.write(key.length());
                                writer.write(key.get());
                            }

                        } finally {
                            if(writer != null)
                                writer.close();
                        }
                        break;
                    case 1:
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
                        RequestRoutingType requestRoutingType = RequestRoutingType.getRequestRoutingType(false,
                                                                                                         false);
                        // Cache connections to all nodes for this store
                        // in advance
                        HashMap<Integer, Store<ByteArray, byte[], byte[]>> socketStoresPerNode = Maps.newHashMap();
                        for(Node node: cluster.getNodes()) {
                            socketStoresPerNode.put(node.getId(),
                                                    socketStoreFactory.create(storeDef.getName(),
                                                                              node.getHost(),
                                                                              node.getSocketPort(),
                                                                              RequestFormatType.VOLDEMORT_V1,
                                                                              requestRoutingType));
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
                        break;
                }
            }
        } finally {
            if(adminClient != null)
                adminClient.stop();
        }
    }
}
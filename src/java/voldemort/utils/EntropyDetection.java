package voldemort.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.store.StoreDefinition;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;

public class EntropyDetection {

    @SuppressWarnings("unchecked")
    public static void main(String args[]) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("output-dir", "[REQUIRED] The output directory where we'll store the keys")
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

        Set<String> missing = CmdUtils.missing(options, "url", "output-dir");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        // compulsory params
        String url = (String) options.valueOf("url");
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

        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(url, new AdminClientConfig().setMaxThreads(10));

            // Get store definition meta-data from node 0
            List<StoreDefinition> storeDefs = adminClient.getRemoteStoreDefList(0).getValue();
            Cluster cluster = adminClient.getAdminClientCluster();

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
                        SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(url));
                        StoreClient storeClient = socketFactory.getStoreClient(storeDef.getName());

                        DefaultSerializerFactory factory = new DefaultSerializerFactory();
                        Serializer<?> keySerializer = factory.getSerializer(storeDef.getKeySerializer());
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

                                Versioned<Object> value = storeClient.get(keySerializer.toObject(key));
                                if(value != null) {
                                    foundKeys++;
                                }
                                totalKeys++;

                            }
                            System.out.println("Found = " + foundKeys + " Total = " + totalKeys);
                            if(foundKeys > 0 && totalKeys > 0) {
                                System.out.println("%age found - " + (double) 100
                                                   * (foundKeys / totalKeys));
                            }
                        } finally {
                            if(reader != null)
                                reader.close();
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

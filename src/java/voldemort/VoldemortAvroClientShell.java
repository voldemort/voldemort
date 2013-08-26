package voldemort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;

import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.Pair;
import voldemort.utils.Utils;

/**
 * Utility to check avro values through command line very handy for debugging
 * since Voldemort shell only supports JSON
 * 
 */

public class VoldemortAvroClientShell {

    private static final String PROMPT = "> ";

    public static void main(String args[]) throws IOException {

        if(args.length < 2 || args.length > 3) {
            System.err.println("Usage: java VoldemortAvroClient store_name bootstrap_url [command_file]");
            System.exit(-1);
        }

        String storeName = args[0];
        String bootstrapUrl = args[1];

        String commandsFileName = "";
        BufferedReader fileReader = null;
        BufferedReader inputReader = null;
        try {
            if(args.length == 3) {
                commandsFileName = args[2];
                fileReader = new BufferedReader(new FileReader(commandsFileName));
            }

            inputReader = new BufferedReader(new InputStreamReader(System.in));
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                      .setEnableLazy(false)
                                                      .setRequestFormatType(RequestFormatType.VOLDEMORT_V3);

        StoreClientFactory factory = null;
        DefaultStoreClient<Object, Object> client = null;
        try {
            try {
                factory = new SocketStoreClientFactory(clientConfig);
                client = (DefaultStoreClient<Object, Object>) factory.getStoreClient(storeName);
            } catch(Exception e) {
                Utils.croak("Could not connect to server: " + e.getMessage());
            }

            System.out.println("Established connection to " + storeName + " via " + bootstrapUrl);
            System.out.print(PROMPT);

            Pair<Schema, Schema> keyValueSchemaPair = getLatestKeyValueSchema(bootstrapUrl,
                                                                              storeName);
            Schema latestKeySchema = keyValueSchemaPair.getFirst();
            if(latestKeySchema == null) {
                Utils.croak("Could not parse latest key schema for store name " + storeName);
            }

            Schema latestValueSchema = keyValueSchemaPair.getSecond();

            if(latestValueSchema == null) {
                Utils.croak("Could not parse latest value schema for store name " + storeName);
            }

            if(fileReader != null) {
                processCommands(client, fileReader, latestKeySchema, latestValueSchema, true);
            } else {
                processCommands(client, inputReader, latestKeySchema, latestValueSchema, false);
            }
        } finally {
            if(factory != null)
                factory.close();
            if(fileReader != null)
                fileReader.close();
        }
    }

    private static Pair<Schema, Schema> getLatestKeyValueSchema(String url, String storeName) {

        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(url, new AdminClientConfig(), new ClientConfig());
            List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(0)
                                                                         .getValue();

            for(StoreDefinition storeDef: storeDefs) {
                if(storeDef.getName().equals(storeName)) {
                    Schema keySchema = Schema.parse(storeDef.getKeySerializer()
                                                            .getCurrentSchemaInfo());
                    Schema valueSchema = Schema.parse(storeDef.getValueSerializer()
                                                              .getCurrentSchemaInfo());
                    return new Pair<Schema, Schema>(keySchema, valueSchema);
                }
            }
        } catch(Exception e) {
            System.err.println("Error while getting lastest key schema " + e.getMessage());
        } finally {
            if(adminClient != null) {
                adminClient.close();
            }
        }

        return null;
    }

    private static void processCommands(DefaultStoreClient<Object, Object> client,
                                        BufferedReader reader,
                                        Schema keySchema,
                                        Schema valueSchema,
                                        boolean printCommands) throws IOException {
        for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            System.out.print(PROMPT);
            if(line.trim().equals(""))
                continue;
            if(printCommands)
                System.out.println(line);
            try {
                if(line.toLowerCase().startsWith("get")) {

                    System.out.println("Enter key:");
                    line = reader.readLine();

                    JsonDecoder decoder = new JsonDecoder(keySchema, line);
                    GenericDatumReader<Object> datumReader = null;
                    Object key = null;
                    try {
                        datumReader = new GenericDatumReader<Object>(keySchema);
                        key = datumReader.read(null, decoder);
                    } catch(IOException e) {}
                    if(key == null) {
                        System.err.println("Error parsing key ");
                        continue;
                    }

                    System.out.println("Value - " + client.get(key));
                } else if(line.toLowerCase().startsWith("put")) {

                    String keyString = null;
                    String valueString = null;

                    System.out.println("Enter key:");
                    line = reader.readLine();
                    keyString = line;

                    System.out.println("Enter value:");
                    line = reader.readLine();
                    valueString = line;

                    JsonDecoder keyDecoder = new JsonDecoder(keySchema, keyString);

                    JsonDecoder valueDecoder = new JsonDecoder(valueSchema, valueString);

                    GenericDatumReader<Object> datumReader = null;
                    Object key = null;
                    Object value = null;
                    try {
                        datumReader = new GenericDatumReader<Object>(keySchema);
                        key = datumReader.read(null, keyDecoder);
                    } catch(IOException e) {}
                    if(key == null) {
                        System.err.println("Error parsing key ");
                        continue;
                    }
                    try {
                        datumReader = new GenericDatumReader<Object>(valueSchema);
                        value = datumReader.read(null, valueDecoder);
                    } catch(IOException e) {}
                    if(value == null) {
                        System.err.println("Error parsing value ");
                        continue;
                    }

                    System.out.println("Put - " + client.put(key, value));
                } else if(line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Only supported 'get' & 'put' ");
                }
            } catch(Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }

        }
    }
}

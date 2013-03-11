/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.serialization.SerializationException;
import voldemort.serialization.json.EndOfFileException;
import voldemort.serialization.json.JsonReader;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * A toy shell to interact with the server via the command line
 * 
 * 
 */
public class VoldemortClientShell {

    private static final String PROMPT = "> ";

    private static DefaultStoreClient<Object, Object> client;

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("client-zone-id", "client zone id for zone routing")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() < 2 || nonOptions.size() > 3) {
            System.err.println("Usage: java VoldemortClientShell store_name bootstrap_url [command_file] [options]");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String storeName = nonOptions.get(0);
        String bootstrapUrl = nonOptions.get(1);

        String commandsFileName = "";
        BufferedReader fileReader = null;
        BufferedReader inputReader = null;
        try {
            if(nonOptions.size() == 3) {
                commandsFileName = nonOptions.get(2);
                fileReader = new BufferedReader(new FileReader(commandsFileName));
            }

            inputReader = new BufferedReader(new InputStreamReader(System.in));
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                      .setEnableLazy(false)
                                                      .setRequestFormatType(RequestFormatType.VOLDEMORT_V3);

        if(options.has("client-zone-id")) {
            clientConfig.setClientZoneId((Integer) options.valueOf("client-zone-id"));
        }

        StoreClientFactory factory = null;
        AdminClient adminClient = null;

        try {
            try {
                factory = new SocketStoreClientFactory(clientConfig);
                client = (DefaultStoreClient<Object, Object>) factory.getStoreClient(storeName);
                adminClient = new AdminClient(bootstrapUrl,
                                              new AdminClientConfig(),
                                              new ClientConfig());
            } catch(Exception e) {
                Utils.croak("Could not connect to server: " + e.getMessage());
            }

            System.out.println("Established connection to " + storeName + " via " + bootstrapUrl);
            System.out.print(PROMPT);
            if(fileReader != null) {
                processCommands(factory, adminClient, fileReader, true);
                fileReader.close();
            }
            processCommands(factory, adminClient, inputReader, false);
        } finally {
            if(adminClient != null)
                adminClient.close();
            if(factory != null)
                factory.close();
        }
    }

    private static void processCommands(StoreClientFactory factory,
                                        AdminClient adminClient,
                                        BufferedReader reader,
                                        boolean printCommands) throws IOException {
        for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            if(line.trim().equals(""))
                continue;
            if(printCommands)
                System.out.println(line);
            try {
                if(line.toLowerCase().startsWith("put")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("put".length())));
                    Object key = tightenNumericTypes(jsonReader.read());
                    Object value = tightenNumericTypes(jsonReader.read());
                    if(jsonReader.hasMore())
                        client.put(key, value, tightenNumericTypes(jsonReader.read()));
                    else
                        client.put(key, value);
                } else if(line.toLowerCase().startsWith("getall")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("getall".length())));
                    List<Object> keys = new ArrayList<Object>();
                    try {
                        while(true)
                            keys.add(jsonReader.read());
                    } catch(EndOfFileException e) {
                        // this is okay, just means we are done reading
                    }
                    Map<Object, Versioned<Object>> vals = client.getAll(keys);
                    if(vals.size() > 0) {
                        for(Map.Entry<Object, Versioned<Object>> entry: vals.entrySet()) {
                            System.out.print(entry.getKey());
                            System.out.print(" => ");
                            printVersioned(entry.getValue());
                        }
                    } else {
                        System.out.println("null");
                    }
                } else if(line.toLowerCase().startsWith("getmetadata")) {
                    String[] args = line.substring("getmetadata".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String key = args[1];
                    Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(remoteNodeId,
                                                                                                key);
                    if(versioned == null) {
                        System.out.println("null");
                    } else {
                        System.out.println(versioned.getVersion());
                        System.out.print(": ");
                        System.out.println(versioned.getValue());
                        System.out.println();
                    }
                } else if(line.toLowerCase().startsWith("get")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("get".length())));
                    Object key = tightenNumericTypes(jsonReader.read());
                    if(jsonReader.hasMore())
                        printVersioned(client.get(key, tightenNumericTypes(jsonReader.read())));
                    else
                        printVersioned(client.get(key));
                } else if(line.toLowerCase().startsWith("delete")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("delete".length())));
                    client.delete(tightenNumericTypes(jsonReader.read()));
                } else if(line.startsWith("preflist")) {
                    JsonReader jsonReader = new JsonReader(new StringReader(line.substring("preflist".length())));
                    Object key = tightenNumericTypes(jsonReader.read());
                    printNodeList(client.getResponsibleNodes(key), factory.getFailureDetector());
                } else if(line.toLowerCase().startsWith("fetchkeys")) {
                    String[] args = line.substring("fetchkeys".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String storeName = args[1];
                    List<Integer> partititionList = parseCsv(args[2]);
                    Iterator<ByteArray> partitionKeys = adminClient.bulkFetchOps.fetchKeys(remoteNodeId,
                                                                                           storeName,
                                                                                           partititionList,
                                                                                           null,
                                                                                           false);

                    BufferedWriter writer = null;
                    try {
                        if(args.length > 3) {
                            writer = new BufferedWriter(new FileWriter(new File(args[3])));
                        } else
                            writer = new BufferedWriter(new OutputStreamWriter(System.out));
                    } catch(IOException e) {
                        System.err.println("Failed to open the output stream");
                        e.printStackTrace();
                    }
                    if(writer != null) {
                        while(partitionKeys.hasNext()) {
                            ByteArray keyByteArray = partitionKeys.next();
                            StringBuilder lineBuilder = new StringBuilder();
                            lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                            lineBuilder.append("\n");
                            writer.write(lineBuilder.toString());
                        }
                        writer.flush();
                    }
                } else if(line.toLowerCase().startsWith("fetch")) {
                    String[] args = line.substring("fetch".length() + 1).split("\\s+");
                    int remoteNodeId = Integer.valueOf(args[0]);
                    String storeName = args[1];
                    List<Integer> partititionList = parseCsv(args[2]);
                    Iterator<Pair<ByteArray, Versioned<byte[]>>> partitionEntries = adminClient.bulkFetchOps.fetchEntries(remoteNodeId,
                                                                                                                          storeName,
                                                                                                                          partititionList,
                                                                                                                          null,
                                                                                                                          false);
                    BufferedWriter writer = null;
                    try {
                        if(args.length > 3) {
                            writer = new BufferedWriter(new FileWriter(new File(args[3])));
                        } else
                            writer = new BufferedWriter(new OutputStreamWriter(System.out));
                    } catch(IOException e) {
                        System.err.println("Failed to open the output stream");
                        e.printStackTrace();
                    }
                    if(writer != null) {
                        while(partitionEntries.hasNext()) {
                            Pair<ByteArray, Versioned<byte[]>> pair = partitionEntries.next();
                            ByteArray keyByteArray = pair.getFirst();
                            Versioned<byte[]> versioned = pair.getSecond();
                            StringBuilder lineBuilder = new StringBuilder();
                            lineBuilder.append(ByteUtils.getString(keyByteArray.get(), "UTF-8"));
                            lineBuilder.append("\t");
                            lineBuilder.append(versioned.getVersion());
                            lineBuilder.append("\t");
                            lineBuilder.append(ByteUtils.getString(versioned.getValue(), "UTF-8"));
                            lineBuilder.append("\n");
                            writer.write(lineBuilder.toString());
                        }
                        writer.flush();
                    }
                } else if(line.startsWith("help")) {
                    System.out.println();
                    System.out.println("Commands:");
                    System.out.println(PROMPT
                                       + "put key value --- Associate the given value with the key.");
                    System.out.println(PROMPT
                                       + "get key --- Retrieve the value associated with the key.");
                    System.out.println(PROMPT
                                       + "getall key1 [key2...] --- Retrieve the value(s) associated with the key(s).");
                    System.out.println(PROMPT
                                       + "delete key --- Remove all values associated with the key.");
                    System.out.println(PROMPT
                                       + "preflist key --- Get node preference list for given key.");
                    String metaKeyValues = voldemort.store.metadata.MetadataStore.METADATA_KEYS.toString();
                    System.out.println(PROMPT
                                       + "getmetadata node_id meta_key --- Get store metadata associated "
                                       + "with meta_key from node_id. meta_key may be one of "
                                       + metaKeyValues.substring(1, metaKeyValues.length() - 1)
                                       + ".");
                    System.out.println(PROMPT
                                       + "fetchkeys node_id store_name partitions <file_name> --- Fetch all keys "
                                       + "from given partitions (a comma separated list) of store_name on "
                                       + "node_id. Optionally, write to file_name. "
                                       + "Use getmetadata to determine appropriate values for store_name and partitions");
                    System.out.println(PROMPT
                                       + "fetch node_id store_name partitions <file_name> --- Fetch all entries "
                                       + "from given partitions (a comma separated list) of store_name on "
                                       + "node_id. Optionally, write to file_name. "
                                       + "Use getmetadata to determine appropriate values for store_name and partitions");
                    System.out.println(PROMPT + "help --- Print this message.");
                    System.out.println(PROMPT + "exit --- Exit from this shell.");
                    System.out.println();

                } else if(line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Invalid command. (Try 'help' for usage.)");
                }
            } catch(EndOfFileException e) {
                System.err.println("Expected additional token.");
            } catch(SerializationException e) {
                System.err.print("Error serializing values: ");
                e.printStackTrace();
            } catch(VoldemortException e) {
                System.err.println("Exception thrown during operation.");
                e.printStackTrace(System.err);
            } catch(ArrayIndexOutOfBoundsException e) {
                System.err.println("Invalid command. (Try 'help' for usage.)");
            } catch(Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }
            System.out.print(PROMPT);
        }
    }

    private static List<Integer> parseCsv(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")), new Function<String, Integer>() {

            public Integer apply(String input) {
                return Integer.valueOf(input);
            }
        });
    }

    private static void printNodeList(List<Node> nodes, FailureDetector failureDetector) {
        if(nodes.size() > 0) {
            for(int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                System.out.println("Node " + node.getId());
                System.out.println("host:  " + node.getHost());
                System.out.println("port: " + node.getSocketPort());
                System.out.println("available: "
                                   + (failureDetector.isAvailable(node) ? "yes" : "no"));
                System.out.println("last checked: " + failureDetector.getLastChecked(node)
                                   + " ms ago");
                System.out.println();
            }
        }
    }

    private static void printVersioned(Versioned<Object> v) {
        if(v == null) {
            System.out.println("null");
        } else {
            System.out.print(v.getVersion());
            System.out.print(": ");
            printObject(v.getValue());
            System.out.println();
        }
    }

    @SuppressWarnings("unchecked")
    private static void printObject(Object o) {
        if(o == null) {
            System.out.print("null");
        } else if(o instanceof String) {
            System.out.print('"');
            System.out.print(o);
            System.out.print('"');
        } else if(o instanceof Date) {
            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
            System.out.print("'");
            System.out.print(df.format((Date) o));
            System.out.print("'");
        } else if(o instanceof List) {
            List<Object> l = (List<Object>) o;
            System.out.print("[");
            for(Object obj: l)
                printObject(obj);
            System.out.print("]");
        } else if(o instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) o;
            System.out.print('{');
            for(String s: m.keySet()) {
                printObject(s);
                System.out.print(':');
                printObject(m.get(s));
                System.out.print(", ");
            }
            System.out.print('}');
        } else if(o instanceof Object[]) {
            Object[] a = (Object[]) o;
            System.out.print(Arrays.deepToString(a));
        } else if(o instanceof byte[]) {
            byte[] a = (byte[]) o;
            System.out.print(Arrays.toString(a));
        } else {
            System.out.print(o);
        }
    }

    /*
     * We need to coerce numbers to the tightest possible type and let the
     * schema coerce them to the proper
     */
    @SuppressWarnings("unchecked")
    private static Object tightenNumericTypes(Object o) {
        if(o == null) {
            return null;
        } else if(o instanceof List) {
            List l = (List) o;
            for(int i = 0; i < l.size(); i++)
                l.set(i, tightenNumericTypes(l.get(i)));
            return l;
        } else if(o instanceof Map) {
            Map m = (Map) o;
            for(Map.Entry entry: (Set<Map.Entry>) m.entrySet())
                m.put(entry.getKey(), tightenNumericTypes(entry.getValue()));
            return m;
        } else if(o instanceof Number) {
            Number n = (Number) o;
            if(o instanceof Integer) {
                if(n.intValue() < Byte.MAX_VALUE)
                    return n.byteValue();
                else if(n.intValue() < Short.MAX_VALUE)
                    return n.shortValue();
                else
                    return n;
            } else if(o instanceof Double) {
                if(n.doubleValue() < Float.MAX_VALUE)
                    return n.floatValue();
                else
                    return n;
            } else {
                throw new RuntimeException("Unsupported numeric type: " + o.getClass());
            }
        } else {
            return o;
        }
    }

}

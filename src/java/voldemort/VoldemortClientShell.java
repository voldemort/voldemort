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
import java.io.PrintStream;
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang.mutable.MutableInt;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.EndOfFileException;
import voldemort.serialization.json.JsonReader;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Shell to interact with the voldemort cluster from the command line...
 *
 */
public class VoldemortClientShell {

    protected static final String PROMPT = "> ";

    protected StoreClient<Object, Object> client;

    private SocketStoreClientFactory factory;

    private StoreDefinition storeDef;

    protected final BufferedReader commandReader;

    protected final PrintStream commandOutput;

    protected final PrintStream errorStream;

    private AdminClient adminClient;

    protected VoldemortClientShell(BufferedReader commandReader,
                                   PrintStream commandOutput,
                                   PrintStream errorStream) {
        this.commandReader = commandReader;
        this.commandOutput = commandOutput;
        this.errorStream = errorStream;
    }

    public VoldemortClientShell(ClientConfig clientConfig,
                                String storeName,
                                BufferedReader commandReader,
                                PrintStream commandOutput,
                                PrintStream errorStream) {

        this.commandReader = commandReader;
        this.commandOutput = commandOutput;
        this.errorStream = errorStream;

        String bootstrapUrl = clientConfig.getBootstrapUrls()[0];

        try {
            factory = new SocketStoreClientFactory(clientConfig);
            client = factory.getStoreClient(storeName);
            adminClient = new AdminClient(bootstrapUrl, new AdminClientConfig(), new ClientConfig());

            storeDef = StoreUtils.getStoreDef(factory.getStoreDefs(), storeName);

            commandOutput.println("Established connection to " + storeName + " via " + bootstrapUrl);
            commandOutput.print(PROMPT);
        } catch(Exception e) {
            safeClose();
            Utils.croak("Could not connect to server: " + e.getMessage());
        }
    }

    // getter method for the Store
    public StoreClient<Object, Object> getStoreClient() {
        return this.client;
    }

    protected void safeClose() {
        if(adminClient != null)
            adminClient.close();
        if(factory != null)
            factory.close();
    }

    public void process(boolean printCommands) {
        try {
            processCommands(printCommands);
        } catch(Exception e) {
            Utils.croak("Error processing commands.." + e.getMessage());
        } finally {
            safeClose();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("client-zone-id", "client zone id for zone routing")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
        OptionSet options = parser.parse(args);

        List<String> nonOptions = (List<String>) options.nonOptionArguments();
        if(nonOptions.size() < 2 || nonOptions.size() > 3) {
            System.err.println("Usage: java VoldemortClientShell store_name bootstrap_url [command_file] [options]");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        String storeName = nonOptions.get(0);
        String bootstrapUrl = nonOptions.get(1);
        BufferedReader inputReader = null;
        boolean fileInput = false;

        try {
            if(nonOptions.size() == 3) {
                inputReader = new BufferedReader(new FileReader(nonOptions.get(2)));
                fileInput = true;
            } else {
                inputReader = new BufferedReader(new InputStreamReader(System.in));
            }
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        ClientConfig clientConfig = new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                      .setEnableLazy(false)
                                                      .setRequestFormatType(RequestFormatType.VOLDEMORT_V3);

        if(options.has("client-zone-id")) {
            clientConfig.setClientZoneId((Integer) options.valueOf("client-zone-id"));
        }

        VoldemortClientShell shell = new VoldemortClientShell(clientConfig,
                                                              storeName,
                                                              inputReader,
                                                              System.out,
                                                              System.err);
        shell.process(fileInput);
    }

    public static Object parseObject(SerializerDefinition serializerDef,
                                     String argStr,
                                     MutableInt parsePos,
                                     PrintStream errorStream) {
        Object obj = null;
        try {
            // TODO everything is read as json string now..
            JsonReader jsonReader = new JsonReader(new StringReader(argStr));
            obj = jsonReader.read();
            // mark how much of the original string, we blew through to
            // extract the avrostring.
            parsePos.setValue(jsonReader.getCurrentLineOffset() - 1);

            if(StoreDefinitionUtils.isAvroSchema(serializerDef.getName())) {
                // TODO Need to check all the avro siblings work
                // For avro, we hack and extract avro key/value as a string,
                // before we do the actual parsing with the schema
                String avroString = (String) obj;
                // From here on, this is just normal avro parsing.
                Schema latestSchema = Schema.parse(serializerDef.getCurrentSchemaInfo());
                try {
                    JsonDecoder decoder = new JsonDecoder(latestSchema, avroString);
                    GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(latestSchema);
                    obj = datumReader.read(null, decoder);
                } catch(IOException io) {
                    errorStream.println("Error parsing avro string " + avroString);
                    io.printStackTrace();
                }
            } else {
                // all json processing does some numeric type tightening
                obj = tightenNumericTypes(obj);
            }
        } catch(EndOfFileException eof) {
            // can be thrown from the jsonReader.read(..) call indicating, we
            // have nothing more to read.
            obj = null;
        }
        return obj;
    }

    protected Object parseKey(String argStr, MutableInt parsePos) {
        return parseObject(storeDef.getKeySerializer(), argStr, parsePos, this.errorStream);
    }

    protected Object parseValue(String argStr, MutableInt parsePos) {
        return parseObject(storeDef.getValueSerializer(), argStr, parsePos, this.errorStream);
    }

    protected void processPut(String putArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(putArgStr, parsePos);
        putArgStr = putArgStr.substring(parsePos.intValue());
        Object value = parseValue(putArgStr, parsePos);
        client.put(key, value);
    }

    /**
     *
     * @param getAllArgStr space separated list of key strings
     */

    protected void processGetAll(String getAllArgStr) {
        List<Object> keys = new ArrayList<Object>();
        MutableInt parsePos = new MutableInt(0);

        while(true) {
            Object key = parseKey(getAllArgStr, parsePos);
            if(key == null) {
                break;
            }
            keys.add(key);
            getAllArgStr = getAllArgStr.substring(parsePos.intValue());
        }

        Map<Object, Versioned<Object>> vals = client.getAll(keys);
        if(vals.size() > 0) {
            for(Map.Entry<Object, Versioned<Object>> entry: vals.entrySet()) {
                commandOutput.print(entry.getKey());
                commandOutput.print(" => ");
                printVersioned(entry.getValue());
            }
        } else {
            commandOutput.println("null");
        }
    }

    protected void processGet(String getArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(getArgStr, parsePos);
        printVersioned(client.get(key));
    }

    protected void processDelete(String deleteArgStr) {
        MutableInt parsePos = new MutableInt(0);
        Object key = parseKey(deleteArgStr, parsePos);
        client.delete(key);
    }

    protected void processCommands(boolean printCommands) throws IOException {
        for(String line = commandReader.readLine(); line != null; line = commandReader.readLine()) {
            if(line.trim().equals("")) {
                commandOutput.print(PROMPT);
                continue;
            }
            if(printCommands)
                commandOutput.println(line);
            evaluateCommand(line, printCommands);
            commandOutput.print(PROMPT);
        }
    }

    // useful as this separates the repeated prompt from the evaluation
    // using no modifier as no sub-class will have access but all classes within
    // package will
    boolean evaluateCommand(String line, boolean printCommands) {
        try {
            if(line.toLowerCase().startsWith("put")) {
                processPut(line.substring("put".length()));
            } else if(line.toLowerCase().startsWith("getall")) {
                processGetAll(line.substring("getall".length()));
            } else if(line.toLowerCase().startsWith("getmetadata")) {
                String[] args = line.substring("getmetadata".length() + 1).split("\\s+");
                int remoteNodeId = Integer.valueOf(args[0]);
                String key = args[1];
                Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(remoteNodeId,
                                                                                            key);
                if(versioned == null) {
                    commandOutput.println("null");
                } else {
                    commandOutput.println(versioned.getVersion());
                    commandOutput.print(": ");
                    commandOutput.println(versioned.getValue());
                    commandOutput.println();
                }
            } else if(line.toLowerCase().startsWith("get")) {
                processGet(line.substring("get".length()));
            } else if(line.toLowerCase().startsWith("delete")) {
                processDelete(line.substring("delete".length()));
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
                        writer = new BufferedWriter(new OutputStreamWriter(commandOutput));
                } catch(IOException e) {
                    errorStream.println("Failed to open the output stream");
                    e.printStackTrace(errorStream);
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
                        writer = new BufferedWriter(new OutputStreamWriter(commandOutput));
                } catch(IOException e) {
                    errorStream.println("Failed to open the output stream");
                    e.printStackTrace(errorStream);
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
                commandOutput.println();
                commandOutput.println("Commands:");
                commandOutput.println(PROMPT
                                      + "put key value --- Associate the given value with the key.");
                commandOutput.println(PROMPT
                                      + "get key --- Retrieve the value associated with the key.");
                commandOutput.println(PROMPT
                                      + "getall key1 [key2...] --- Retrieve the value(s) associated with the key(s).");
                commandOutput.println(PROMPT
                                      + "delete key --- Remove all values associated with the key.");
                commandOutput.println(PROMPT
                                      + "preflist key --- Get node preference list for given key.");
                String metaKeyValues = voldemort.store.metadata.MetadataStore.METADATA_KEYS.toString();
                commandOutput.println(PROMPT
                                      + "getmetadata node_id meta_key --- Get store metadata associated "
                                      + "with meta_key from node_id. meta_key may be one of "
                                      + metaKeyValues.substring(1, metaKeyValues.length() - 1)
                                      + ".");
                commandOutput.println(PROMPT
                                      + "fetchkeys node_id store_name partitions <file_name> --- Fetch all keys "
                                      + "from given partitions (a comma separated list) of store_name on "
                                      + "node_id. Optionally, write to file_name. "
                                      + "Use getmetadata to determine appropriate values for store_name and partitions");
                commandOutput.println(PROMPT
                                      + "fetch node_id store_name partitions <file_name> --- Fetch all entries "
                                      + "from given partitions (a comma separated list) of store_name on "
                                      + "node_id. Optionally, write to file_name. "
                                      + "Use getmetadata to determine appropriate values for store_name and partitions");
                commandOutput.println(PROMPT + "help --- Print this message.");
                commandOutput.println(PROMPT + "exit --- Exit from this shell.");
                commandOutput.println();
                commandOutput.println("Avro usage:");
                commandOutput.println("For avro keys or values, ensure that the entire json string is enclosed within single quotes (').");
                commandOutput.println("Also, the field names and strings should STRICTLY be enclosed by double quotes(\")");
                commandOutput.println("eg: > put '{\"id\":1,\"name\":\"Vinoth Chandar\"}' '[{\"skill\":\"java\", \"score\":90.27, \"isendorsed\": true}]'");

            } else if(line.equals("quit") || line.equals("exit")) {
                commandOutput.println("bye.");
                System.exit(0);
            } else {
                errorStream.println("Invalid command. (Try 'help' for usage.)");
                return false;
            }
        } catch(EndOfFileException e) {
            errorStream.println("Expected additional token.");
        } catch(SerializationException e) {
            errorStream.print("Error serializing values: ");
            e.printStackTrace(errorStream);
        } catch(VoldemortException e) {
            errorStream.println("Exception thrown during operation.");
            e.printStackTrace(errorStream);
        } catch(ArrayIndexOutOfBoundsException e) {
            errorStream.println("Invalid command. (Try 'help' for usage.)");
        } catch(Exception e) {
            errorStream.println("Unexpected error:");
            e.printStackTrace(errorStream);
        }
        return true;
    }

    protected List<Integer> parseCsv(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")), new Function<String, Integer>() {

            public Integer apply(String input) {
                return Integer.valueOf(input);
            }
        });
    }

    private void printNodeList(List<Node> nodes, FailureDetector failureDetector) {
        if(nodes.size() > 0) {
            for(int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                commandOutput.println("Node " + node.getId());
                commandOutput.println("host:  " + node.getHost());
                commandOutput.println("port: " + node.getSocketPort());
                commandOutput.println("available: "
                                      + (failureDetector.isAvailable(node) ? "yes" : "no"));
                commandOutput.println("last checked: " + failureDetector.getLastChecked(node)
                                      + " ms ago");
                commandOutput.println();
            }
        }
    }

    protected void printVersioned(Versioned<Object> v) {
        if(v == null) {
            commandOutput.println("null");
        } else {
            commandOutput.print(v.getVersion());
            commandOutput.print(": ");
            printObject(v.getValue());
            commandOutput.println();
        }
    }

    @SuppressWarnings("unchecked")
    protected void printObject(Object o) {
        if(o == null) {
            commandOutput.print("null");
        } else if(o instanceof String) {
            commandOutput.print('"');
            commandOutput.print(o);
            commandOutput.print('"');
        } else if(o instanceof Date) {
            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
            commandOutput.print("'");
            commandOutput.print(df.format((Date) o));
            commandOutput.print("'");
        } else if(o instanceof List) {
            List<Object> l = (List<Object>) o;
            commandOutput.print("[");
            for(Object obj: l)
                printObject(obj);
            commandOutput.print("]");
        } else if(o instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) o;
            commandOutput.print('{');
            for(String s: m.keySet()) {
                printObject(s);
                commandOutput.print(':');
                printObject(m.get(s));
                commandOutput.print(", ");
            }
            commandOutput.print('}');
        } else if(o instanceof Object[]) {
            Object[] a = (Object[]) o;
            commandOutput.print(Arrays.deepToString(a));
        } else if(o instanceof byte[]) {
            byte[] a = (byte[]) o;
            commandOutput.print(Arrays.toString(a));
        } else {
            commandOutput.print(o);
        }
    }

    /*
     * We need to coerce numbers to the tightest possible type and let the
     * schema coerce them to the proper
     */
    @SuppressWarnings("unchecked")
    public static Object tightenNumericTypes(Object o) {
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

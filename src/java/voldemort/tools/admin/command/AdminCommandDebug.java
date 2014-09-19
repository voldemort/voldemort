/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools.admin.command;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.codec.DecoderException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.cluster.Zone;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.store.InvalidMetadataException;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;
import com.sleepycat.persist.StoreNotFoundException;

/**
 * Implements all debug commands.
 */
public class AdminCommandDebug extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("query-keys")) {
            SubCommandDebugQueryKeys.executeCommand(args);
        } else if(subCmd.equals("route")) {
            SubCommandDebugRoute.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Debug Commands");
        stream.println("-----------------------------------");
        stream.println("query-keys   Query stores for a set of keys.");
        stream.println("route        Show detailed routing plan for a given set of keys on a store.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help debug <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("query-keys")) {
            SubCommandDebugQueryKeys.printHelp(stream);
        } else if(subCmd.equals("route")) {
            SubCommandDebugRoute.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * debug query-keys command
     */
    public static class SubCommandDebugQueryKeys extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            AdminParserUtils.acceptsHex(parser); // either --hex or
                                                 // --json
            AdminParserUtils.acceptsJson(parser); // either --hex or
                                                  // --json
            AdminParserUtils.acceptsStoreMultiple(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  debug query-keys - Query stores for a set of keys");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  debug query-keys ((-x | -j) <key-list>) -s <store-name-list> -u <url>");
            stream.println("                   [-n <node-id-list> | --all-nodes]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and queries stores for a set of keys
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            String keyType = null;
            List<String> keyStrings = null;
            List<String> storeNames = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_HEX,
                                           AdminParserUtils.OPT_JSON);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            if(options.has(AdminParserUtils.OPT_HEX)) {
                keyType = AdminParserUtils.OPT_HEX;
                keyStrings = (List<String>) options.valuesOf(AdminParserUtils.OPT_HEX);
            } else if(options.has(AdminParserUtils.OPT_JSON)) {
                keyType = AdminParserUtils.OPT_JSON;
                keyStrings = (List<String>) options.valuesOf(AdminParserUtils.OPT_JSON);
            }
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(allNodes) {
                nodeIds = AdminToolUtils.getAllNodeIds(adminClient);
            }

            doDebugQueryKeys(adminClient, nodeIds, storeNames, keyStrings, keyType);
        }

        private static void doDebugQueryKey(AdminClient adminClient,
                                            List<Integer> queryingNodes,
                                            List<String> storeNames,
                                            String keyString,
                                            String keyFormat) throws IOException {

            Map<String, StoreDefinition> storeDefinitions = AdminToolUtils.getUserStoreDefMapOnNode(adminClient,
                                                                                                    queryingNodes.get(0));

            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));

            // iterate through stores
            for(final String storeName: storeNames) {
                // store definition
                StoreDefinition storeDefinition = storeDefinitions.get(storeName);
                if(storeDefinition == null) {
                    throw new StoreNotFoundException("Store " + storeName + " not found");
                }

                out.write("STORE_NAME: " + storeDefinition.getName() + "\n");

                // k-v serializer
                final SerializerDefinition keySerializerDef = storeDefinition.getKeySerializer();
                final SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
                SerializerFactory serializerFactory = new DefaultSerializerFactory();
                @SuppressWarnings("unchecked")
                final Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(keySerializerDef);
                @SuppressWarnings("unchecked")
                final Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(valueSerializerDef);

                // compression strategy
                final CompressionStrategy keyCompressionStrategy;
                final CompressionStrategy valueCompressionStrategy;
                if(keySerializerDef != null && keySerializerDef.hasCompression()) {
                    keyCompressionStrategy = new CompressionStrategyFactory().get(keySerializerDef.getCompression());
                } else {
                    keyCompressionStrategy = null;
                }
                if(valueSerializerDef != null && valueSerializerDef.hasCompression()) {
                    valueCompressionStrategy = new CompressionStrategyFactory().get(valueSerializerDef.getCompression());
                } else {
                    valueCompressionStrategy = null;
                }

                if(keyCompressionStrategy == null) {
                    out.write("KEY_COMPRESSION_STRATEGY: None\n");
                } else {
                    out.write("KEY_COMPRESSION_STRATEGY: " + keyCompressionStrategy.getType()
                              + "\n");
                }
                out.write("KEY_SERIALIZER_NAME: " + keySerializerDef.getName() + "\n");
                for(Map.Entry<Integer, String> entry: keySerializerDef.getAllSchemaInfoVersions()
                                                                      .entrySet()) {
                    out.write(String.format("KEY_SCHEMA VERSION=%d\n", entry.getKey()));
                    out.write("====================================\n");
                    out.write(entry.getValue());
                    out.write("\n====================================\n");
                }
                out.write("\n");
                if(valueCompressionStrategy == null) {
                    out.write("VALUE_COMPRESSION_STRATEGY: None\n");
                } else {
                    out.write("VALUE_COMPRESSION_STRATEGY: " + valueCompressionStrategy.getType()
                              + "\n");
                }
                out.write("VALUE_SERIALIZER_NAME: " + valueSerializerDef.getName() + "\n");
                for(Map.Entry<Integer, String> entry: valueSerializerDef.getAllSchemaInfoVersions()
                                                                        .entrySet()) {
                    out.write(String.format("VALUE_SCHEMA %d\n", entry.getKey()));
                    out.write("====================================\n");
                    out.write(entry.getValue());
                    out.write("\n====================================\n");
                }
                out.write("\n");

                // although the streamingOps support multiple keys, we only
                // query
                // one key here
                ByteArray key;
                try {
                    if(keyFormat.equals(AdminParserUtils.OPT_JSON)) {
                        Object keyObject;
                        String keySerializerName = keySerializerDef.getName();
                        if(isAvroSchema(keySerializerName)) {
                            Schema keySchema = Schema.parse(keySerializerDef.getCurrentSchemaInfo());
                            JsonDecoder decoder = new JsonDecoder(keySchema, keyString);
                            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(keySchema);
                            keyObject = datumReader.read(null, decoder);
                        } else if(keySerializerName.equals(DefaultSerializerFactory.JSON_SERIALIZER_TYPE_NAME)) {
                            JsonReader jsonReader = new JsonReader(new StringReader(keyString));
                            keyObject = jsonReader.read();
                        } else {
                            keyObject = keyString;
                        }

                        key = new ByteArray(keySerializer.toBytes(keyObject));
                    } else {
                        key = new ByteArray(ByteUtils.fromHexString(keyString));
                    }
                } catch(SerializationException se) {
                    System.err.println("Error serializing key " + keyString);
                    System.err.println("If this is a JSON key, you need to include escaped quotation marks in the command line if it is a string");
                    se.printStackTrace();
                    return;
                } catch(DecoderException de) {
                    System.err.println("Error decoding key " + keyString);
                    de.printStackTrace();
                    return;
                } catch(IOException io) {
                    System.err.println("Error parsing avro string " + keyString);
                    io.printStackTrace();
                    return;
                }

                boolean printedKey = false;
                // A Map<> could have been used instead of List<Entry<>> if
                // Versioned supported correct hash codes. Read the comment in
                // Versioned about the issue
                List<Entry<List<Versioned<byte[]>>, List<Integer>>> nodeValues = new ArrayList<Entry<List<Versioned<byte[]>>, List<Integer>>>();
                for(final Integer queryNodeId: queryingNodes) {
                    Iterator<QueryKeyResult> iterator;
                    iterator = adminClient.streamingOps.queryKeys(queryNodeId,
                                                                  storeName,
                                                                  Arrays.asList(key).iterator());
                    final StringWriter stringWriter = new StringWriter();

                    QueryKeyResult queryKeyResult = iterator.next();

                    if(!printedKey) {
                        // de-serialize and write key
                        byte[] keyBytes = queryKeyResult.getKey().get();
                        Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                                  : keyCompressionStrategy.inflate(keyBytes));

                        writeVoldKeyOrValueInternal(keyBytes,
                                                    keySerializer,
                                                    keyCompressionStrategy,
                                                    "KEY",
                                                    out);
                        printedKey = true;
                    }

                    // iterate through, de-serialize and write values
                    if(queryKeyResult.hasValues() && queryKeyResult.getValues().size() > 0) {

                        int elementId = -1;
                        for(int i = 0; i < nodeValues.size(); i++) {
                            if(Objects.equal(nodeValues.get(i).getKey(), queryKeyResult.getValues())) {
                                elementId = i;
                                break;
                            }
                        }

                        if(elementId == -1) {
                            ArrayList<Integer> nodes = new ArrayList<Integer>();
                            nodes.add(queryNodeId);
                            nodeValues.add(new AbstractMap.SimpleEntry<List<Versioned<byte[]>>, List<Integer>>(queryKeyResult.getValues(),
                                                                                                               nodes));
                        } else {
                            nodeValues.get(elementId).getValue().add(queryNodeId);
                        }

                        out.write(String.format("\nQueried node %d on store %s\n",
                                                queryNodeId,
                                                storeName));

                        int versionCount = 0;

                        if(queryKeyResult.getValues().size() > 1) {
                            out.write("VALUE " + versionCount + "\n");
                        }

                        for(Versioned<byte[]> versioned: queryKeyResult.getValues()) {

                            // write version
                            VectorClock version = (VectorClock) versioned.getVersion();
                            out.write("VECTOR_CLOCK_BYTE: "
                                      + ByteUtils.toHexString(version.toBytes()) + "\n");
                            out.write("VECTOR_CLOCK_TEXT: " + version.toString() + '['
                                      + new Date(version.getTimestamp()).toString() + "]\n");

                            // write value
                            byte[] valueBytes = versioned.getValue();
                            writeVoldKeyOrValueInternal(valueBytes,
                                                        valueSerializer,
                                                        valueCompressionStrategy,
                                                        "VALUE",
                                                        out);
                            versionCount++;
                        }
                    } // If a node does not host a key, it returns
                      // invalidmetdata
                      // exception.
                    else if(queryKeyResult.hasException()) {
                        boolean isInvalidMetadataException = queryKeyResult.getException() instanceof InvalidMetadataException;

                        // Print the exception if not InvalidMetadataException
                        // or
                        // you are querying only a single node.
                        if(!isInvalidMetadataException || queryingNodes.size() == 1) {
                            out.write(String.format("\nNode %d on store %s returned exception\n",
                                                    queryNodeId,
                                                    storeName));
                            out.write(queryKeyResult.getException().toString());
                            out.write("\n====================================\n");
                        }
                    } else {
                        if(queryingNodes.size() == 1) {
                            out.write(String.format("\nNode %d on store %s returned NULL\n",
                                                    queryNodeId,
                                                    storeName));
                            out.write("\n====================================\n");
                        }
                    }
                    out.flush();
                }

                out.write("\n====================================\n");
                for(Map.Entry<List<Versioned<byte[]>>, List<Integer>> nodeValue: nodeValues) {
                    out.write("Nodes with same Value "
                              + Arrays.toString(nodeValue.getValue().toArray()));
                    out.write("\n====================================\n");
                }
                if(nodeValues.size() > 1) {
                    out.write("\n*** Multiple (" + nodeValues.size()
                              + ") versions of key/value exist for the key ***\n");
                }
                out.flush();
            }
        }

        private static void writeVoldKeyOrValueInternal(byte[] input,
                                                        Serializer<Object> serializer,
                                                        CompressionStrategy compressionStrategy,
                                                        String prefix,
                                                        BufferedWriter out) throws IOException {
            out.write(prefix + "_BYTES\n====================================\n");
            out.write(ByteUtils.toHexString(input));
            out.write("\n====================================\n");
            try {
                Object inputObject = serializer.toObject((null == compressionStrategy) ? input
                                                                                      : compressionStrategy.inflate(input));

                out.write(prefix + "_TEXT\n====================================\n");
                if(inputObject instanceof GenericRecord) {
                    out.write(inputObject.toString());
                } else {
                    new JsonFactory(new ObjectMapper()).createJsonGenerator(out)
                                                       .writeObject(inputObject);
                }
                out.write("\n====================================\n\n");
            } catch(SerializationException e) {}

        }

        /**
         * Queries stores for a set of keys
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeIds Node ids to query keys from
         * @param storeNames Stores to be queried
         * @param keyStrings Keys to be queried
         * @param keyType Format of the keys: hex, json
         * @throws IOException
         * 
         */
        public static void doDebugQueryKeys(AdminClient adminClient,
                                            List<Integer> nodeIds,
                                            List<String> storeNames,
                                            List<String> keyStrings,
                                            String keyType) throws IOException {
            // decide queryNode for storeDef
            Integer storeDefNodeId = nodeIds.get(0);
            Map<String, StoreDefinition> storeDefinitions = AdminToolUtils.getUserStoreDefMapOnNode(adminClient,
                                                                                                    storeDefNodeId);

            for(String keyString: keyStrings) {
                doDebugQueryKey(adminClient, nodeIds, storeNames, keyString, keyType);
            }
        }

        /**
         * Tells if serializer is avro schema
         * 
         * @param serializerName Serializer to be checked
         * @return
         */
        private static boolean isAvroSchema(String serializerName) {
            if(serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME)
               || serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME)
               || serializerName.equals(DefaultSerializerFactory.AVRO_REFLECTIVE_TYPE_NAME)
               || serializerName.equals(DefaultSerializerFactory.AVRO_SPECIFIC_TYPE_NAME)) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * debug route command
     */
    public static class SubCommandDebugRoute extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            AdminParserUtils.acceptsHex(parser); // either --hex or
                                                 // --json
            AdminParserUtils.acceptsJson(parser); // either --hex or
                                                  // --json
            AdminParserUtils.acceptsStoreSingle(parser);
            AdminParserUtils.acceptsUrl(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  debug route - Show detailed routing plan for a given set of keys on a store");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  debug route ((-x | -j) <key-list>) -s <store-name> -u <url>");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and shows detailed routing information for a
         * given set of keys on a store.
         * 
         * Usage: debug route ((-h | -j) <key-list>) -s <store-name> -u <url>
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws Exception
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws Exception {

            OptionParser parser = getParser();

            // declare parameters
            String keyType = null;
            List<String> keyStrings = null;
            String storeName = null;
            String url = null;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_HEX,
                                           AdminParserUtils.OPT_JSON);

            // load parameters
            if(options.has(AdminParserUtils.OPT_HEX)) {
                keyType = AdminParserUtils.OPT_HEX;
                keyStrings = (List<String>) options.valuesOf(AdminParserUtils.OPT_HEX);
            } else if(options.has(AdminParserUtils.OPT_JSON)) {
                keyType = AdminParserUtils.OPT_JSON;
                keyStrings = (List<String>) options.valuesOf(AdminParserUtils.OPT_JSON);
                throw new VoldemortException("Key type OPT_JSON not supported.");
            }
            storeName = (String) options.valueOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            // execute command
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            doDebugRoute(adminClient, storeName, keyStrings, keyType);
        }

        /**
         * Shows detailed routing information for a given set of keys on a
         * store.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param storeName Store that contains keys
         * @param keyStrings Keys to show routing plan
         * @param keyType Format of the keys: hex
         * @throws DecoderException
         * @throws IOException
         * 
         */
        public static void doDebugRoute(AdminClient adminClient,
                                        String storeName,
                                        List<String> keyStrings,
                                        String keyType) throws DecoderException {
            Cluster cluster = adminClient.getAdminClientCluster();
            List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList()
                                                                         .getValue();
            StoreDefinition storeDef = StoreDefinitionUtils.getStoreDefinitionWithName(storeDefs,
                                                                                       storeName);
            StoreRoutingPlan routingPlan = new StoreRoutingPlan(cluster, storeDef);
            BaseStoreRoutingPlan bRoutingPlan = new BaseStoreRoutingPlan(cluster, storeDef);

            final int COLUMN_WIDTH = 30;

            for(String keyStr: keyStrings) {
                byte[] key = ByteUtils.fromHexString(keyStr);
                System.out.println("Key :" + keyStr);
                System.out.println("Replicating Partitions :"
                                   + routingPlan.getReplicatingPartitionList(key));
                System.out.println("Replicating Nodes :");
                List<Integer> nodeList = routingPlan.getReplicationNodeList(routingPlan.getMasterPartitionId(key));
                for(int i = 0; i < nodeList.size(); i++) {
                    System.out.println(nodeList.get(i) + "\t"
                                       + cluster.getNodeById(nodeList.get(i)).getHost());
                }

                System.out.println("Zone Nary information :");
                HashMap<Integer, Integer> zoneRepMap = storeDef.getZoneReplicationFactor();

                for(Zone zone: cluster.getZones()) {
                    System.out.println("\tZone #" + zone.getId());
                    int numReplicas = -1;
                    if(zoneRepMap == null) {
                        // non zoned cluster
                        numReplicas = storeDef.getReplicationFactor();
                    } else {
                        // zoned cluster
                        if(!zoneRepMap.containsKey(zone.getId())) {
                            Utils.croak("Repfactor for Zone " + zone.getId()
                                        + " not found in storedef");
                        }
                        numReplicas = zoneRepMap.get(zone.getId());
                    }

                    String FormatString = "%s %s %s\n";
                    System.out.format(FormatString,
                                      Utils.paddedString("REPLICA#", COLUMN_WIDTH),
                                      Utils.paddedString("PARTITION", COLUMN_WIDTH),
                                      Utils.paddedString("NODE", COLUMN_WIDTH));
                    for(int i = 0; i < numReplicas; i++) {
                        Integer nodeId = bRoutingPlan.getNodeIdForZoneNary(zone.getId(), i, key);
                        Integer partitionId = routingPlan.getNodesPartitionIdForKey(nodeId, key);
                        System.out.format(FormatString,
                                          Utils.paddedString(i + "", COLUMN_WIDTH),
                                          Utils.paddedString(partitionId.toString(), COLUMN_WIDTH),
                                          Utils.paddedString(nodeId
                                                                     + "("
                                                                     + cluster.getNodeById(nodeId)
                                                                              .getHost() + ")",
                                                             COLUMN_WIDTH));
                    }
                    System.out.println();
                }

                System.out.println("-----------------------------------------------");
                System.out.println();
            }
        }
    }

}

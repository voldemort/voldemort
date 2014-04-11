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

package voldemort.tools.admin;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.codec.DecoderException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.sleepycat.persist.StoreNotFoundException;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.routing.StoreRoutingPlan;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * Implements all functionality of admin debug operations.
 * 
 */
public class AdminCommandDebug {

    /**
     * Main entry of group command 'debug', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
    	String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if (subCmd.compareTo("query-keys") == 0) executeDebugQueryKeys(args, printHelp);
        else if (subCmd.compareTo("route") == 0) executeDebugRoute(args, printHelp);
        else executeDebugHelp();
    }

    private static boolean isAvroSchema(String serializerName) {
        if (serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_REFLECTIVE_TYPE_NAME)
           || serializerName.equals(DefaultSerializerFactory.AVRO_SPECIFIC_TYPE_NAME)) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * Queries stores for a set of keys
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodes Nodes to query keys from
     * @param storeNames Stores to be queried
     * @param keyStrings Keys to be queried
     * @param keyType Format of the keys: hex, json
     * @throws IOException 
     * 
     */
    private static void doDebugQueryKeys(AdminClient adminClient, Collection<Node> nodes, List<String> storeNames,
                                  List<String> keyStrings, String keyType) throws IOException {
         // decide queryNode for storeDef
        Node storeDefNode = nodes.iterator().next();
        Map<String, StoreDefinition> storeDefinitions = AdminUtils.getUserStoreDefs(adminClient, storeDefNode);
        
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out));

        for (String keyString: keyStrings) {
            // iterate through stores
            for (final String storeName: storeNames) {
                // store definition
                StoreDefinition storeDefinition = storeDefinitions.get(storeName);
                if (storeDefinition == null) {
                    throw new StoreNotFoundException("Store " + storeName + " not found.");
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
                if (keySerializerDef != null && keySerializerDef.hasCompression()) {
                    keyCompressionStrategy = new CompressionStrategyFactory().get(keySerializerDef.getCompression());
                } else {
                    keyCompressionStrategy = null;
                }
                if (valueSerializerDef != null && valueSerializerDef.hasCompression()) {
                    valueCompressionStrategy = new CompressionStrategyFactory().get(valueSerializerDef.getCompression());
                } else {
                    valueCompressionStrategy = null;
                }
    
                if (keyCompressionStrategy == null) {
                    out.write("KEY_COMPRESSION_STRATEGY: None\n");
                } else {
                    out.write("KEY_COMPRESSION_STRATEGY: " + keyCompressionStrategy.getType() + "\n");
                }
                out.write("KEY_SERIALIZER_NAME: " + keySerializerDef.getName() + "\n");
                for (Map.Entry<Integer, String> entry: keySerializerDef.getAllSchemaInfoVersions()
                                                                      .entrySet()) {
                    out.write(String.format("KEY_SCHEMA VERSION=%d\n", entry.getKey()));
                    out.write("====================================\n");
                    out.write(entry.getValue());
                    out.write("\n====================================\n");
                }
                out.write("\n");
                if (valueCompressionStrategy == null) {
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
    
                // although the streamingOps support multiple keys, we only query one key here
                ByteArray key;
                try {
                    if (keyType.equals(AdminOptionParser.OPT_JSON)) {
                        Object keyObject;
                        String keySerializerName = keySerializerDef.getName();
                        if (isAvroSchema(keySerializerName)) {
                            Schema keySchema = Schema.parse(keySerializerDef.getCurrentSchemaInfo());
                            JsonDecoder decoder = new JsonDecoder(keySchema, keyString);
                            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(keySchema);
                            keyObject = datumReader.read(null, decoder);
                        } else if (keySerializerName.equals(DefaultSerializerFactory.JSON_SERIALIZER_TYPE_NAME)) {
                            JsonReader jsonReader = new JsonReader(new StringReader(keyString));
                            keyObject = jsonReader.read();
                        } else {
                            keyObject = keyString;
                        }
    
                        key = new ByteArray(keySerializer.toBytes(keyObject));
                    } else if (keyType.equals(AdminOptionParser.OPT_HEX)) {
                        key = new ByteArray(ByteUtils.fromHexString(keyString));
                    } else {
                        key = null;
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
                for (Node node: nodes) {
                    Iterator<QueryKeyResult> iterator;
                    iterator = adminClient.streamingOps.queryKeys(node.getId(),
                                                                  storeName,
                                                                  Arrays.asList(key).iterator());
                    //final StringWriter stringWriter = new StringWriter();
    
                    QueryKeyResult queryKeyResult = iterator.next();
                    // de-serialize and write key
                    byte[] keyBytes = queryKeyResult.getKey().get();
                    Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                              : keyCompressionStrategy.inflate(keyBytes));
    
                    if (!printedKey) {
                        out.write("KEY_BYTES\n====================================\n");
                        out.write(queryKeyResult.getKey().toString());
                        out.write("\n====================================\n");
                        out.write("KEY_TEXT\n====================================\n");
                        if (keyObject instanceof GenericRecord) {
                            out.write(keyObject.toString());
                        } else {
                            new JsonFactory(new ObjectMapper()).createJsonGenerator(out)
                                                               .writeObject(keyObject);
                        }
                        out.write("\n====================================\n\n");
                        printedKey = true;
                    }
                    out.write(String.format("\nQueried node %d on store %s\n", node.getId(), storeName));
    
                    // iterate through, de-serialize and write values
                    if (queryKeyResult.hasValues() && queryKeyResult.getValues().size() > 0) {
                        int versionCount = 0;
    
                        out.write("VALUE " + versionCount + "\n");
    
                        for (Versioned<byte[]> versioned: queryKeyResult.getValues()) {
    
                            // write version
                            VectorClock version = (VectorClock) versioned.getVersion();
                            out.write("VECTOR_CLOCK_BYTE: " + ByteUtils.toHexString(version.toBytes())
                                      + "\n");
                            out.write("VECTOR_CLOCK_TEXT: " + version.toString() + '['
                                      + new Date(version.getTimestamp()).toString() + "]\n");
    
                            // write value
                            byte[] valueBytes = versioned.getValue();
                            out.write("VALUE_BYTE\n====================================\n");
                            out.write(ByteUtils.toHexString(valueBytes));
                            out.write("\n====================================\n");
                            out.write("VALUE_TEXT\n====================================\n");
                            Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                                            : valueCompressionStrategy.inflate(valueBytes));
                            if (valueObject instanceof GenericRecord) {
                                out.write(valueObject.toString());
                            } else {
                                new JsonFactory(new ObjectMapper()).createJsonGenerator(out)
                                                                   .writeObject(valueObject);
                            }
                            out.write("\n====================================\n");
                            versionCount++;
                        }
                    } else {
                        out.write("VALUE_RESPONSE\n====================================\n");
                        // write null or exception
                        if (queryKeyResult.hasException()) {
                            out.write(queryKeyResult.getException().toString());
                        } else {
                            out.write("null");
                        }
                        out.write("\n====================================\n");
                    }
                    out.flush();
                }
            }
        }
    }

    /**
     * Shows detailed routing information for a given set of keys on a store.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param storeName Store that contains keys
     * @param keyStrings Keys to show routing plan
     * @param keyType Format of the keys: hex
     * @throws DecoderException 
     * @throws IOException 
     * 
     */
    private static void doDebugRoute(AdminClient adminClient, String storeName, List<String> keyStrings, String keyType)
            throws DecoderException {
        Cluster cluster = adminClient.getAdminClientCluster();
        List<StoreDefinition> storeDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(0)
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
                if (zoneRepMap == null) {
                    // non zoned cluster
                    numReplicas = storeDef.getReplicationFactor();
                } else {
                    // zoned cluster
                    if (!zoneRepMap.containsKey(zone.getId())) {
                        Utils.croak("Repfactor for Zone " + zone.getId() + " not found in storedef");
                    }
                    numReplicas = zoneRepMap.get(zone.getId());
                }

                System.out.format("%s%s%s\n",
                                  Utils.paddedString("REPLICA#", COLUMN_WIDTH),
                                  Utils.paddedString("PARTITION", COLUMN_WIDTH),
                                  Utils.paddedString("NODE", COLUMN_WIDTH));
                for(int i = 0; i < numReplicas; i++) {
                    Integer nodeId = bRoutingPlan.getNodeIdForZoneNary(zone.getId(), i, key);
                    Integer partitionId = routingPlan.getNodesPartitionIdForKey(nodeId, key);
                    System.out.format("%s%s%s\n",
                                      Utils.paddedString(i + "", COLUMN_WIDTH),
                                      Utils.paddedString(partitionId.toString(), COLUMN_WIDTH),
                                      Utils.paddedString(nodeId + "("
                                                         + cluster.getNodeById(nodeId).getHost()
                                                         + ")", COLUMN_WIDTH));
                }
                System.out.println();
            }

            System.out.println("-----------------------------------------------");
            System.out.println();
        }
    }

    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeDebugHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Debug Commands");
        System.out.println("-----------------------------------");
        System.out.println("query-keys   Query stores for a set of keys.");
        System.out.println("route        Show detailed routing plan for a given set of keys on a store.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help debug <command-name>\'.");
        System.out.println();
    }

    /**
     * Parses command-line and queries stores for a set of keys
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeDebugQueryKeys(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String keyType = null;
        List<String> keyStrings = null;
        List<String> storeNames = null;
        String url = null;
        List<Integer> nodeIds = null;
        Boolean allNodes = true;

        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEX, AdminOptionParser.OPT_JSON);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_NODE_MULTIPLE, AdminOptionParser.OPT_ALL_NODES);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  debug query-keys - Query stores for a set of keys");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  debug query-keys ((-h | -j) <key-list>) -s <store-name-list> -u <url>");
            System.out.println("                   [-n <node-id-list> | --all-nodes]");
            System.out.println();
            parser.printHelp();
            System.out.println();
            return;
        }

        // parse command-line input
        try {
            parser.parse(args, 0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // load parameters
        if (parser.hasOption(AdminOptionParser.OPT_HEX)) {
            keyType = AdminOptionParser.OPT_HEX;
            keyStrings = (List<String>) parser.getValueList(AdminOptionParser.OPT_HEX);
        } else if (parser.hasOption(AdminOptionParser.OPT_JSON)) {
            keyType = AdminOptionParser.OPT_JSON;
            keyStrings = (List<String>) parser.getValueList(AdminOptionParser.OPT_JSON);
        }
        storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_NODE)) {
            nodeIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_NODE);
            allNodes = false;
        }

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

        doDebugQueryKeys(adminClient, nodes, storeNames, keyStrings, keyType);
    }

    /**
     * Parses command-line and shows detailed routing information for a given set of keys on a store.
     * 
     * Usage:
     * debug route ((-h | -j)  <key-list>) -s <store-name> -u <url>
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeDebugRoute(String[] args, Boolean printHelp) throws Exception {

        AdminOptionParser parser = new AdminOptionParser();

        // declare parameters
        String keyType = null;
        List<String> keyStrings = null;
        String storeName = null;
        String url = null;

        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_HEX, AdminOptionParser.OPT_JSON);
        parser.addRequired(AdminOptionParser.OPT_STORE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  debug route - Show detailed routing plan for a given set of keys on a store");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  debug route ((-h | -j) <key-list>) -s <store-name> -u <url>");
            System.out.println();
            parser.printHelp();
            System.out.println();
            return;
        }

        // parse command-line input
        try {
            parser.parse(args, 0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // load parameters
        if (parser.hasOption(AdminOptionParser.OPT_HEX)) {
            keyType = AdminOptionParser.OPT_HEX;
            keyStrings = (List<String>) parser.getValueList(AdminOptionParser.OPT_HEX);
        } else if (parser.hasOption(AdminOptionParser.OPT_JSON)) {
            keyType = AdminOptionParser.OPT_JSON;
            keyStrings = (List<String>) parser.getValueList(AdminOptionParser.OPT_JSON);
            throw new VoldemortException("Key type OPT_JSON not supported.");
        }
        storeName = (String) parser.getValue(AdminOptionParser.OPT_STORE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);

        AdminClient adminClient = AdminUtils.getAdminClient(url);

        doDebugRoute(adminClient, storeName, keyStrings, keyType);
    }

}

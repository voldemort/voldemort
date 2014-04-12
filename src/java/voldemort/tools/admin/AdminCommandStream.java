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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Implements all stream commands.
 */
public class AdminCommandStream extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.compareTo("fetch-entries") == 0) {
            SubCommandStreamFetchEntries.executeCommand(args);
        } else if(subCmd.compareTo("fetch-keys") == 0) {
            SubCommandStreamFetchKeys.executeCommand(args);
        } else if(subCmd.compareTo("mirror") == 0) {
            SubCommandStreamMirror.executeCommand(args);
        } else if(subCmd.compareTo("update-entries") == 0) {
            SubCommandStreamUpdateEntries.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Stream Commands");
        stream.println("------------------------------------");
        stream.println("fetch-entries    Fetch entries from a node.");
        stream.println("fetch-keys       Fetch keys from a node.");
        stream.println("mirror           Mirror data from a node to another.");
        stream.println("update-entries   Update entries from file to a node.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help stream <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.compareTo("fetch-entries") == 0) {
            SubCommandStreamFetchEntries.printHelp(stream);
        } else if(subCmd.compareTo("fetch-keys") == 0) {
            SubCommandStreamFetchKeys.printHelp(stream);
        } else if(subCmd.compareTo("mirror") == 0) {
            SubCommandStreamMirror.printHelp(stream);
        } else if(subCmd.compareTo("update-entries") == 0) {
            SubCommandStreamUpdateEntries.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * stream fetch-entries command
     */
    private static class SubCommandStreamFetchEntries extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsPartition(parser, false); // --partition or
                                                              // --all-partitions
                                                              // or --orphaned
            AdminParserUtils.acceptsAllPartitions(parser); // --partition or
                                                           // --all-partitions
                                                           // or --orphaned
            AdminParserUtils.acceptsOrphaned(parser); // --partition or
                                                      // --all-partitions or
                                                      // --orphaned
            AdminParserUtils.acceptsStoreMultiple(parser, false); // either
                                                                  // --store or
                                                                  // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsDir(parser, false);
            AdminParserUtils.acceptsFormat(parser, false);
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
            stream.println("  stream fetch-entries - Fetch entries from a node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  stream fetch-entries -n <node-id>");
            stream.println("                       (-p <partition-id-list> | --all-partitions | --orphaned)");
            stream.println("                       (-s <store-name-list> | --all-stores) -u <url>");
            stream.println("                       [-d <output-dir>] [--format json | binary | hex]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and fetches entries from a given node.
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> requiredPart = Lists.newArrayList();
            List<String> requiredStore = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_URL);
            requiredPart.add(AdminParserUtils.OPT_PARTITION);
            requiredPart.add(AdminParserUtils.OPT_ALL_PARTITIONS);
            requiredPart.add(AdminParserUtils.OPT_ORPHANED);
            requiredStore.add(AdminParserUtils.OPT_STORE);
            requiredStore.add(AdminParserUtils.OPT_ALL_STORES);

            // declare parameters
            Integer nodeId = null;
            List<Integer> partIds = null;
            Boolean allParts = false;
            Boolean orphaned = false;
            List<String> storeNames = null;
            Boolean allStores = false;
            String url = null;
            String dir = null;
            String format = null;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            if(options.has(AdminParserUtils.OPT_PARTITION)) {
                partIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_PARTITION);
                allParts = false;
                orphaned = false;
            } else if(options.has(AdminParserUtils.OPT_ALL_PARTITIONS)) {
                allParts = true;
                orphaned = false;
            } else if(options.has(AdminParserUtils.OPT_ORPHANED)) {
                allParts = false;
                orphaned = true;
            }
            if(options.has(AdminParserUtils.OPT_STORE)) {
                storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
                allStores = false;
            } else {
                allStores = true;
            }
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_DIR)) {
                dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            }
            if(options.has(AdminParserUtils.OPT_FORMAT)) {
                format = (String) options.valueOf(AdminParserUtils.OPT_FORMAT);
            } else {
                format = AdminParserUtils.ARG_FORMAT_JSON;
            }

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkRequiredOne(options, requiredPart);
            AdminParserUtils.checkRequiredOne(options, requiredStore);

            // execute command
            File directory = AdminUtils.createDir(dir);
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
            if(!orphaned)
                partIds = AdminUtils.getPartitions(adminClient, partIds, allParts);
            storeNames = AdminUtils.getUserStoresOnNode(adminClient, node, storeNames, allStores);

            doStreamFetchEntries(adminClient,
                                 node,
                                 storeNames,
                                 partIds,
                                 orphaned,
                                 directory,
                                 format);
        }

        /**
         * Fetches entries from a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to fetch entries from
         * @param storeNames List of stores to fetch from
         * @param partIds List of partitions to fetch from
         * @param orphaned Tells if orphaned entries to be fetched
         * @param directory File object of directory to output to
         * @param format output format
         * @throws IOException
         */
        private static void doStreamFetchEntries(AdminClient adminClient,
                                                 Node node,
                                                 List<String> storeNames,
                                                 List<Integer> partIds,
                                                 Boolean orphaned,
                                                 File directory,
                                                 String format) throws IOException {
            HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
            storeDefinitionMap.putAll(AdminUtils.getUserStoreDefs(adminClient, node));
            storeDefinitionMap.putAll(AdminUtils.getSystemStoreDefs());

            for(String store: storeNames) {

                StoreDefinition storeDefinition = storeDefinitionMap.get(store);

                if(null == storeDefinition) {
                    System.out.println("No store found under the name \'" + store + "\'");
                    continue;
                }

                Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIteratorRef = null;

                if(orphaned) {
                    System.out.println("Fetching orphaned entries of " + store);
                    entryIteratorRef = adminClient.bulkFetchOps.fetchOrphanedEntries(node.getId(),
                                                                                     store);
                } else {
                    System.out.println("Fetching entries in partitions "
                                       + Joiner.on(", ").join(partIds) + " of " + store);
                    entryIteratorRef = adminClient.bulkFetchOps.fetchEntries(node.getId(),
                                                                             store,
                                                                             partIds,
                                                                             null,
                                                                             false);
                }

                File outFile = null;
                if(directory != null) {
                    outFile = new File(directory, store + ".entries");
                }

                final Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = entryIteratorRef;

                if(format.compareTo(AdminParserUtils.ARG_FORMAT_JSON) == 0) {
                    // k-v serializer
                    SerializerDefinition keySerializerDef = storeDefinition.getKeySerializer();
                    SerializerDefinition valueSerializerDef = storeDefinition.getValueSerializer();
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

                    writeAscii(outFile, new Writable() {

                        @Override
                        public void writeTo(BufferedWriter out) throws IOException {

                            while(entryIterator.hasNext()) {
                                final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(out);
                                Pair<ByteArray, Versioned<byte[]>> kvPair = entryIterator.next();
                                byte[] keyBytes = kvPair.getFirst().get();
                                byte[] valueBytes = kvPair.getSecond().getValue();
                                VectorClock version = (VectorClock) kvPair.getSecond().getVersion();

                                Object keyObject = keySerializer.toObject((null == keyCompressionStrategy) ? keyBytes
                                                                                                          : keyCompressionStrategy.inflate(keyBytes));
                                Object valueObject = valueSerializer.toObject((null == valueCompressionStrategy) ? valueBytes
                                                                                                                : valueCompressionStrategy.inflate(valueBytes));
                                if(keyObject instanceof GenericRecord) {
                                    out.write(keyObject.toString());
                                } else {
                                    generator.writeObject(keyObject);
                                }
                                out.write(' ' + version.toString() + ' ');
                                if(valueObject instanceof GenericRecord) {
                                    out.write(valueObject.toString());
                                } else {
                                    generator.writeObject(valueObject);
                                }
                                out.write('\n');
                            }
                        }
                    });
                } else if(format.compareTo(AdminParserUtils.ARG_FORMAT_HEX) == 0) {
                    writeBinary(outFile, new Printable() {

                        @Override
                        public void printTo(DataOutputStream out) throws IOException {
                            while(entryIterator.hasNext()) {
                                Pair<ByteArray, Versioned<byte[]>> kvPair = entryIterator.next();
                                byte[] keyBytes = kvPair.getFirst().get();
                                VectorClock clock = ((VectorClock) kvPair.getSecond().getVersion());
                                byte[] valueBytes = kvPair.getSecond().getValue();

                                out.writeChars(ByteUtils.toHexString(keyBytes));
                                out.writeChars(",");
                                out.writeChars(clock.toString());
                                out.writeChars(",");
                                out.writeChars(ByteUtils.toHexString(valueBytes));
                                out.writeChars("\n");
                            }
                        }
                    });
                } else if(format.compareTo(AdminParserUtils.ARG_FORMAT_BINARY) == 0) {
                    // TODO
                    throw new VoldemortException("--format binary   not implemented yet.");
                } else
                    throw new VoldemortException("Specify valid format: json, binary, hex.");

                if(outFile != null) {
                    System.out.println("Fetched entries from " + store + " to " + outFile);
                }
            }
        }
    }

    /**
     * stream fetch-keys command
     */
    private static class SubCommandStreamFetchKeys extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsPartition(parser, false); // --partition or
                                                              // --all-partitions
                                                              // or --orphaned
            AdminParserUtils.acceptsAllPartitions(parser); // --partition or
                                                           // --all-partitions
                                                           // or --orphaned
            AdminParserUtils.acceptsOrphaned(parser); // --partition or
                                                      // --all-partitions or
                                                      // --orphaned
            AdminParserUtils.acceptsStoreMultiple(parser, false); // either
                                                                  // --store or
                                                                  // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsDir(parser, false);
            AdminParserUtils.acceptsFormat(parser, false);
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
            stream.println("  stream fetch-keys - Fetch keys from a node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  stream fetch-keys -n <node-id>");
            stream.println("                    (-p <partition-id-list> | --all-partitions | --orphaned)");
            stream.println("                    (-s <store-name-list> | --all-stores) -u <url>");
            stream.println("                    [-d <output-dir>] [--format json | binary | hex]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and fetches keys from a given node.
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> requiredPart = Lists.newArrayList();
            List<String> requiredStore = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_URL);
            requiredPart.add(AdminParserUtils.OPT_PARTITION);
            requiredPart.add(AdminParserUtils.OPT_ALL_PARTITIONS);
            requiredPart.add(AdminParserUtils.OPT_ORPHANED);
            requiredStore.add(AdminParserUtils.OPT_STORE);
            requiredStore.add(AdminParserUtils.OPT_ALL_STORES);

            // declare parameters
            Integer nodeId = null;
            List<Integer> partIds = null;
            Boolean allParts = false;
            Boolean orphaned = false;
            List<String> storeNames = null;
            Boolean allStores = false;
            String url = null;
            String dir = null;
            String format = null;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            if(options.has(AdminParserUtils.OPT_PARTITION)) {
                partIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_PARTITION);
                allParts = false;
                orphaned = false;
            } else if(options.has(AdminParserUtils.OPT_ALL_PARTITIONS)) {
                allParts = true;
                orphaned = false;
            } else if(options.has(AdminParserUtils.OPT_ORPHANED)) {
                allParts = false;
                orphaned = true;
            }
            if(options.has(AdminParserUtils.OPT_STORE)) {
                storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
                allStores = false;
            } else {
                allStores = true;
            }
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_DIR)) {
                dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            }
            if(options.has(AdminParserUtils.OPT_FORMAT)) {
                format = (String) options.valueOf(AdminParserUtils.OPT_FORMAT);
            } else {
                format = AdminParserUtils.ARG_FORMAT_JSON;
            }

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkRequiredOne(options, requiredPart);
            AdminParserUtils.checkRequiredOne(options, requiredStore);

            // execute command
            File directory = AdminUtils.createDir(dir);
            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
            if(!orphaned)
                partIds = AdminUtils.getPartitions(adminClient, partIds, allParts);
            storeNames = AdminUtils.getUserStoresOnNode(adminClient, node, storeNames, allStores);

            doStreamFetchKeys(adminClient, node, storeNames, partIds, orphaned, directory, format);
        }

        /**
         * Fetches keys from a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to fetch entries from
         * @param storeNames List of stores to fetch from
         * @param partIds List of partitions to fetch from
         * @param orphaned Tells if orphaned entries to be fetched
         * @param directory File object of directory to output to
         * @param format output format
         * @throws IOException
         */
        private static void doStreamFetchKeys(AdminClient adminClient,
                                              Node node,
                                              List<String> storeNames,
                                              List<Integer> partIds,
                                              Boolean orphaned,
                                              File directory,
                                              String format) throws IOException {
            HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
            storeDefinitionMap.putAll(AdminUtils.getUserStoreDefs(adminClient, node));
            storeDefinitionMap.putAll(AdminUtils.getSystemStoreDefs());

            for(String store: storeNames) {

                StoreDefinition storeDefinition = storeDefinitionMap.get(store);

                if(null == storeDefinition) {
                    System.out.println("No store found under the name \'" + store + "\'");
                    continue;
                }

                Iterator<ByteArray> keyIteratorRef = null;

                if(orphaned) {
                    System.out.println("Fetching orphaned keys of " + store);
                    keyIteratorRef = adminClient.bulkFetchOps.fetchOrphanedKeys(node.getId(), store);
                } else {
                    System.out.println("Fetching keys in partitions "
                                       + Joiner.on(", ").join(partIds) + " of " + store);
                    keyIteratorRef = adminClient.bulkFetchOps.fetchKeys(node.getId(),
                                                                        store,
                                                                        partIds,
                                                                        null,
                                                                        false);
                }

                final Iterator<ByteArray> keyIterator = keyIteratorRef;
                File outFile = null;
                if(directory != null) {
                    outFile = new File(directory, store + ".keys");
                }

                if(format.compareTo(AdminParserUtils.ARG_FORMAT_JSON) == 0) {
                    final SerializerDefinition serializerDef = storeDefinition.getKeySerializer();
                    final SerializerFactory serializerFactory = new DefaultSerializerFactory();
                    @SuppressWarnings("unchecked")
                    final Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);

                    final CompressionStrategy keysCompressionStrategy;
                    if(serializerDef != null && serializerDef.hasCompression()) {
                        keysCompressionStrategy = new CompressionStrategyFactory().get(serializerDef.getCompression());
                    } else {
                        keysCompressionStrategy = null;
                    }

                    writeAscii(outFile, new Writable() {

                        @Override
                        public void writeTo(BufferedWriter out) throws IOException {

                            while(keyIterator.hasNext()) {
                                final JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(out);

                                byte[] keyBytes = keyIterator.next().get();
                                Object keyObject = serializer.toObject((null == keysCompressionStrategy) ? keyBytes
                                                                                                        : keysCompressionStrategy.inflate(keyBytes));

                                if(keyObject instanceof GenericRecord) {
                                    out.write(keyObject.toString());
                                } else {
                                    generator.writeObject(keyObject);
                                }
                                out.write('\n');
                            }
                        }
                    });
                } else if(format.compareTo(AdminParserUtils.ARG_FORMAT_HEX) == 0) {
                    writeBinary(outFile, new Printable() {

                        @Override
                        public void printTo(DataOutputStream out) throws IOException {
                            while(keyIterator.hasNext()) {
                                byte[] keyBytes = keyIterator.next().get();
                                out.writeChars(ByteUtils.toHexString(keyBytes) + "\n");
                            }
                        }
                    });
                } else if(format.compareTo(AdminParserUtils.ARG_FORMAT_BINARY) == 0) {
                    // TODO
                    throw new VoldemortException("--format binary   not implemented yet.");
                } else
                    throw new VoldemortException("Specify valid format: json, binary, hex.");

                if(outFile != null) {
                    System.out.println("Fetched keys from " + store + " to " + outFile);
                }
            }
        }
    }

    /**
     * stream mirror command
     */
    private static class SubCommandStreamMirror extends AbstractAdminCommand {

        public static final String OPT_FROM_NODE = "from-node";
        public static final String OPT_FROM_URL = "from-url";
        public static final String OPT_TO_NODE = "to-node";
        public static final String OPT_TO_URL = "to-url";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            parser.accepts(OPT_FROM_URL, "mirror source bootstrap url")
                  .withRequiredArg()
                  .describedAs("src-url")
                  .ofType(String.class);
            parser.accepts(OPT_FROM_NODE, "mirror source node id")
                  .withRequiredArg()
                  .describedAs("src-node-id")
                  .ofType(Integer.class);
            parser.accepts(OPT_TO_URL, "mirror destination bootstrap url")
                  .withRequiredArg()
                  .describedAs("d-url")
                  .ofType(String.class);
            parser.accepts(OPT_TO_NODE, "mirror destination node id")
                  .withRequiredArg()
                  .describedAs("dest-node-id")
                  .ofType(Integer.class);
            AdminParserUtils.acceptsStoreMultiple(parser, false); // either
                                                                  // --store or
                                                                  // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
            // optional options
            AdminParserUtils.acceptsConfirm(parser);
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
            stream.println("  stream mirror - Mirror data from a node to another");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  stream mirror --from-url <src-url> --from-node <src-node-id>");
            stream.println("                --to-url <dest-url> --to-node <dest-node-id>");
            stream.println("                (-s <store-name-list> | --all-stores) [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and mirrors data from a node to another.
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
            List<String> requiredAll = Lists.newArrayList();
            List<String> requiredStore = Lists.newArrayList();
            requiredAll.add(OPT_FROM_URL);
            requiredAll.add(OPT_FROM_NODE);
            requiredAll.add(OPT_TO_URL);
            requiredAll.add(OPT_TO_NODE);
            requiredStore.add(AdminParserUtils.OPT_STORE);
            requiredStore.add(AdminParserUtils.OPT_ALL_STORES);

            // declare parameters
            Integer srcNodeId = null, destNodeId = null;
            String srcUrl = null, destUrl = null;
            List<String> storeNames = null;
            Boolean allStores = false;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            srcUrl = (String) options.valueOf(OPT_FROM_URL);
            destUrl = (String) options.valueOf(OPT_TO_URL);
            srcNodeId = (Integer) options.valueOf(OPT_FROM_NODE);
            destNodeId = (Integer) options.valueOf(OPT_TO_NODE);
            if(options.has(AdminParserUtils.OPT_STORE)) {
                storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
                allStores = false;
            } else {
                allStores = true;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);
            AdminParserUtils.checkRequiredOne(options, requiredStore);

            // execute command
            if(!AdminUtils.askConfirm(confirm, "mirror stores from " + srcUrl + ":" + srcNodeId
                                               + " to " + destUrl + ":" + destNodeId)) {
                return;
            }

            AdminClient srcAdminClient = AdminUtils.getAdminClient(srcUrl);
            AdminClient destAdminClient = AdminUtils.getAdminClient(destUrl);
            Node srcNode = srcAdminClient.getAdminClientCluster().getNodeById(srcNodeId);
            // Node destNode =
            // destAdminClient.getAdminClientCluster().getNodeById(srcNodeId);
            storeNames = AdminUtils.getUserStoresOnNode(srcAdminClient,
                                                        srcNode,
                                                        storeNames,
                                                        allStores);

            destAdminClient.restoreOps.mirrorData(destNodeId, srcNodeId, srcUrl, storeNames);
        }
    }

    /**
     * stream update-entries command
     */
    private static class SubCommandStreamUpdateEntries extends AbstractAdminCommand {

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // required options
            AdminParserUtils.acceptsDir(parser, true);
            AdminParserUtils.acceptsNodeSingle(parser, true);
            AdminParserUtils.acceptsUrl(parser, true);
            // optional options
            AdminParserUtils.acceptsStoreMultiple(parser, false);
            AdminParserUtils.acceptsConfirm(parser);
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
            stream.println("  stream update-entries - Update entries from file to a node");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  stream update-entries -d <input-dir> -n <node-id> -u <url>");
            stream.println("                        [-s <store-name-list>] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and updates entries on a given node.
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
            List<String> requiredAll = Lists.newArrayList();
            requiredAll.add(AdminParserUtils.OPT_DIR);
            requiredAll.add(AdminParserUtils.OPT_NODE);
            requiredAll.add(AdminParserUtils.OPT_URL);

            // declare parameters
            String dir = null;
            Integer nodeId = null;
            String url = null;
            List<String> storeNames = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);

            // load parameters
            dir = (String) options.valueOf(AdminParserUtils.OPT_DIR);
            nodeId = (Integer) options.valueOf(AdminParserUtils.OPT_NODE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            if(options.has(AdminParserUtils.OPT_STORE)) {
                storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // check correctness
            AdminParserUtils.checkRequiredAll(options, requiredAll);

            // execute command
            if(!AdminUtils.askConfirm(confirm, "update entries")) {
                return;
            }

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
            File inDir = new File(dir);
            if(!inDir.exists()) {
                throw new FileNotFoundException("Input directory " + dir + " doesn't exist");
            }

            doStreamUpdateEntries(adminClient, node, storeNames, inDir);
        }

        /**
         * Updates entries on a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param node Node to update entries to
         * @param storeNames Stores to update entries
         * @param inDir File object of directory to input entries from
         * @throws IOException
         * 
         */
        private static void doStreamUpdateEntries(AdminClient adminClient,
                                                  Node node,
                                                  List<String> storeNames,
                                                  File inDir) throws IOException {
            if(storeNames == null) {
                storeNames = Lists.newArrayList();
                for(File storeFile: inDir.listFiles()) {
                    String fileName = storeFile.getName();
                    if(fileName.endsWith(".entries")) {
                        int extPosition = fileName.lastIndexOf(".entries");
                        storeNames.add(fileName.substring(0, extPosition));
                    }
                }
            }
            for(String storeName: storeNames) {
                Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = readEntriesBinary(inDir,
                                                                                          storeName);
                adminClient.streamingOps.updateEntries(node.getId(), storeName, iterator, null);
            }
        }
    }

    private abstract static class Printable {

        public abstract void printTo(DataOutputStream out) throws IOException;
    }

    private abstract static class Writable {

        public abstract void writeTo(BufferedWriter out) throws IOException;
    }

    private static Iterator<Pair<ByteArray, Versioned<byte[]>>> readEntriesBinary(File inputDir,
                                                                                  String storeName)
            throws IOException {
        File inputFile = new File(inputDir, storeName + ".entries");
        if(!inputFile.exists()) {
            throw new FileNotFoundException("File " + inputFile.getAbsolutePath()
                                            + " does not exist!");
        }

        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)));

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    int length = dis.readInt();
                    byte[] keyBytes = new byte[length];
                    ByteUtils.read(dis, keyBytes);
                    length = dis.readInt();
                    byte[] versionBytes = new byte[length];
                    ByteUtils.read(dis, versionBytes);
                    length = dis.readInt();
                    byte[] valueBytes = new byte[length];
                    ByteUtils.read(dis, valueBytes);

                    ByteArray key = new ByteArray(keyBytes);
                    VectorClock version = new VectorClock(versionBytes);
                    Versioned<byte[]> value = new Versioned<byte[]>(valueBytes, version);

                    return new Pair<ByteArray, Versioned<byte[]>>(key, value);
                } catch(EOFException e) {
                    try {
                        dis.close();
                    } catch(IOException ie) {
                        ie.printStackTrace();
                    }
                    return endOfData();
                } catch(IOException e) {
                    try {
                        dis.close();
                    } catch(IOException ie) {
                        ie.printStackTrace();
                    }
                    throw new VoldemortException("Error reading from input file ", e);
                }
            }
        };
    }

    private static void writeBinary(File outputFile, Printable printable) throws IOException {
        OutputStream outputStream = null;
        if(outputFile == null) {
            outputStream = new FilterOutputStream(System.out) {

                @Override
                public void close() throws IOException {
                    flush();
                }
            };
        } else {
            outputStream = new FileOutputStream(outputFile);
        }
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(outputStream));
        try {
            printable.printTo(dataOutputStream);
        } finally {
            dataOutputStream.close();
        }
    }

    private static void writeAscii(File outputFile, Writable writable) throws IOException {
        Writer writer = null;
        if(outputFile == null) {
            writer = new OutputStreamWriter(new FilterOutputStream(System.out) {

                @Override
                public void close() throws IOException {
                    flush();
                }
            });
        } else {
            writer = new FileWriter(outputFile);
        }
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        try {
            writable.writeTo(bufferedWriter);
        } finally {
            bufferedWriter.close();
        }
    }
}

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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminToolUtils;
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
        args = AdminToolUtils.copyArrayCutFirst(args);
        if(subCmd.equals("fetch-entries")) {
            SubCommandStreamFetchEntries.executeCommand(args);
        } else if(subCmd.equals("fetch-keys")) {
            SubCommandStreamFetchKeys.executeCommand(args);
        } else if(subCmd.equals("mirror")) {
            SubCommandStreamMirror.executeCommand(args);
        } else if(subCmd.equals("update-entries")) {
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
        if(subCmd.equals("fetch-entries")) {
            SubCommandStreamFetchEntries.printHelp(stream);
        } else if(subCmd.equals("fetch-keys")) {
            SubCommandStreamFetchKeys.printHelp(stream);
        } else if(subCmd.equals("mirror")) {
            SubCommandStreamMirror.printHelp(stream);
        } else if(subCmd.equals("update-entries")) {
            SubCommandStreamUpdateEntries.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * stream fetch-entries command
     */
    public static class SubCommandStreamFetchEntries extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsNodeSingle(parser);
            AdminParserUtils.acceptsPartition(parser); // --partition or
                                                       // --all-partitions
                                                       // or --orphaned
            AdminParserUtils.acceptsAllPartitions(parser); // --partition or
                                                           // --all-partitions
                                                           // or --orphaned
            AdminParserUtils.acceptsOrphaned(parser); // --partition or
                                                      // --all-partitions or
                                                      // --orphaned
            AdminParserUtils.acceptsStoreMultiple(parser); // either
                                                           // --store or
                                                           // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsDir(parser);
            AdminParserUtils.acceptsFormat(parser);
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
            stream.println("                       [-d <output-dir>] [--format json | hex]");
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
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_PARTITION,
                                           AdminParserUtils.OPT_ALL_PARTITIONS,
                                           AdminParserUtils.OPT_ORPHANED);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_STORE,
                                           AdminParserUtils.OPT_ALL_STORES);

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

            // execute command
            File directory = AdminToolUtils.createDir(dir);
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(!orphaned && allParts) {
                partIds = AdminToolUtils.getAllPartitions(adminClient);
            }

            if(allStores) {
                storeNames = AdminToolUtils.getAllUserStoreNamesOnNode(adminClient, nodeId);
            } else {
                AdminToolUtils.validateUserStoreNamesOnNode(adminClient, nodeId, storeNames);
            }

            doStreamFetchEntries(adminClient,
                                 nodeId,
                                 storeNames,
                                 partIds,
                                 orphaned,
                                 directory,
                                 format);
        }

        public static void writeObjectAsJson(BufferedWriter out,
                                             Object object,
                                             JsonGenerator generator)
                throws IOException {
            if(object instanceof GenericRecord) {
                out.write(object.toString());
            } else if(object instanceof Utf8) {
                out.write(object.toString());
            } else if(object instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) object;
                out.write(ByteUtils.toHexString(buffer.array()));
            } else {
                generator.writeObject(object);
            }
        }

        /**
         * Fetches entries from a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeId Node id to fetch entries from
         * @param storeNames List of stores to fetch from
         * @param partIds List of partitions to fetch from
         * @param orphaned Tells if orphaned entries to be fetched
         * @param directory File object of directory to output to
         * @param format output format
         * @throws IOException
         */
        public static void doStreamFetchEntries(AdminClient adminClient,
                                                Integer nodeId,
                                                List<String> storeNames,
                                                List<Integer> partIds,
                                                Boolean orphaned,
                                                File directory,
                                                String format) throws IOException {
            HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
            storeDefinitionMap.putAll(AdminToolUtils.getUserStoreDefMapOnNode(adminClient, nodeId));
            storeDefinitionMap.putAll(AdminToolUtils.getSystemStoreDefMap());

            for(String store: storeNames) {

                StoreDefinition storeDefinition = storeDefinitionMap.get(store);

                if(null == storeDefinition) {
                    System.out.println("No store found under the name \'" + store + "\'");
                    continue;
                }

                Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIteratorRef = null;

                if(orphaned) {
                    System.out.println("Fetching orphaned entries of " + store);
                    entryIteratorRef = adminClient.bulkFetchOps.fetchOrphanedEntries(nodeId, store);
                } else {
                    System.out.println("Fetching entries in partitions "
                                       + Joiner.on(", ").join(partIds) + " of " + store);
                    entryIteratorRef = adminClient.bulkFetchOps.fetchEntries(nodeId,
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

                if(format.equals(AdminParserUtils.ARG_FORMAT_JSON)) {
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

                                writeObjectAsJson(out, keyObject, generator);
                                out.write(' ' + version.toString() + ' ');
                                writeObjectAsJson(out, valueObject, generator);
                                out.write('\n');
                            }
                        }
                    });
                } else if(format.equals(AdminParserUtils.ARG_FORMAT_HEX)) {
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
                } else {
                    throw new VoldemortException("Invalid format \'" + format + "\'.");
                }

                if(outFile != null) {
                    System.out.println("Fetched entries from " + store + " to " + outFile);
                }
            }
        }
    }

    /**
     * stream fetch-keys command
     */
    public static class SubCommandStreamFetchKeys extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsNodeSingle(parser);
            AdminParserUtils.acceptsPartition(parser); // --partition or
                                                       // --all-partitions
                                                       // or --orphaned
            AdminParserUtils.acceptsAllPartitions(parser); // --partition or
                                                           // --all-partitions
                                                           // or --orphaned
            AdminParserUtils.acceptsOrphaned(parser); // --partition or
                                                      // --all-partitions or
                                                      // --orphaned
            AdminParserUtils.acceptsStoreMultiple(parser); // either
                                                           // --store or
                                                           // --all-stores
            AdminParserUtils.acceptsAllStores(parser); // either --store or
                                                       // --all-stores
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsDir(parser);
            AdminParserUtils.acceptsFormat(parser);
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
            stream.println("                    [-d <output-dir>] [--format json | hex]");
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
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_PARTITION,
                                           AdminParserUtils.OPT_ALL_PARTITIONS,
                                           AdminParserUtils.OPT_ORPHANED);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_STORE,
                                           AdminParserUtils.OPT_ALL_STORES);

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

            // execute command
            File directory = AdminToolUtils.createDir(dir);
            AdminClient adminClient = AdminToolUtils.getAdminClient(url);

            if(!orphaned && allParts) {
                partIds = AdminToolUtils.getAllPartitions(adminClient);
            }

            if(allStores) {
                storeNames = AdminToolUtils.getAllUserStoreNamesOnNode(adminClient, nodeId);
            } else {
                AdminToolUtils.validateUserStoreNamesOnNode(adminClient, nodeId, storeNames);
            }

            doStreamFetchKeys(adminClient, nodeId, storeNames, partIds, orphaned, directory, format);
        }

        /**
         * Fetches keys from a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeId Node id to fetch entries from
         * @param storeNames List of stores to fetch from
         * @param partIds List of partitions to fetch from
         * @param orphaned Tells if orphaned entries to be fetched
         * @param directory File object of directory to output to
         * @param format output format
         * @throws IOException
         */
        public static void doStreamFetchKeys(AdminClient adminClient,
                                             Integer nodeId,
                                             List<String> storeNames,
                                             List<Integer> partIds,
                                             Boolean orphaned,
                                             File directory,
                                             String format) throws IOException {
            HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
            storeDefinitionMap.putAll(AdminToolUtils.getUserStoreDefMapOnNode(adminClient, nodeId));
            storeDefinitionMap.putAll(AdminToolUtils.getSystemStoreDefMap());

            for(String store: storeNames) {

                StoreDefinition storeDefinition = storeDefinitionMap.get(store);

                if(null == storeDefinition) {
                    System.out.println("No store found under the name \'" + store + "\'");
                    continue;
                }

                Iterator<ByteArray> keyIteratorRef = null;

                if(orphaned) {
                    System.out.println("Fetching orphaned keys of " + store);
                    keyIteratorRef = adminClient.bulkFetchOps.fetchOrphanedKeys(nodeId, store);
                } else {
                    System.out.println("Fetching keys in partitions "
                                       + Joiner.on(", ").join(partIds) + " of " + store);
                    keyIteratorRef = adminClient.bulkFetchOps.fetchKeys(nodeId,
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

                if(format.equals(AdminParserUtils.ARG_FORMAT_JSON)) {
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

                                SubCommandStreamFetchEntries.writeObjectAsJson(out,
                                                                               keyObject,
                                                                               generator);
                                out.write('\n');
                            }
                        }
                    });
                } else if(format.equals(AdminParserUtils.ARG_FORMAT_HEX)) {
                    writeBinary(outFile, new Printable() {

                        @Override
                        public void printTo(DataOutputStream out) throws IOException {
                            while(keyIterator.hasNext()) {
                                byte[] keyBytes = keyIterator.next().get();
                                out.writeChars(ByteUtils.toHexString(keyBytes) + "\n");
                            }
                        }
                    });
                } else {
                    throw new VoldemortException("Invalid format \'" + format + "\'.");
                }

                if(outFile != null) {
                    System.out.println("Fetched keys from " + store + " to " + outFile);
                }
            }
        }
    }

    /**
     * stream mirror command
     */
    public static class SubCommandStreamMirror extends AbstractAdminCommand {

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
            // help options
            AdminParserUtils.acceptsHelp(parser);
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
            AdminParserUtils.acceptsStoreMultiple(parser); // either
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

            // declare parameters
            Integer srcNodeId = null, destNodeId = null;
            String srcUrl = null, destUrl = null;
            List<String> storeNames = null;
            Boolean allStores = false;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_FROM_NODE);
            AdminParserUtils.checkRequired(options, OPT_FROM_URL);
            AdminParserUtils.checkRequired(options, OPT_TO_NODE);
            AdminParserUtils.checkRequired(options, OPT_TO_URL);
            AdminParserUtils.checkRequired(options,
                                           AdminParserUtils.OPT_STORE,
                                           AdminParserUtils.OPT_ALL_STORES);

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

            // print summary
            System.out.println("Mirror data from one node to another");
            System.out.println("Store:");
            if(allStores) {
                System.out.println("  all stores");
            } else {
                System.out.println("  " + Joiner.on(", ").join(storeNames));
            }
            System.out.println("Location:");
            System.out.println("  source bootstrap url = " + srcUrl);
            System.out.println("  source node = " + srcNodeId);
            System.out.println("  destination bootstrap url = " + destUrl);
            System.out.println("  destination node = " + destNodeId);

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "mirror stores")) {
                return;
            }

            AdminClient srcAdminClient = AdminToolUtils.getAdminClient(srcUrl);
            AdminClient destAdminClient = AdminToolUtils.getAdminClient(destUrl);

            if(allStores) {
                storeNames = AdminToolUtils.getAllUserStoreNamesOnNode(srcAdminClient, srcNodeId);
            } else {
                AdminToolUtils.validateUserStoreNamesOnNode(srcAdminClient, srcNodeId, storeNames);
            }

            AdminToolUtils.assertServerNotInRebalancingState(srcAdminClient, srcNodeId);
            AdminToolUtils.assertServerNotInRebalancingState(destAdminClient, destNodeId);
            destAdminClient.restoreOps.mirrorData(destNodeId, srcNodeId, srcUrl, storeNames);
        }
    }

    /**
     * stream update-entries command
     */
    public static class SubCommandStreamUpdateEntries extends AbstractAdminCommand {

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
            AdminParserUtils.acceptsDir(parser);
            AdminParserUtils.acceptsNodeSingle(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsStoreMultiple(parser);
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

            // declare parameters
            String dir = null;
            Integer nodeId = null;
            String url = null;
            List<String> storeNames = null;
            Boolean confirm = false;

            // parse command-line input
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_DIR);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_NODE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

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

            // print summary
            System.out.println("Update entries from file");
            System.out.println("Input directory = \'" + dir + "\'");
            System.out.println("Store:");
            if(storeNames == null) {
                System.out.println("  all stores available from input files");
            } else {
                System.out.println("  " + Joiner.on(", ").join(storeNames));
            }
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = " + nodeId);

            // execute command
            if(!AdminToolUtils.askConfirm(confirm, "update entries")) {
                return;
            }

            AdminClient adminClient = AdminToolUtils.getAdminClient(url);
            File inDir = new File(dir);

            if(!inDir.exists()) {
                throw new FileNotFoundException("Input directory " + dir + " doesn't exist");
            }

            AdminToolUtils.assertServerInNormalState(adminClient, nodeId);

            doStreamUpdateEntries(adminClient, nodeId, storeNames, inDir);
        }

        /**
         * Updates entries on a given node.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodeId Node id to update entries to
         * @param storeNames Stores to update entries
         * @param inDir File object of directory to input entries from
         * @throws IOException
         * 
         */
        public static void doStreamUpdateEntries(AdminClient adminClient,
                                                 Integer nodeId,
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
                adminClient.streamingOps.updateEntries(nodeId, storeName, iterator, null);
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

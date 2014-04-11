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

import java.io.*;
import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

/**
 * Implements all functionality of admin stream operations.
 * 
 */
public class AdminCommandStream {

    /**
     * Main entry of group command 'stream', directs to sub-commands
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception
     * 
     */
    public static void execute(String[] args, Boolean printHelp) throws Exception {
    	String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if (subCmd.compareTo("fetch-entries") == 0) executeStreamFetchEntries(args, printHelp);
        else if (subCmd.compareTo("fetch-keys") == 0) executeStreamFetchKeys(args, printHelp);
        else if (subCmd.compareTo("mirror") == 0) executeStreamMirror(args, printHelp);
        else if (subCmd.compareTo("update-entries") == 0) executeStreamUpdateEntries(args, printHelp);
        else executeStreamHelp();
    }

    private abstract static class Printable {
        public abstract void printTo(DataOutputStream out) throws IOException;
    }
    private abstract static class Writable {
        public abstract void writeTo(BufferedWriter out) throws IOException;
    }
    private static Iterator<Pair<ByteArray, Versioned<byte[]>>> readEntriesBinary(File inputDir, String storeName)
        throws IOException {
        File inputFile = new File(inputDir, storeName + ".entries");
        if (!inputFile.exists()) {
            throw new FileNotFoundException("File " + inputFile.getAbsolutePath() + " does not exist!");
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
        if (outputFile == null) {
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
        if (outputFile == null) {
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
    private static void doStreamFetchEntries(AdminClient adminClient, Node node, List<String> storeNames,
                                      List<Integer> partIds, Boolean orphaned, File directory, String format)
                 throws IOException {
        HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        storeDefinitionMap.putAll(AdminUtils.getUserStoreDefs(adminClient, node));
        storeDefinitionMap.putAll(AdminUtils.getSystemStoreDefs());
        
        for(String store: storeNames) {

            StoreDefinition storeDefinition = storeDefinitionMap.get(store);

            if (null == storeDefinition) {
                System.out.println("No store found under the name \'" + store + "\'");
                continue;
            }

            Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIteratorRef = null;
            
            if (orphaned) {
                System.out.println("Fetching orphaned entries of " + store);
                entryIteratorRef = adminClient.bulkFetchOps.fetchOrphanedEntries(node.getId(), store);
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
            if (directory != null) {
                outFile = new File(directory, store + ".entries");
            }

            final Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = entryIteratorRef;
            
            if (format.compareTo(AdminOptionParser.ARG_FORMAT_JSON) == 0) {
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
                            if (keyObject instanceof GenericRecord) {
                                out.write(keyObject.toString());
                            } else {
                                generator.writeObject(keyObject);
                            }
                            out.write(' ' + version.toString() + ' ');
                            if (valueObject instanceof GenericRecord) {
                                out.write(valueObject.toString());
                            } else {
                                generator.writeObject(valueObject);
                            }
                            out.write('\n');
                        }
                    }
                });
            } else if (format.compareTo(AdminOptionParser.ARG_FORMAT_HEX) == 0) {
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
            } else if (format.compareTo(AdminOptionParser.ARG_FORMAT_BINARY) == 0) {
                // TODO
                throw new VoldemortException("--format binary   not implemented yet.");
            } else throw new VoldemortException("Specify valid format: json, binary, hex.");

            if (outFile != null) {
                System.out.println("Fetched entries from " + store + " to " + outFile);
            }
        }
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
    private static void doStreamFetchKeys(AdminClient adminClient, Node node, List<String> storeNames,
                                      List<Integer> partIds, Boolean orphaned, File directory, String format)
                 throws IOException {
        HashMap<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        storeDefinitionMap.putAll(AdminUtils.getUserStoreDefs(adminClient, node));
        storeDefinitionMap.putAll(AdminUtils.getSystemStoreDefs());
        
        for(String store: storeNames) {

            StoreDefinition storeDefinition = storeDefinitionMap.get(store);

            if (null == storeDefinition) {
                System.out.println("No store found under the name \'" + store + "\'");
                continue;
            }

            Iterator<ByteArray> keyIteratorRef = null;
            
            if (orphaned) {
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
            if (directory != null) {
                outFile = new File(directory, store + ".keys");
            }

            if (format.compareTo(AdminOptionParser.ARG_FORMAT_JSON) == 0) {
                final SerializerDefinition serializerDef = storeDefinition.getKeySerializer();
                final SerializerFactory serializerFactory = new DefaultSerializerFactory();
                @SuppressWarnings("unchecked")
                final Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(serializerDef);

                final CompressionStrategy keysCompressionStrategy;
                if (serializerDef != null && serializerDef.hasCompression()) {
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

                            if (keyObject instanceof GenericRecord) {
                                out.write(keyObject.toString());
                            } else {
                                generator.writeObject(keyObject);
                            }
                            out.write('\n');
                        }
                    }
                });
            } else if (format.compareTo(AdminOptionParser.ARG_FORMAT_HEX) == 0) {
                writeBinary(outFile, new Printable() {
                    @Override
                    public void printTo(DataOutputStream out) throws IOException {
                        while(keyIterator.hasNext()) {
                            byte[] keyBytes = keyIterator.next().get();
                            out.writeChars(ByteUtils.toHexString(keyBytes) + "\n");
                        }
                    }
                });
            } else if (format.compareTo(AdminOptionParser.ARG_FORMAT_BINARY) == 0) {
                // TODO
                throw new VoldemortException("--format binary   not implemented yet.");
            } else throw new VoldemortException("Specify valid format: json, binary, hex.");

            if (outFile != null) {
                System.out.println("Fetched keys from " + store + " to " + outFile);
            }
        }
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
    private static void doStreamUpdateEntries(AdminClient adminClient, Node node, List<String> storeNames, File inDir) throws IOException {
        if (storeNames == null) {
            storeNames = Lists.newArrayList();
            for (File storeFile: inDir.listFiles()) {
                String fileName = storeFile.getName();
                if (fileName.endsWith(".entries")) {
                    int extPosition = fileName.lastIndexOf(".entries");
                    storeNames.add(fileName.substring(0, extPosition));
                }
            }
        }
        for (String storeName: storeNames) {
            Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = readEntriesBinary(inDir, storeName);
            adminClient.streamingOps.updateEntries(node.getId(), storeName, iterator, null);
        }
    }
 
    /**
     * Prints command-line help menu.
     * 
     */
    private static void executeStreamHelp() {
        System.out.println();
        System.out.println("Voldemort Admin Tool Stream Commands");
        System.out.println("------------------------------------");
        System.out.println("fetch-entries    Fetch entries from a node.");
        System.out.println("fetch-keys       Fetch keys from a node.");
        System.out.println("mirror           Mirror data from a node to another.");
        System.out.println("update-entries   Update entries from file to a node.");
        System.out.println();
        System.out.println("To get more information on each command,");
        System.out.println("please try \'help stream <command-name>\'.");
        System.out.println();
    }
    
    /**
     * Parses command-line and fetches entries from a given node.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStreamFetchEntries(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> partOptList = Lists.newArrayList();
        Integer nodeId = null;
        List<Integer> partIds = null;
        Boolean allParts = false;
        Boolean orphaned = false;
        List<String> storeNames = null;
        Boolean allStores = false;
        String url = null;
        String dir = null;
        String format = null;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        partOptList.add(AdminOptionParser.OPT_PARTITION);
        partOptList.add(AdminOptionParser.OPT_ALL_PARTITIONS);
        partOptList.add(AdminOptionParser.OPT_ORPHANED);
        parser.addRequired(partOptList);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE, AdminOptionParser.OPT_ALL_STORES);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_DIR);
        parser.addOptional(AdminOptionParser.OPT_FORMAT);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  stream fetch-entries - Fetch entries from a node");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  stream fetch-entries -n <node-id>");
            System.out.println("                       (-p <partition-id-list> | --all-partitions | --orphaned)");
            System.out.println("                       (-s <store-name-list> | --all-stores) -u <url>");
            System.out.println("                       [-d <output-dir>] [--format json | binary | hex]");
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
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        if (parser.hasOption(AdminOptionParser.OPT_PARTITION)) {
            partIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_PARTITION);
            allParts = false;
            orphaned = false;
        } else if (parser.hasOption(AdminOptionParser.OPT_ALL_PARTITIONS)) {
            allParts = true;
            orphaned = false;
        } else if (parser.hasOption(AdminOptionParser.OPT_ORPHANED)) {
            allParts = false;
            orphaned = true;
        }
        if (parser.hasOption(AdminOptionParser.OPT_STORE)) {
            storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
            allStores = false;
        } else {
            allStores = true;
        }
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_DIR)) {
            dir = (String) parser.getValue(AdminOptionParser.OPT_DIR);
        }
        if (parser.hasOption(AdminOptionParser.OPT_FORMAT)) {
            format = (String) parser.getValue(AdminOptionParser.OPT_FORMAT);
        } else {
            format = AdminOptionParser.ARG_FORMAT_JSON;
        }

        File directory = AdminUtils.createDir(dir);
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
        if (!orphaned) partIds = AdminUtils.getPartitions(adminClient, partIds, allParts);
        storeNames = AdminUtils.getUserStoresOnNode(adminClient, node, storeNames, allStores);
        
        doStreamFetchEntries(adminClient, node, storeNames, partIds, orphaned, directory, format);
    }

    /**
     * Parses command-line and fetches keys from a given node.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStreamFetchKeys(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        List<String> partOptList = Lists.newArrayList();
        Integer nodeId = null;
        List<Integer> partIds = null;
        Boolean allParts = false;
        Boolean orphaned = false;
        List<String> storeNames = null;
        Boolean allStores = false;
        String url = null;
        String dir = null;
        String format = null;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        partOptList.add(AdminOptionParser.OPT_PARTITION);
        partOptList.add(AdminOptionParser.OPT_ALL_PARTITIONS);
        partOptList.add(AdminOptionParser.OPT_ORPHANED);
        parser.addRequired(partOptList);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE, AdminOptionParser.OPT_ALL_STORES);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_DIR);
        parser.addOptional(AdminOptionParser.OPT_FORMAT);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  stream fetch-keys - Fetch keys from a node");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  stream fetch-keys -n <node-id>");
            System.out.println("                    (-p <partition-id-list> | --all-partitions | --orphaned)");
            System.out.println("                    (-s <store-name-list> | --all-stores) -u <url>");
            System.out.println("                    [-d <output-dir>] [--format json | binary | hex]");
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
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        if (parser.hasOption(AdminOptionParser.OPT_PARTITION)) {
            partIds = (List<Integer>) parser.getValueList(AdminOptionParser.OPT_PARTITION);
            allParts = false;
            orphaned = false;
        } else if (parser.hasOption(AdminOptionParser.OPT_ALL_PARTITIONS)) {
            allParts = true;
            orphaned = false;
        } else if (parser.hasOption(AdminOptionParser.OPT_ORPHANED)) {
            allParts = false;
            orphaned = true;
        }
        if (parser.hasOption(AdminOptionParser.OPT_STORE)) {
            storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
            allStores = false;
        } else {
            allStores = true;
        }
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        if (parser.hasOption(AdminOptionParser.OPT_DIR)) {
            dir = (String) parser.getValue(AdminOptionParser.OPT_DIR);
        }
        if (parser.hasOption(AdminOptionParser.OPT_FORMAT)) {
            format = (String) parser.getValue(AdminOptionParser.OPT_FORMAT);
        } else {
            format = AdminOptionParser.ARG_FORMAT_JSON;
        }

        File directory = AdminUtils.createDir(dir);
        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
        if (!orphaned) partIds = AdminUtils.getPartitions(adminClient, partIds, allParts);
        storeNames = AdminUtils.getUserStoresOnNode(adminClient, node, storeNames, allStores);
        
        doStreamFetchKeys(adminClient, node, storeNames, partIds, orphaned, directory, format);
    }

    /**
     * Parses command-line and mirrors data from a node to another.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws IOException 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStreamMirror(String[] args, Boolean printHelp) throws IOException {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        Integer srcNodeId = null, destNodeId = null;
        String srcUrl = null, destUrl = null;
        List<String> storeNames = null;
        Boolean allStores = false;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_FROM_URL);
        parser.addRequired(AdminOptionParser.OPT_FROM_NODE);
        parser.addRequired(AdminOptionParser.OPT_TO_URL);
        parser.addRequired(AdminOptionParser.OPT_TO_NODE);
        parser.addRequired(AdminOptionParser.OPT_STORE_MULTIPLE, AdminOptionParser.OPT_ALL_STORES);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  stream mirror - Mirror data from a node to another");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  stream mirror --from-url <src-url> --from-node <src-node-id>");
            System.out.println("                --to-url <dest-url> --to-node <dest-node-id>");
            System.out.println("                (-s <store-name-list> | --all-stores) [--confirm]");
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
        srcUrl = (String) parser.getValue(AdminOptionParser.OPT_FROM_URL);
        destUrl = (String) parser.getValue(AdminOptionParser.OPT_TO_URL);
        srcNodeId = (Integer) parser.getValue(AdminOptionParser.OPT_FROM_NODE);
        destNodeId = (Integer) parser.getValue(AdminOptionParser.OPT_TO_NODE);
        if (parser.hasOption(AdminOptionParser.OPT_STORE)) {
            storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
            allStores = false;
        } else {
            allStores = true;
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;
        
        if (!AdminUtils.askConfirm(confirm, "mirror stores from " + srcUrl + ":" + srcNodeId
                                   + " to " + destUrl + ":" + destNodeId)) {
            return;
        }

        AdminClient srcAdminClient = AdminUtils.getAdminClient(srcUrl);
        AdminClient destAdminClient = AdminUtils.getAdminClient(destUrl);
        Node srcNode = srcAdminClient.getAdminClientCluster().getNodeById(srcNodeId);
        //Node destNode = destAdminClient.getAdminClientCluster().getNodeById(srcNodeId);
        storeNames = AdminUtils.getUserStoresOnNode(srcAdminClient, srcNode, storeNames, allStores);
        
        destAdminClient.restoreOps.mirrorData(destNodeId, srcNodeId, srcUrl, storeNames);
    }

    /**
     * Parses command-line and updates entries on a given node.
     * 
     * @param args Command-line input
     * @param printHelp Tells whether to print help only or execute command actually
     * @throws Exception 
     * 
     */
    @SuppressWarnings("unchecked")
    private static void executeStreamUpdateEntries(String[] args, Boolean printHelp) throws Exception {

        AdminOptionParser parser = new AdminOptionParser();
        
        // declare parameters
        String dir = null;
        Integer nodeId = null;
        String url = null;
        List<String> storeNames = null;
        Boolean confirm = false;
        
        // add parameters to parser
        parser.addRequired(AdminOptionParser.OPT_DIR);
        parser.addRequired(AdminOptionParser.OPT_NODE_SINGLE);
        parser.addRequired(AdminOptionParser.OPT_URL);
        parser.addOptional(AdminOptionParser.OPT_STORE_MULTIPLE);
        parser.addOptional(AdminOptionParser.OPT_CONFIRM);
        
        // print help menu if help command executed
        if (printHelp) {
            System.out.println();
            System.out.println("NAME");
            System.out.println("  stream update-entries - Update entries from file to a node");
            System.out.println();
            System.out.println("SYNOPSIS");
            System.out.println("  stream update-entries -d <input-dir> -n <node-id> -u <url>");
            System.out.println("                        [-s <store-name-list>] [--confirm]");
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
        dir = (String) parser.getValue(AdminOptionParser.OPT_DIR);
        nodeId = (Integer) parser.getValue(AdminOptionParser.OPT_NODE);
        url = (String) parser.getValue(AdminOptionParser.OPT_URL);
        
        if (parser.hasOption(AdminOptionParser.OPT_STORE)) {
            storeNames = (List<String>) parser.getValueList(AdminOptionParser.OPT_STORE);
        }
        if (parser.hasOption(AdminOptionParser.OPT_CONFIRM)) confirm = true;

        if (!AdminUtils.askConfirm(confirm, "update entries")) return;

        AdminClient adminClient = AdminUtils.getAdminClient(url);
        Node node = adminClient.getAdminClientCluster().getNodeById(nodeId);
        File inDir = new File(dir);
        if (!inDir.exists()) {
            throw new FileNotFoundException("Input directory " + dir + " doesn't exist");
        }
        
        doStreamUpdateEntries(adminClient, node, storeNames, inDir);
    }
}

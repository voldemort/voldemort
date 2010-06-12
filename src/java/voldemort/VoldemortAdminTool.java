/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import voldemort.annotations.Experimental;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.*;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

/**
 * Provides a command line interface to the {@link voldemort.client.protocol.admin.AdminClient}
 */
public class VoldemortAdminTool {
    public static void main (String [] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("node", "node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("delete-partitions", "Delete partitions")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("restore", "Restore from replication");
        parser.accepts("ascii", "Fetch keys as ASCII");
        parser.accepts("parallelism", "Parallelism")
              .withRequiredArg()
              .describedAs("parallelism")
              .ofType(Integer.class);
        parser.accepts("fetch-keys", "Fetch keys")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("fetch-entries", "[EXPERIMENTAL] Fetch full entries")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("outdir", "Output directory")
              .withRequiredArg()
              .describedAs("output-directory")
              .ofType(String.class);
        parser.accepts("stores", "Store names")
              .withRequiredArg()
              .describedAs("store-names")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        parser.accepts("add-stores", "Add stores in this stores.xml")
              .withRequiredArg()
              .describedAs("stores.xml")
              .ofType(String.class);
        parser.accepts("update-entries", "[EXPERIMENTAL] Insert or update entries")
              .withRequiredArg()
              .describedAs("input-directory")
              .ofType(String.class);
        
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "url",
                                               "node");
        if (missing.size() > 0) {
            // Not the most elegant way to do this
            if (!(missing.equals(ImmutableSet.of("node")) && options.has("add-stores"))) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }

        String url = (String) options.valueOf("url");
        Integer nodeId = (Integer) options.valueOf("node");
        Integer parallelism = CmdUtils.valueOf(options, "parallelism", 5);

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig());

        String ops = "";
        if (options.has("delete-partitions")) {
            ops += "d";
        }
        if (options.has("fetch-keys")) {
            ops += "k";
        }
        if (options.has("fetch-entries")) {
            ops += "v";
        }
        if (options.has("restore")) {
            ops += "r";
        }
        if (options.has("add-stores")) {
            ops += "a";
        }
        if (options.has("update-entries")) {
            ops += "u";
        }
        if (ops.length() < 1) {
            Utils.croak("At least one of (delete-partitions, restore, add-node, fetch-entries, fetch-keys) must be specified");
        }

        List<String> storeNames = null;

        if (options.has("stores")) {
            // For some reason one can't just do @SuppressWarnings without identifier following it
            @SuppressWarnings("unchecked")
            List<String> temp = (List<String>) options.valuesOf("stores");
            storeNames = temp;
        }

        try {
            if (ops.contains("d")) {
                System.out.println("Starting delete-partitions");
                @SuppressWarnings("unchecked")
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("delete-partitions");
                executeDeletePartitions(nodeId,
                                        adminClient,
                                        partitionIdList,
                                        storeNames);
                System.out.println("Finished delete-partitions");
            }
            if (ops.contains("r")) {
                System.out.println("Starting restore");
                adminClient.restoreDataFromReplications(nodeId, parallelism);
                System.err.println("Finished restore");
            }
            if (ops.contains("k")) {
                if (!options.has("outdir")) {
                    Utils.croak("Directory name (outdir) must be specified for fetch-keys");
                }
                String outputDir = (String) options.valueOf("outdir");
                boolean useAscii = options.has("ascii");
                System.out.println("Starting fetch keys");
                @SuppressWarnings("unchecked")
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("fetch-keys");
                executeFetchKeys(nodeId,
                                 adminClient,
                                 partitionIdList,
                                 outputDir,
                                 storeNames,
                                 useAscii);
            }
            if (ops.contains("v")) {
                if (!options.has("outdir")) {
                    Utils.croak("Directory name (outdir) must be specified for fetch-values");
                }
                String outputDir = (String) options.valueOf("outdir");
                boolean useAscii = options.has("ascii");
                @SuppressWarnings("unchecked")
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("fetch-values");
                executeFetchEntries(nodeId,
                                   adminClient,
                                   partitionIdList,
                                   outputDir,
                                   storeNames,
                                   useAscii);
            }
            if (ops.contains("a")) {
                String storesXml = (String) options.valueOf("add-stores");
                executeAddStores(adminClient,
                                 storesXml,
                                 storeNames);
            }
            if (ops.contains("u")) {
                String inputDir = (String) options.valueOf("update-entries");
                boolean useAscii = options.has("ascii");
                executeUpdateEntries(nodeId,
                                     adminClient,
                                     storeNames,
                                     inputDir,
                                     useAscii);
                
            }
        } catch (Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }

    public static void executeAddStores(AdminClient adminClient,
                                        String storesXml,
                                        List<String> storeNames) throws IOException {
        List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(new File(storesXml));
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for (StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }
        List<String> stores = storeNames;
        if (stores == null) {
            stores = Lists.newArrayList();
            stores.addAll(storeDefinitionMap.keySet());
        }
        for (String store: stores) {
            System.out.println("Adding " + store);
            adminClient.addStore(storeDefinitionMap.get(store));
        }
    }

    @Experimental
    public static void executeFetchEntries(Integer nodeId,
                                           AdminClient adminClient,
                                           List<Integer> partitionIdList,
                                           String outputDir,
                                           List<String> storeNames,
                                           boolean useAscii) throws IOException {
        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for (StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File directory = new File(outputDir);
        if (directory.exists() || directory.mkdir()) {
            List<String> stores = storeNames;
            if (stores == null) {
                stores = Lists.newArrayList();
                stores.addAll(storeDefinitionMap.keySet());
            }
            for (String store: stores) {
                System.out.println("Fetching entries in partitions " + Joiner.on(", ").join(partitionIdList) + " of " + store);
                Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                           store,
                                                                                                           partitionIdList,
                                                                                                           null,
												           false);
                File outputFile = new File(directory, store + ".entries");
                if (useAscii) {
                    StoreDefinition storeDefinition = storeDefinitionMap.get(store);
                    writeEntriesAscii(entriesIterator, outputFile, storeDefinition);
                } else {
                    writeEntriesBinary(entriesIterator, outputFile);
                }

            }
        }
    }

    @Experimental
    private static void executeUpdateEntries(Integer nodeId,
                                             AdminClient adminClient,
                                             List<String> storeNames,
                                             String inputDirPath,
                                             boolean useAscii) throws IOException {
        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for (StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File inputDir = new File(inputDirPath);
        if (!inputDir.exists()) {
            throw new FileNotFoundException("input directory " + inputDirPath + " doesn't exist");
        }

        if (storeNames == null) {
            storeNames = Lists.newArrayList();
            for (File storeFile: inputDir.listFiles()) {
                String fileName = storeFile.getName();
                if (fileName.endsWith(".entries")) {
                    int extPosition = fileName.lastIndexOf(".entries");
                    storeNames.add(fileName.substring(0, extPosition));
                }
            }
        }

        for (String storeName: storeNames) {
            Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator;
            if (useAscii) {
                StoreDefinition storeDefinition = storeDefinitionMap.get(storeName);
                if (storeDefinition == null) {
                    throw new IllegalArgumentException("No definition found for " + storeName);
                }
                iterator = readEntriesAscii(inputDir,storeName,storeDefinition);
            } else {
                iterator = readEntriesBinary(inputDir,storeName);

            }
            adminClient.updateEntries(nodeId, storeName, iterator, null);
        }

    }

    // TODO: implement this
    private static Iterator<Pair<ByteArray,Versioned<byte[]>>> readEntriesAscii(File inputDir,
                                                                                String storeName,
                                                                                StoreDefinition storeDefinition)
                   throws IOException {
        File inputFile = new File(inputDir, storeName + ".entries");
        if (!inputFile.exists()) {
            throw new FileNotFoundException("File " + inputFile.getAbsolutePath() + " does not exist!");
        }

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>> () {
            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                System.err.println("Updating stores from ASCII/JSON data is not yet supported!");
                return endOfData();
            }
        };
    }

    private static Iterator<Pair<ByteArray,Versioned<byte[]>>> readEntriesBinary(File inputDir, String storeName)
                   throws IOException {
        File inputFile = new File(inputDir, storeName + ".entries");
        if (!inputFile.exists()) {
            throw new FileNotFoundException("File " + inputFile.getAbsolutePath() + " does not exist!");
        }
        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(inputFile)));

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>> () {
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
                } catch (EOFException e) {
                    try {
                        dis.close();
                    } catch (IOException ie) {
                        ie.printStackTrace();
                    }
                    return endOfData();
                } catch (IOException e) {
                    try {
                        dis.close();
                    } catch (IOException ie) {
                        ie.printStackTrace();
                    }
                    throw new VoldemortException("Error reading from input file ", e);
                }
            }
        };
    }

    private static void writeEntriesAscii(Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator,
                                          File outputFile,
                                          StoreDefinition storeDefinition) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        SerializerFactory serializerFactory = new DefaultSerializerFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        @SuppressWarnings("unchecked")
        Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getKeySerializer());
        @SuppressWarnings("unchecked")
        Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getValueSerializer());

        try {
            while (iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> kvPair = iterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                VectorClock version = (VectorClock) kvPair.getSecond().getVersion();
                byte[] valueBytes = kvPair.getSecond().getValue();

                Object keyObject = keySerializer.toObject(keyBytes);
                Object valueObject = valueSerializer.toObject(valueBytes);

                generator.writeObject(keyObject);
                stringWriter.write(' ');
                stringWriter.write(version.toString());
                generator.writeObject(valueObject);

                StringBuffer buf = stringWriter.getBuffer();
                if (buf.charAt(0) == ' ') {
                    buf.setCharAt(0, '\n');
                }
                writer.write(buf.toString());
                buf.setLength(0);
            }
            writer.write('\n');
        } finally {
            writer.close();
        }
    }

    private static void writeEntriesBinary(Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator,
                                           File outputFile) throws IOException {
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
        try {
            while (iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> kvPair = iterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                byte[] versionBytes = ((VectorClock) kvPair.getSecond().getVersion()).toBytes();
                byte[] valueBytes = kvPair.getSecond().getValue();
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeInt(versionBytes.length);
                dos.write(versionBytes);
                dos.write(valueBytes.length);
                dos.write(valueBytes);
            }
        } finally {
            dos.close();
        }
    }
                                         
    public static void executeFetchKeys(Integer nodeId,
                                        AdminClient adminClient,
                                        List<Integer> partitionIdList,
                                        String outputDir,
                                        List<String> storeNames,
                                        boolean useAscii) throws IOException {
        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for (StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        File directory = new File(outputDir);
        if (directory.exists() || directory.mkdir()) {
            List<String> stores = storeNames;
            if (stores == null) {
                stores = Lists.newArrayList();
                stores.addAll(storeDefinitionMap.keySet());
            }
            for (String store: stores) {
                System.out.println("Fetching keys in partitions " + Joiner.on(", ").join(partitionIdList) + " of " + store);
                Iterator<ByteArray> keyIterator = adminClient.fetchKeys(nodeId, store, partitionIdList, null, false);
                File outputFile = new File(directory, store + ".keys");
                if (useAscii) {
                    StoreDefinition storeDefinition = storeDefinitionMap.get(store);
                    writeKeysAscii(keyIterator, outputFile, storeDefinition);
                } else {
                    writeKeysBinary(keyIterator, outputFile);
                }

                System.out.println("Fetched keys from " + store + " to " + outputFile);
            }
        } else {
            Utils.croak("Can't find or create directory " + outputDir);
        }
    }

    private static void writeKeysAscii(Iterator<ByteArray> keyIterator,
                                       File outputFile,
                                       StoreDefinition storeDefinition) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        SerializerFactory serializerFactory = new DefaultSerializerFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getKeySerializer());
        try {
            while (keyIterator.hasNext()) {
                // Ugly hack to be able to separate text by newlines vs. spaces
                byte[] keyBytes = keyIterator.next().get();
                Object keyObject = serializer.toObject(keyBytes);
                generator.writeObject(keyObject);
                StringBuffer buf = stringWriter.getBuffer();
                if (buf.charAt(0) == ' ') {
                    buf.setCharAt(0, '\n');
                }
                writer.write(buf.toString());
                buf.setLength(0);
            }
            writer.write('\n');
        } finally {
            writer.close();
        }
    }
    
    private static void writeKeysBinary(Iterator<ByteArray> keyIterator, File outputFile) throws IOException {
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));

        try {
            while (keyIterator.hasNext()) {
                byte[] keyBytes = keyIterator.next().get();
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
            }
        } finally {
            dos.close();
        }
    }

    public static void executeDeletePartitions(Integer nodeId,
                                               AdminClient adminClient,
                                               List<Integer> partitionIdList,
                                               List<String> storeNames) {
        List<String> stores = storeNames;
        if (stores == null) {
            stores = Lists.newArrayList();
            List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
            for (StoreDefinition storeDefinition: storeDefinitionList) {
                stores.add(storeDefinition.getName());
            }
        }
        
        for (String store: stores) {
            System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList) + " of " + store);
            adminClient.deletePartitions(nodeId, store, partitionIdList, null);
        }
    }
}

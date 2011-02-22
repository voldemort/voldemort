/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.EndOfFileException;
import voldemort.serialization.json.JsonReader;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteUtils;
import voldemort.utils.CmdUtils;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Build a read-only store from given input.
 * 
 * 
 */
public class JsonStoreBuilder {

    private static final Logger logger = Logger.getLogger(JsonStoreBuilder.class);

    private final JsonReader reader;
    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final RoutingStrategy routingStrategy;
    private final File outputDir;
    private final File tempDir;
    private final int internalSortSize;
    private final int numThreads;
    private final int numChunks;
    private final int ioBufferSize;
    private final boolean gzipIntermediate;

    public JsonStoreBuilder(JsonReader reader,
                            Cluster cluster,
                            StoreDefinition storeDefinition,
                            RoutingStrategy routingStrategy,
                            File outputDir,
                            File tempDir,
                            int internalSortSize,
                            int numThreads,
                            int numChunks,
                            int ioBufferSize,
                            boolean gzipIntermediate) {
        if(cluster.getNumberOfNodes() < storeDefinition.getReplicationFactor())
            throw new IllegalStateException("Number of nodes is " + cluster.getNumberOfNodes()
                                            + " but the replication factor is "
                                            + storeDefinition.getReplicationFactor() + ".");
        this.reader = reader;
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        if(tempDir == null)
            this.tempDir = new File(Utils.notNull(System.getProperty("java.io.tmpdir")));
        else
            this.tempDir = tempDir;
        this.outputDir = outputDir;
        this.routingStrategy = routingStrategy;
        this.internalSortSize = internalSortSize;
        this.numThreads = numThreads;
        this.numChunks = numChunks;
        this.ioBufferSize = ioBufferSize;
        this.gzipIntermediate = gzipIntermediate;
    }

    /**
     * Main method to run on a input text file
     * 
     * @param args see USAGE for details
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("cluster", "[REQUIRED] path to cluster xml config file")
              .withRequiredArg()
              .describedAs("cluster.xml");
        parser.accepts("stores", "[REQUIRED] path to stores xml config file")
              .withRequiredArg()
              .describedAs("stores.xml");
        parser.accepts("name", "[REQUIRED] store name").withRequiredArg().describedAs("store name");
        parser.accepts("buffer", "[REQUIRED] number of key/value pairs to buffer in memory")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("input", "[REQUIRED] input file to read from")
              .withRequiredArg()
              .describedAs("input-file");
        parser.accepts("output", "[REQUIRED] directory to output stores to")
              .withRequiredArg()
              .describedAs("output directory");
        parser.accepts("threads", "number of threads").withRequiredArg().ofType(Integer.class);
        parser.accepts("chunks", "number of chunks [per node, per partition]")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("io-buffer-size", "size of i/o buffers in bytes")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("temp-dir", "temporary directory for sorted file pieces")
              .withRequiredArg()
              .describedAs("temp dir");
        parser.accepts("gzip", "compress intermediate chunk files");
        parser.accepts("format",
                       "read-only store format [" + ReadOnlyStorageFormat.READONLY_V0.getCode()
                               + "," + ReadOnlyStorageFormat.READONLY_V1.getCode() + ","
                               + ReadOnlyStorageFormat.READONLY_V2.getCode() + "]")
              .withRequiredArg()
              .ofType(String.class);
        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "cluster",
                                               "stores",
                                               "name",
                                               "buffer",
                                               "input",
                                               "output");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String clusterFile = (String) options.valueOf("cluster");
        String storeDefFile = (String) options.valueOf("stores");
        String storeName = (String) options.valueOf("name");
        int sortBufferSize = (Integer) options.valueOf("buffer");
        String inputFile = (String) options.valueOf("input");
        File outputDir = new File((String) options.valueOf("output"));
        int numThreads = CmdUtils.valueOf(options, "threads", 2);
        int chunks = CmdUtils.valueOf(options, "chunks", 2);
        int ioBufferSize = CmdUtils.valueOf(options, "io-buffer-size", 1000000);
        ReadOnlyStorageFormat storageFormat = ReadOnlyStorageFormat.fromCode(CmdUtils.valueOf(options,
                                                                                              "format",
                                                                                              ReadOnlyStorageFormat.READONLY_V2.getCode()));
        boolean gzipIntermediate = options.has("gzip");
        File tempDir = new File(CmdUtils.valueOf(options,
                                                 "temp-dir",
                                                 System.getProperty("java.io.tmpdir")));

        try {
            JsonReader reader = new JsonReader(new BufferedReader(new FileReader(inputFile),
                                                                  ioBufferSize));
            Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));
            StoreDefinition storeDef = null;
            List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeDefFile)));
            for(StoreDefinition def: stores) {
                if(def.getName().equals(storeName))
                    storeDef = def;
            }

            if(storeDef == null)
                Utils.croak("No store found with name \"" + storeName + "\"");

            if(!outputDir.exists())
                Utils.croak("Directory \"" + outputDir.getAbsolutePath() + "\" does not exist.");

            RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster);

            new JsonStoreBuilder(reader,
                                 cluster,
                                 storeDef,
                                 routingStrategy,
                                 outputDir,
                                 tempDir,
                                 sortBufferSize,
                                 numThreads,
                                 chunks,
                                 ioBufferSize,
                                 gzipIntermediate).build(storageFormat);
        } catch(FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
    }

    public void build(ReadOnlyStorageFormat type) throws IOException {
        switch(type) {
            case READONLY_V0:
                buildVersion0();
                break;

            case READONLY_V1:
                buildVersion1();
                break;

            case READONLY_V2:
                buildVersion2();
                break;

            default:
                throw new VoldemortException("Invalid storage format " + type);
        }
    }

    public void buildVersion0() throws IOException {
        logger.info("Building store " + storeDefinition.getName() + " for "
                    + cluster.getNumberOfNodes() + " with " + numChunks
                    + " chunks per node and type " + ReadOnlyStorageFormat.READONLY_V0);

        // initialize nodes
        int numNodes = cluster.getNumberOfNodes();
        DataOutputStream[][] indexes = new DataOutputStream[numNodes][numChunks];
        DataOutputStream[][] datas = new DataOutputStream[numNodes][numChunks];
        int[][] positions = new int[numNodes][numChunks];
        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            File nodeDir = new File(outputDir, "node-" + Integer.toString(nodeId));
            nodeDir.mkdirs();

            // Create metadata file
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(nodeDir, ".metadata")));
            ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
            metadata.add(ReadOnlyStorageMetadata.FORMAT,
                         ReadOnlyStorageFormat.READONLY_V0.getCode());
            writer.write(metadata.toJsonString());
            writer.close();

            for(int chunk = 0; chunk < numChunks; chunk++) {
                File indexFile = new File(nodeDir, chunk + ".index");
                File dataFile = new File(nodeDir, chunk + ".data");
                positions[nodeId][chunk] = 0;
                indexes[nodeId][chunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile),
                                                                                       ioBufferSize));
                datas[nodeId][chunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile),
                                                                                     ioBufferSize));
            }
        }

        logger.info("Reading items...");
        int count = 0;
        ExternalSorter<KeyValuePair> sorter = new ExternalSorter<KeyValuePair>(new KeyValuePairSerializer(),
                                                                               new KeyMd5Comparator(),
                                                                               internalSortSize,
                                                                               tempDir.getAbsolutePath(),
                                                                               ioBufferSize,
                                                                               numThreads,
                                                                               gzipIntermediate);
        JsonObjectIterator iter = new JsonObjectIterator(reader, storeDefinition);
        for(KeyValuePair pair: sorter.sorted(iter)) {
            List<Node> nodes = this.routingStrategy.routeRequest(pair.getKey());
            byte[] keyMd5 = pair.getKeyMd5();
            for(int i = 0; i < this.storeDefinition.getReplicationFactor(); i++) {
                int nodeId = nodes.get(i).getId();
                int chunk = ReadOnlyUtils.chunk(keyMd5, numChunks);
                int numBytes = pair.getValue().length;
                datas[nodeId][chunk].writeInt(numBytes);
                datas[nodeId][chunk].write(pair.getValue());
                indexes[nodeId][chunk].write(keyMd5);
                indexes[nodeId][chunk].writeInt(positions[nodeId][chunk]);
                positions[nodeId][chunk] += numBytes + 4;
                checkOverFlow(chunk, positions[nodeId][chunk]);
            }
            count++;
        }

        logger.info(count + " items read.");

        // sort and write out
        logger.info("Closing all store files.");
        for(int node = 0; node < numNodes; node++) {
            for(int chunk = 0; chunk < numChunks; chunk++) {
                indexes[node][chunk].close();
                datas[node][chunk].close();
            }
        }
    }

    public void buildVersion1() throws IOException {
        logger.info("Building store " + storeDefinition.getName() + " for "
                    + cluster.getNumberOfPartitions() + " partitions with " + numChunks
                    + " chunks per partitions and type " + ReadOnlyStorageFormat.READONLY_V1);

        // initialize nodes
        int numNodes = cluster.getNumberOfNodes();
        DataOutputStream[][] indexes = new DataOutputStream[numNodes][];
        DataOutputStream[][] datas = new DataOutputStream[numNodes][];
        int[][] positions = new int[numNodes][];

        int[] partitionIdToChunkOffset = new int[cluster.getNumberOfPartitions()];
        int[] partitionIdToNodeId = new int[cluster.getNumberOfPartitions()];

        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            indexes[nodeId] = new DataOutputStream[node.getNumberOfPartitions() * numChunks];
            datas[nodeId] = new DataOutputStream[node.getNumberOfPartitions() * numChunks];
            positions[nodeId] = new int[node.getNumberOfPartitions() * numChunks];

            File nodeDir = new File(outputDir, "node-" + Integer.toString(nodeId));
            nodeDir.mkdirs();

            // Create metadata file
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(nodeDir, ".metadata")));
            ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
            metadata.add(ReadOnlyStorageMetadata.FORMAT,
                         ReadOnlyStorageFormat.READONLY_V1.getCode());
            writer.write(metadata.toJsonString());
            writer.close();

            int globalChunk = 0;
            for(Integer partition: node.getPartitionIds()) {
                partitionIdToChunkOffset[partition] = globalChunk;
                partitionIdToNodeId[partition] = node.getId();
                for(int chunk = 0; chunk < numChunks; chunk++) {
                    File indexFile = new File(nodeDir, Integer.toString(partition) + "_"
                                                       + Integer.toString(chunk) + ".index");
                    File dataFile = new File(nodeDir, Integer.toString(partition) + "_"
                                                      + Integer.toString(chunk) + ".data");
                    positions[nodeId][globalChunk] = 0;
                    indexes[nodeId][globalChunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile),
                                                                                                 ioBufferSize));
                    datas[nodeId][globalChunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile),
                                                                                               ioBufferSize));
                    globalChunk++;
                }

            }
        }

        logger.info("Reading items...");
        int count = 0;
        ExternalSorter<KeyValuePair> sorter = new ExternalSorter<KeyValuePair>(new KeyValuePairSerializer(),
                                                                               new KeyMd5Comparator(),
                                                                               internalSortSize,
                                                                               tempDir.getAbsolutePath(),
                                                                               ioBufferSize,
                                                                               numThreads,
                                                                               gzipIntermediate);
        JsonObjectIterator iter = new JsonObjectIterator(reader, storeDefinition);
        for(KeyValuePair pair: sorter.sorted(iter)) {
            byte[] keyMd5 = pair.getKeyMd5();
            List<Integer> partitionIds = this.routingStrategy.getPartitionList(pair.getKey());
            for(Integer partitionId: partitionIds) {
                int localChunkId = ReadOnlyUtils.chunk(keyMd5, numChunks);
                int chunk = localChunkId + partitionIdToChunkOffset[partitionId];
                int nodeId = partitionIdToNodeId[partitionId];
                datas[nodeId][chunk].writeInt(pair.getValue().length);
                datas[nodeId][chunk].write(pair.getValue());
                indexes[nodeId][chunk].write(keyMd5);
                indexes[nodeId][chunk].writeInt(positions[nodeId][chunk]);
                positions[nodeId][chunk] += pair.getValue().length + 4;
                checkOverFlow(chunk, positions[nodeId][chunk]);
            }
            count++;
        }

        logger.info(count + " items read.");

        // sort and write out
        logger.info("Closing all store files.");
        for(Node node: cluster.getNodes()) {
            for(int chunk = 0; chunk < numChunks * node.getNumberOfPartitions(); chunk++) {
                indexes[node.getId()][chunk].close();
                datas[node.getId()][chunk].close();
            }
        }
    }

    public void buildVersion2() throws IOException {
        logger.info("Building store " + storeDefinition.getName() + " for "
                    + cluster.getNumberOfPartitions() + " partitions with " + numChunks
                    + " chunks per partitions and type " + ReadOnlyStorageFormat.READONLY_V2);

        // initialize nodes
        int numNodes = cluster.getNumberOfNodes();
        DataOutputStream[][] indexes = new DataOutputStream[numNodes][];
        DataOutputStream[][] datas = new DataOutputStream[numNodes][];
        int[][] positions = new int[numNodes][];

        int[] partitionIdToChunkOffset = new int[cluster.getNumberOfPartitions()];
        int[] partitionIdToNodeId = new int[cluster.getNumberOfPartitions()];

        for(Node node: cluster.getNodes()) {
            int nodeId = node.getId();
            indexes[nodeId] = new DataOutputStream[node.getNumberOfPartitions() * numChunks];
            datas[nodeId] = new DataOutputStream[node.getNumberOfPartitions() * numChunks];
            positions[nodeId] = new int[node.getNumberOfPartitions() * numChunks];

            File nodeDir = new File(outputDir, "node-" + Integer.toString(nodeId));
            nodeDir.mkdirs();

            // Create metadata file
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(nodeDir, ".metadata")));
            ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
            metadata.add(ReadOnlyStorageMetadata.FORMAT,
                         ReadOnlyStorageFormat.READONLY_V2.getCode());
            writer.write(metadata.toJsonString());
            writer.close();

            int globalChunk = 0;
            for(Integer partition: node.getPartitionIds()) {
                partitionIdToChunkOffset[partition] = globalChunk;
                partitionIdToNodeId[partition] = node.getId();
                for(int chunk = 0; chunk < numChunks; chunk++) {
                    File indexFile = new File(nodeDir, Integer.toString(partition) + "_"
                                                       + Integer.toString(chunk) + ".index");
                    File dataFile = new File(nodeDir, Integer.toString(partition) + "_"
                                                      + Integer.toString(chunk) + ".data");
                    positions[nodeId][globalChunk] = 0;
                    indexes[nodeId][globalChunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile),
                                                                                                 ioBufferSize));
                    datas[nodeId][globalChunk] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile),
                                                                                               ioBufferSize));
                    globalChunk++;
                }

            }
        }

        logger.info("Reading items...");
        int count = 0;
        ExternalSorter<KeyValuePair> sorter = new ExternalSorter<KeyValuePair>(new KeyValuePairSerializer(),
                                                                               new KeyMd5Comparator(),
                                                                               internalSortSize,
                                                                               tempDir.getAbsolutePath(),
                                                                               ioBufferSize,
                                                                               numThreads,
                                                                               gzipIntermediate);
        JsonObjectIterator iter = new JsonObjectIterator(reader, storeDefinition);

        // Put them into buckets on a per node-chunk basis
        ListMultimap<Pair<Integer, Integer>, KeyValuePair> buckets = ArrayListMultimap.create();

        for(KeyValuePair pair: sorter.sorted(iter)) {
            List<Integer> partitionIds = this.routingStrategy.getPartitionList(pair.getKey());
            for(Integer partitionId: partitionIds) {
                int localChunkId = ReadOnlyUtils.chunk(pair.getKeyMd5(), numChunks);
                int chunk = localChunkId + partitionIdToChunkOffset[partitionId];
                int nodeId = partitionIdToNodeId[partitionId];

                buckets.put(Pair.create(nodeId, chunk), pair);
            }
            count++;
        }

        logger.info(count + " items read.");

        for(Pair<Integer, Integer> bucket: buckets.keySet()) {
            List<KeyValuePair> keyValuePairs = buckets.get(bucket);
            int nodeId = bucket.getFirst();
            int chunk = bucket.getSecond();

            int index = 0;
            KeyValuePair prevPair = null, currentPair = null;
            while(currentPair != null || index < keyValuePairs.size()) {
                if(currentPair == null)
                    currentPair = keyValuePairs.get(index);

                indexes[nodeId][chunk].write(ByteUtils.copy(currentPair.keyMd5,
                                                            0,
                                                            2 * ByteUtils.SIZE_OF_INT));
                indexes[nodeId][chunk].writeInt(positions[nodeId][chunk]);

                int tuples = 0;
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                DataOutputStream valueStream = new DataOutputStream(stream);
                do {
                    valueStream.writeInt(currentPair.getKey().length);
                    valueStream.write(currentPair.getKey());
                    valueStream.writeInt(currentPair.getValue().length);
                    valueStream.write(currentPair.getValue());

                    index++;
                    tuples++;
                    if(index < keyValuePairs.size()) {
                        prevPair = currentPair;
                        currentPair = keyValuePairs.get(index);
                    } else {
                        currentPair = null;
                    }

                } while(currentPair != null
                        && ByteUtils.compare(ByteUtils.copy(prevPair.keyMd5,
                                                            0,
                                                            2 * ByteUtils.SIZE_OF_INT),
                                             ByteUtils.copy(currentPair.keyMd5,
                                                            0,
                                                            2 * ByteUtils.SIZE_OF_INT)) == 0);

                valueStream.flush();

                byte[] numBuf = new byte[1];
                numBuf[0] = (byte) tuples;

                datas[nodeId][chunk].write(numBuf);
                datas[nodeId][chunk].write(stream.toByteArray());

                positions[nodeId][chunk] += 1 + stream.toByteArray().length;
            }
        }

        // sort and write out
        logger.info("Closing all store files.");
        for(Node node: cluster.getNodes()) {
            for(int chunk = 0; chunk < numChunks * node.getNumberOfPartitions(); chunk++) {
                indexes[node.getId()][chunk].close();
                datas[node.getId()][chunk].close();
            }
        }
    }

    /* Check if the position has exceeded Integer.MAX_VALUE */
    private void checkOverFlow(int chunk, int position) {
        if(position < 0)
            throw new VoldemortException("Chunk overflow: chunk " + chunk + " has exceeded "
                                         + Integer.MAX_VALUE + " bytes.");
    }

    private static class KeyValuePairSerializer implements Serializer<KeyValuePair> {

        private final MessageDigest digest = ByteUtils.getDigest("MD5");

        public byte[] toBytes(KeyValuePair pair) {
            byte[] key = pair.getKey();
            byte[] value = pair.getValue();
            byte[] bytes = new byte[key.length + value.length + 8];
            ByteUtils.writeInt(bytes, key.length, 0);
            ByteUtils.writeInt(bytes, value.length, 4);
            System.arraycopy(key, 0, bytes, 8, key.length);
            System.arraycopy(value, 0, bytes, 8 + key.length, value.length);

            return bytes;
        }

        public KeyValuePair toObject(byte[] bytes) {
            int keySize = ByteUtils.readInt(bytes, 0);
            int valueSize = ByteUtils.readInt(bytes, 4);
            byte[] key = new byte[keySize];
            byte[] value = new byte[valueSize];
            System.arraycopy(bytes, 8, key, 0, keySize);
            System.arraycopy(bytes, 8 + keySize, value, 0, valueSize);
            byte[] md5 = digest.digest(key);
            digest.reset();

            return new KeyValuePair(key, md5, value);
        }

    }

    private static class JsonObjectIterator extends AbstractIterator<KeyValuePair> {

        private final JsonReader reader;
        private final Serializer<Object> keySerializer;
        private final Serializer<Object> valueSerializer;
        private final MessageDigest digest;
        private final SerializerDefinition keySerializerDefinition;
        private final SerializerDefinition valueSerializerDefinition;
        private CompressionStrategy valueCompressor;
        private CompressionStrategy keyCompressor;

        @SuppressWarnings("unchecked")
        public JsonObjectIterator(JsonReader reader, StoreDefinition storeDefinition) {
            SerializerFactory factory = new DefaultSerializerFactory();

            this.reader = reader;
            this.digest = ByteUtils.getDigest("MD5");
            this.keySerializerDefinition = storeDefinition.getKeySerializer();
            this.valueSerializerDefinition = storeDefinition.getValueSerializer();
            this.keySerializer = (Serializer<Object>) factory.getSerializer(storeDefinition.getKeySerializer());
            this.valueSerializer = (Serializer<Object>) factory.getSerializer(storeDefinition.getValueSerializer());
            this.keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
            this.valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());
        }

        @Override
        protected KeyValuePair computeNext() {
            try {
                Object key = reader.read();
                Object value = null;
                try {
                    value = reader.read();
                } catch(EndOfFileException e) {
                    throw new VoldemortException("Invalid file: reached end of file with key but no matching value.",
                                                 e);
                }
                byte[] keyBytes = keySerializer.toBytes(key);
                byte[] valueBytes = valueSerializer.toBytes(value);

                // compress key and values if required
                if(keySerializerDefinition.hasCompression()) {
                    keyBytes = keyCompressor.deflate(keyBytes);
                }

                if(valueSerializerDefinition.hasCompression()) {
                    valueBytes = valueCompressor.deflate(valueBytes);
                }

                byte[] keyMd5 = digest.digest(keyBytes);
                digest.reset();

                return new KeyValuePair(keyBytes, keyMd5, valueBytes);
            } catch(EndOfFileException e) {
                return endOfData();
            } catch(IOException e) {
                throw new VoldemortException("Unable to deflate key/value pair.", e);
            }
        }

    }

    public static class KeyMd5Comparator implements Comparator<KeyValuePair> {

        public int compare(KeyValuePair kv1, KeyValuePair kv2) {
            return ByteUtils.compare(kv1.getKeyMd5(), kv2.getKeyMd5());
        }

    }

    private static class KeyValuePair {

        private final byte[] key;
        private final byte[] keyMd5;
        private final byte[] value;

        public KeyValuePair(byte[] key, byte[] keyMd5, byte[] value) {
            this.key = key;
            this.keyMd5 = keyMd5;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getKeyMd5() {
            return this.keyMd5;
        }

        public byte[] getValue() {
            return value;
        }

        @Override
        public String toString() {
            return new String("Key - " + new String(this.key) + " - Value -  "
                              + new String(this.value) + " - KeyMD5 - " + new String(this.keyMd5));
        }
    }

}

/**
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.tools;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.codec.DecoderException;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class ImportTextDumpToBDB {
    static Integer READER_BUFFER_SIZE = 16777216;

    public static OptionParser getParser() {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("input"), "Input file or containing folder of data dump")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("input-file-or-folder");
        parser.acceptsAll(Arrays.asList("bdb"), "BDB folder store folder (not master)")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("bdb-folder");
        parser.acceptsAll(Arrays.asList("stores-xml"), "Location of stores xml")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("stores-xml");
        parser.acceptsAll(Arrays.asList("cluster-xml"), "Location of cluster xml")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("cluster-xml");
        parser.acceptsAll(Arrays.asList("node-id"), "Current node id")
                .withRequiredArg()
                .ofType(Integer.class)
                .describedAs("node-id");

        return parser;
    }

    public static void validateOptions(OptionSet options) throws IOException {
        Integer exitStatus = null;
        if(options.has("help")) {
            exitStatus = 0;
            System.out.println("This programs loads a data dump to a Voldemort Store.");
            System.out.println("Supported data dump should be a file or a folder containing files with lines of data.");
            System.out.println("Each line should be in the format of the following (all in Hex Decimal format)");
            System.out.println("\n\tKEY_BINARY VECTOR_CLOCK_BINARY VALUE_BINARY\n");
        }
        else if(!options.has("input")) {
            System.err.println("Option \"input\" is required");
            exitStatus = 1;
        }
        else if(!new File((String)options.valueOf("input")).isDirectory()) {
            System.err.println("Not a directory: " + options.valueOf("input") );
            exitStatus = 1;
        }
        else if(!options.has("bdb")) {
            System.err.println("Option \"bdb\" is required");
            exitStatus = 1;
        }
        else if(!options.has("stores-xml")) {
            System.err.println("Option \"stores-xml\" is required");
            exitStatus = 1;
        }
        else if(!new File((String)options.valueOf("stores-xml")).isFile()) {
            System.err.println("Not a file: " + options.valueOf("stores-xml") );
            exitStatus = 1;
        }
        else if(!options.has("cluster-xml")) {
            System.err.println("Option \"cluster-xml\" is required");
            exitStatus = 1;
        }
        else if(!new File((String)options.valueOf("cluster-xml")).isFile()) {
            System.err.println("Not a file: " + options.valueOf("cluster-xml") );
            exitStatus = 1;
        }
        if(exitStatus != null) {
            if(exitStatus == 0)
                getParser().printHelpOn(System.out);
            else
                getParser().printHelpOn(System.err);
            System.exit(exitStatus);
        }
    }

    public static void main(String[] argv) throws Exception {
        OptionParser parser = getParser();
        OptionSet options = parser.parse(argv);
        validateOptions(options);

        String inputPath = (String) options.valueOf("input");
        String storeBdbFolderPath = (String) options.valueOf("bdb");
        String clusterXmlPath = (String) options.valueOf("cluster-xml");
        String storesXmlPath = (String) options.valueOf("stores-xml");
        Integer nodeId = (Integer) options.valueOf("node-id");
        File input = new File(inputPath);
        List<File> dataFiles = new ArrayList<File>();
        if(input.isDirectory()) {
            File[] files = input.listFiles();
            if(files != null)
                Collections.addAll(dataFiles, files);
        } else if(input.isFile()) {
            dataFiles.add(input);
        } else {
            System.err.println(inputPath + "is not file or directory");
        }
        File storeBdbFolder = new File(storeBdbFolderPath);
        final String storeName = storeBdbFolder.getName();

        Cluster cluster = new ClusterMapper().readCluster(new File(clusterXmlPath));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlPath));
        StoreDefinition storeDef = null;
        for(StoreDefinition sd: storeDefs) {
            if(sd.getName() != null && sd.getName().equals(storeName)) {
                storeDef = sd;
            }
        }
        if(storeDef == null) {
            throw new VoldemortException("StoreNotfound: " + storeName);
        }
        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef, cluster);

        Properties properties = new Properties();
        properties.put("node.id","0");
        properties.put("voldemort.home",storeBdbFolder.getParent());
        VoldemortConfig voldemortConfig = new VoldemortConfig(properties);
        voldemortConfig.setBdbDataDirectory(storeBdbFolder.getParent());
        voldemortConfig.setEnableJmx(false);
        voldemortConfig.setBdbOneEnvPerStore(true);
        BdbStorageConfiguration bdbConfiguration = new BdbStorageConfiguration(voldemortConfig);
        class MockStoreDefinition extends StoreDefinition {
            public MockStoreDefinition() {
                super(storeName,null,null,null,null,null,null,null,0,null,0,null,0,null,null,null,null,null,null,null,null,null,null,null,null,0,null);
            }
            @Override
            public boolean hasMemoryFootprint() {
                return false;
            }
        }
        StoreDefinition mockStoreDef = new MockStoreDefinition();
        StorageEngine<ByteArray, byte[], byte[]> engine = bdbConfiguration.getStore(mockStoreDef, routingStrategy);
        long reportIntervalMs = 10000L;
        long lastCount = 0;
        long lastInserted = 0;
        Reporter<Boolean> rp = new Reporter<Boolean>(reportIntervalMs);

        long count = 0;
        long inserted = 0;
        for(File f: dataFiles) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(f), READER_BUFFER_SIZE);
                engine.beginBatchModifications();
                while(true) {
                    String line = bufferedReader.readLine();
                    if(line == null) {
                        break;
                    }
                    Pair<ByteArray, Versioned<byte[]>> entry;
                    try {
                        entry = lineToEntry(line);
                    } catch (Exception e) {
                        System.err.println("Skipping line: " + line);
                        e.printStackTrace();
                        continue;
                    }
                    ByteArray key = entry.getFirst();
                    List<Node> nodeList = routingStrategy.routeRequest(key.get());
                    for(Node node: nodeList) {
                        if(nodeId == node.getId()) {
                            try {
                                engine.put(key, entry.getSecond(), null);
                                inserted++;
                            } catch(ObsoleteVersionException e) {
                                e.printStackTrace();
                            }
                            break;
                        }
                    }
                    count++;
                    final Long countObject = count;
                    final Long insertedObject = inserted;
                    Boolean reported = rp.tryReport(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            System.out.print(String.format("Imported %15d entries; Inserted %15d entries", countObject, insertedObject));
                            return true;
                        }
                    });
                    if(reported != null) {
                        long importSpeed = (count - lastCount)/ (reportIntervalMs / 1000);
                        long insertSpeed = (inserted - lastInserted)/ (reportIntervalMs / 1000);
                        System.out.println(String.format("; ImportSpeed: %8d/s; InsertSpeed: %8d/s ", importSpeed, insertSpeed));
                        lastCount = count;
                        lastInserted = inserted;
                    }
                }
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                engine.endBatchModifications();
            }
        }
        engine.close();

        System.out.println(String.format("Finished importing %d entries (%d inserted, rest discarded)", count, inserted));
    }

    static private class Reporter<T> {
        Long intervalMs;
        Long lastReport;
        Reporter(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        public T tryReport(Callable<T> callable) throws Exception {
            if(lastReport == null) {
                lastReport = System.currentTimeMillis();
                return null;
            } else {
                if(lastReport + intervalMs < System.currentTimeMillis()) {
                    T result = callable.call();
                    lastReport = System.currentTimeMillis();
                    return result;
                }
            }
            return null;
        }
    }

    public static Pair<ByteArray, Versioned<byte[]>> lineToEntry(String line) throws DecoderException {
        String[] components = line.split(" ");

        String keyBytesString = components[0];
        byte[] keyBytes = ByteUtils.fromHexString(keyBytesString);
        ByteArray key = new ByteArray(keyBytes);

        String versionBytesString = components[1];
        byte[] versionBytes = ByteUtils.fromHexString(versionBytesString);
        Version version = new VectorClock(versionBytes, 0);

        String valueBytesString = components[1];
        byte[] value = ByteUtils.fromHexString(valueBytesString);

        return new Pair<ByteArray, Versioned<byte[]>>(key, new Versioned<byte[]>(value, version));
    }
}

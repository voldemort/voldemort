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

package voldemort.performance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.BinarySearchStrategy;
import voldemort.store.readonly.JsonStoreBuilder;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.SearchStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;

public class ReadOnlyStorePerformanceTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("threads", "number of threads").withRequiredArg().ofType(Integer.class);
        parser.accepts("requests", "[REQUIRED] number of requests")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("store-dir", "[REQUIRED] store directory")
              .withRequiredArg()
              .describedAs("directory");
        parser.accepts("search-strategy", "class of the search strategy to use")
              .withRequiredArg()
              .describedAs("class_name");
        parser.accepts("build", "If present, first build the data");
        parser.accepts("num-values", "The number of values in the store")
              .withRequiredArg()
              .describedAs("count")
              .ofType(Integer.class);
        parser.accepts("num-chunks", "The number of chunks in the store")
              .withRequiredArg()
              .describedAs("chunks")
              .ofType(Integer.class);
        parser.accepts("internal-sort-size", "The number of items to sort in memory at a time")
              .withRequiredArg()
              .describedAs("size")
              .ofType(Integer.class);
        parser.accepts("value-size", "The size of the values in the store")
              .withRequiredArg()
              .describedAs("size")
              .ofType(Integer.class);
        parser.accepts("working-dir", "The directory in which to store temporary data")
              .withRequiredArg()
              .describedAs("dir");
        parser.accepts("request-file", "file get request ids from").withRequiredArg();
        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        failIfMissing(parser, options, "requests", "store-dir");

        final int numThreads = CmdUtils.valueOf(options, "threads", 10);
        final int numRequests = (Integer) options.valueOf("requests");
        final int internalSortSize = CmdUtils.valueOf(options, "internal-sort-size", 500000);
        int numValues = numRequests;
        final String inputFile = (String) options.valueOf("request-file");
        final String searcherClass = CmdUtils.valueOf(options,
                                                      "search-strategy",
                                                      BinarySearchStrategy.class.getName()).trim();
        final SearchStrategy searcher = (SearchStrategy) ReflectUtils.callConstructor(ReflectUtils.loadClass(searcherClass));
        final File workingDir = new File(CmdUtils.valueOf(options,
                                                          "working-dir",
                                                          System.getProperty("java.io.tmpdir")));
        String storeDir = (String) options.valueOf("store-dir");

        if(options.has("build")) {
            failIfMissing(parser, options, "num-values", "value-size");
            numValues = (Integer) options.valueOf("num-values");
            int numChunks = 1;
            if(options.has("num-chunks"))
                numChunks = (Integer) options.valueOf("num-chunks");
            int valueSize = (Integer) options.valueOf("value-size");

            // generate test data
            File temp = File.createTempFile("json-data", ".txt.gz", workingDir);
            temp.deleteOnExit();
            System.out.println("Generating test data in " + temp);
            OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(temp));
            Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream),
                                               10 * 1024 * 1024);
            String value = TestUtils.randomLetters(valueSize);
            for(int i = 0; i < numValues; i++) {
                writer.write("\"");
                writer.write(Integer.toString(i));
                writer.write("\" \"");
                writer.write(value);
                writer.write("\"");
                writer.write("\n");
            }
            writer.close();
            writer = null;

            System.out.println("Building store.");
            InputStream inputStream = new GZIPInputStream(new FileInputStream(temp));
            Reader r = new BufferedReader(new InputStreamReader(inputStream), 1 * 1024 * 1024);
            File output = TestUtils.createTempDir(workingDir);
            File tempDir = TestUtils.createTempDir(workingDir);
            SerializerDefinition sdef = new SerializerDefinition("json", "'string'");
            Cluster cluster = ServerTestUtils.getLocalCluster(1);
            StoreDefinition storeDef = new StoreDefinitionBuilder().setName("test")
                                                                   .setKeySerializer(sdef)
                                                                   .setValueSerializer(sdef)
                                                                   .setRequiredReads(1)
                                                                   .setReplicationFactor(1)
                                                                   .setRequiredWrites(1)
                                                                   .setType("read-only")
                                                                   .setRoutingPolicy(RoutingTier.CLIENT)
                                                                   .build();
            JsonStoreBuilder builder = new JsonStoreBuilder(new JsonReader(r),
                                                            cluster,
                                                            storeDef,
                                                            new ConsistentRoutingStrategy(cluster.getNodes(),
                                                                                          1),
                                                            output,
                                                            tempDir,
                                                            internalSortSize,
                                                            2,
                                                            numChunks,
                                                            64 * 1024);
            builder.build();

            // copy to store dir
            File dir = new File(storeDir);
            Utils.rm(dir);
            dir.mkdirs();
            System.out.println("Moving store data from " + output + " to " + dir);
            boolean copyWorked = new File(output, "node-0").renameTo(new File(dir, "version-0"));
            if(!copyWorked)
                Utils.croak("Copy of data from " + output + " to " + dir + " failed.");
        }

        final Store<ByteArray, byte[]> store = new ReadOnlyStorageEngine("test",
                                                                         searcher,
                                                                         new File(storeDir),
                                                                         0);

        final AtomicInteger obsoletes = new AtomicInteger(0);
        final AtomicInteger nullResults = new AtomicInteger(0);
        final AtomicInteger totalResults = new AtomicInteger(0);

        final BlockingQueue<String> requestIds = new ArrayBlockingQueue<String>(20000);
        final Executor executor = Executors.newFixedThreadPool(1);

        // if they have given us a file make a request generator that reads from
        // it, otherwise just generate random values
        final int numVals = numValues;
        Runnable requestGenerator;
        if(inputFile == null) {
            requestGenerator = new Runnable() {

                public void run() {
                    System.out.println("Generating random requests.");
                    Random random = new Random();
                    try {
                        while(true)
                            requestIds.put(Integer.toString(random.nextInt(numRequests) % numVals));
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
        } else {
            requestGenerator = new Runnable() {

                public void run() {
                    try {
                        System.out.println("Using request file to generate requests.");
                        BufferedReader reader = new BufferedReader(new FileReader(inputFile),
                                                                   1000000);
                        while(true) {
                            String line = reader.readLine();
                            if(line == null)
                                return;
                            requestIds.put(line.trim());
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        }
        executor.execute(requestGenerator);

        final Serializer<Object> keySerializer = new JsonTypeSerializer(JsonTypeDefinition.fromJson("'string'"),
                                                                        true);
        final AtomicInteger current = new AtomicInteger();
        final int progressIncrement = numRequests / 5;
        PerformanceTest readWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                try {
                    totalResults.incrementAndGet();
                    int curr = current.getAndIncrement();
                    List<Versioned<byte[]>> results = store.get(new ByteArray(keySerializer.toBytes(requestIds.take())));
                    if(curr % progressIncrement == 0)
                        System.out.println(curr);

                    if(results.size() == 0)
                        nullResults.incrementAndGet();

                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        System.out.println("Running test...");
        readWriteTest.run(numRequests, numThreads);
        System.out.println("Random Access Read Only store Results:");
        System.out.println("Null reads ratio:" + (nullResults.doubleValue())
                           / totalResults.doubleValue());
        readWriteTest.printStats();
        System.exit(0);
    }

    private static void failIfMissing(OptionParser parser, OptionSet options, String... required) {
        Set<String> missing = CmdUtils.missing(options, required);
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            try {
                parser.printHelpOn(System.err);
            } catch(IOException e) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }
}

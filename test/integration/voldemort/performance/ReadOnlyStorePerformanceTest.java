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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.serialization.Serializer;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.Store;
import voldemort.store.readonly.BinarySearchStrategy;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.SearchStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.ReflectUtils;
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
        parser.accepts("request-file", "file get request ids from").withRequiredArg();
        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options, "requests", "store-dir");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        final int numThreads = CmdUtils.valueOf(options, "threads", 10);
        final int numRequests = (Integer) options.valueOf("requests");
        final String inputFile = (String) options.valueOf("request-file");
        final String searcherClass = CmdUtils.valueOf(options,
                                                      "search-strategy",
                                                      BinarySearchStrategy.class.getName()).trim();
        final SearchStrategy searcher = (SearchStrategy) ReflectUtils.callConstructor(ReflectUtils.loadClass(searcherClass));
        String storeDir = (String) options.valueOf("store-dir");

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
        Runnable requestGenerator;
        if(inputFile == null) {
            requestGenerator = new Runnable() {

                public void run() {
                    System.out.println("Generating random requests.");
                    Random random = new Random();
                    try {
                        while(true)
                            requestIds.put(Integer.toString(random.nextInt(numRequests)));
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
        PerformanceTest readWriteTest = new PerformanceTest() {

            @Override
            public void doOperation(int index) throws Exception {
                try {
                    totalResults.incrementAndGet();
                    int curr = current.getAndIncrement();
                    List<Versioned<byte[]>> results = store.get(new ByteArray(keySerializer.toBytes(requestIds.take())));
                    if(curr % 10000 == 0)
                        System.out.println(curr);

                    if(results.size() == 0)
                        nullResults.incrementAndGet();

                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        readWriteTest.run(numRequests, numThreads);
        System.out.println("Random Access Read Only store Results:");
        System.out.println("Null reads ratio:" + (nullResults.doubleValue())
                           / totalResults.doubleValue());
        readWriteTest.printStats();
        System.exit(0);
    }
}

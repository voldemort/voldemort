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
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.utils.CmdUtils;
import voldemort.versioning.Versioned;

public class RemoteTest {

    public static final int MAX_WORKERS = 8;

    public static class KeyProvider {

        private final List<String> keys;
        private final AtomicInteger index;

        public KeyProvider(int start, List<String> keys) {
            this.index = new AtomicInteger(start);
            this.keys = keys;
        }

        public String next() {
            if(keys != null) {
                return keys.get(index.getAndIncrement() % keys.size());
            } else {
                return Integer.toString(index.getAndIncrement());
            }
        }
    }

    public static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/bin/remote-test.sh \\");
        out.println("          [options] bootstrapUrl storeName num-requests\n");
        parser.printHelpOn(out);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("r", "execute read operations");
        parser.accepts("w", "execute write operations");
        parser.accepts("d", "execute delete operations");
        parser.accepts("i", "ignore null values");
        parser.accepts("v", "verbose");
        parser.accepts("request-file", "execute specific requests in order").withRequiredArg();
        parser.accepts("start-key-index", "starting point when using int keys. Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("value-size", "size in bytes for random value.  Default = 1024")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("iterations", "number of times to repeat the test  Default = 1")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("threads", "max number concurrent worker threads  Default = " + MAX_WORKERS)
              .withRequiredArg()
              .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();
        if(nonOptions.size() != 3) {
            printUsage(System.err, parser);
        }

        String url = nonOptions.get(0);
        String storeName = nonOptions.get(1);
        int numRequests = Integer.parseInt(nonOptions.get(2));
        String ops = "";
        List<String> keys = null;

        Integer startNum = CmdUtils.valueOf(options, "start-key-index", 0);
        Integer valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        Integer numIterations = CmdUtils.valueOf(options, "iterations", 1);
        Integer numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        final boolean verbose = options.has("v");

        if(options.has("request-file")) {
            keys = loadKeys((String) options.valueOf("request-file"));
        }

        if(options.has("r")) {
            ops += "r";
        }
        if(options.has("w")) {
            ops += "w";
        }
        if(options.has("d")) {
            ops += "d";
        }

        if(ops.length() == 0) {
            ops = "rwd";
        }

        System.out.println("operations : " + ops);
        System.out.println("value size : " + valueSize);
        System.out.println("start index : " + startNum);
        System.out.println("iterations : " + numIterations);
        System.out.println("threads : " + numThreads);

        System.out.println("Bootstraping cluster data.");
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setMaxThreads(numThreads)
                                                                                    .setMaxTotalConnections(numThreads)
                                                                                    .setMaxConnectionsPerNode(numThreads)
                                                                                    .setBootstrapUrls(url)
                                                                                    .setConnectionTimeout(60,
                                                                                                          TimeUnit.SECONDS)
                                                                                    .setSocketTimeout(60,
                                                                                                      TimeUnit.SECONDS)
                                                                                    .setSocketBufferSize(4 * 1024));
        final StoreClient<String, String> store = factory.getStoreClient(storeName);

        final String value = TestUtils.randomLetters(valueSize);
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        /*
         * send the store a value and then delete it - useful for the NOOP store
         * which will then use that value for other queries
         */

        final String key = new KeyProvider(startNum, keys).next();

        // We need to delete just in case there's an existing value there that
        // would otherwise cause the test run to bomb out.
        store.delete(key);
        store.put(key, new Versioned<String>(value));
        store.delete(key);

        for(int loopCount = 0; loopCount < numIterations; loopCount++) {

            System.out.println("======================= iteration = " + loopCount
                               + " ======================================");

            if(ops.contains("d")) {
                System.out.println("Beginning delete test.");
                final AtomicInteger successes = new AtomicInteger(0);
                final KeyProvider keyProvider0 = new KeyProvider(startNum, keys);
                final CountDownLatch latch0 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                store.delete(keyProvider0.next());
                                successes.getAndIncrement();
                            } catch(Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch0.countDown();
                            }
                        }
                    });
                }
                latch0.await();
                long deleteTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numRequests / (float) deleteTime * 1000)
                                   + " deletes/sec.");
                System.out.println(successes.get() + " things deleted.");
            }

            if(ops.contains("w")) {
                final AtomicInteger numWrites = new AtomicInteger(0);
                System.out.println("Beginning write test.");
                final KeyProvider keyProvider1 = new KeyProvider(startNum, keys);
                final CountDownLatch latch1 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                String key = keyProvider1.next();
                                store.put(key, value);
                                numWrites.incrementAndGet();
                            } catch(Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                            } finally {
                                latch1.countDown();
                            }
                        }
                    });
                }
                latch1.await();
                long writeTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numWrites.get() / (float) writeTime * 1000)
                                   + " writes/sec.");
            }

            if(ops.contains("r")) {
                final boolean ignoreNulls = options.has("i");
                final AtomicInteger numReads = new AtomicInteger(0);
                System.out.println("Beginning read test.");
                final KeyProvider keyProvider2 = new KeyProvider(startNum, keys);
                final CountDownLatch latch2 = new CountDownLatch(numRequests);
                long start = System.currentTimeMillis();
                keyProvider2.next();
                for(int i = 0; i < numRequests; i++) {
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                String key = keyProvider2.next();
                                Versioned<String> v = store.get(key);
                                numReads.incrementAndGet();
                                if(v == null && !ignoreNulls) {
                                    throw new Exception("value returned is null for key " + key);
                                }

                                if(!value.equals(v.getValue())) {
                                    throw new Exception("value returned isn't same as set value.  My val size = "
                                                        + value.length()
                                                        + " ret size = "
                                                        + v.getValue().length() + " for key " + key);
                                }

                            } catch(Exception e) {
                                if (verbose) {
                                    e.printStackTrace();
                                }
                            } finally {
                                latch2.countDown();
                            }
                        }
                    });
                }
                latch2.await();
                long readTime = System.currentTimeMillis() - start;
                System.out.println("Throughput: " + (numReads.get() / (float) readTime * 1000.0)
                                   + " reads/sec.");
            }
        }

        System.exit(0);
    }

    public static List<String> loadKeys(String path) throws IOException {

        List<String> targets = new ArrayList<String>();
        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null) {
                targets.add(text);
            }
        } finally {
            try {
                if(reader != null) {
                    reader.close();
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }

        return targets;
    }
}

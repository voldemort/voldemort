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

package voldemort.performance;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.CmdUtils;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class RemoteTest {

    public static final int MAX_WORKERS = 8;

    public interface KeyProvider<T> {

        public T next();
    }

    public abstract static class AbstractKeyProvider<T> implements KeyProvider<T> {

        private final List<Integer> keys;
        private final AtomicInteger index;

        private AbstractKeyProvider(int start, List<Integer> keys) {
            this.index = new AtomicInteger(start);
            this.keys = keys;
        }

        public Integer nextInteger() {
            if(keys != null) {
                return keys.get(index.getAndIncrement() % keys.size());
            } else {
                return index.getAndIncrement();
            }
        }

        public abstract T next();
    }

    public static class IntegerKeyProvider extends AbstractKeyProvider<Integer> {

        private IntegerKeyProvider(int start, List<Integer> keys) {
            super(start, keys);
        }

        @Override
        public Integer next() {
            return nextInteger();
        }
    }

    public static class StringKeyProvider extends AbstractKeyProvider<String> {

        private StringKeyProvider(int start, List<Integer> keys) {
            super(start, keys);
        }

        @Override
        public String next() {
            return Integer.toString(nextInteger());
        }
    }

    public static class ByteArrayKeyProvider extends AbstractKeyProvider<byte[]> {

        private ByteArrayKeyProvider(int start, List<Integer> keys) {
            super(start, keys);
        }

        @Override
        public byte[] next() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(nextInteger());
            return bos.toByteArray();
        }
    }

    public static class CachedKeyProvider<T> implements KeyProvider<T> {

        private final KeyProvider<T> delegate;

        private final int percentCached;

        private final AtomicInteger totalRequests = new AtomicInteger(0);

        private final AtomicInteger cachedRequests = new AtomicInteger(0);

        private final List<T> visitedKeys = new ArrayList<T>();

        private CachedKeyProvider(KeyProvider<T> delegate, int percentCached) {
            this.delegate = delegate;
            this.percentCached = percentCached;
        }

        public T next() {
            int expectedCacheCount = (totalRequests.getAndIncrement() * percentCached) / 100;

            if(expectedCacheCount >= cachedRequests.get()) {
                synchronized(visitedKeys) {
                    if(!visitedKeys.isEmpty()) {
                        cachedRequests.incrementAndGet();
                        return visitedKeys.get(new Random().nextInt(visitedKeys.size()));
                    }
                }
            }

            T value = delegate.next();

            synchronized(visitedKeys) {
                visitedKeys.add(value);
            }

            return value;
        }

    }

    public static KeyProvider<?> getKeyProvider(Class<?> cls,
                                                int start,
                                                List<Integer> keys,
                                                int percentCached) {
        if(cls == Integer.class) {
            IntegerKeyProvider kp = new IntegerKeyProvider(start, keys);
            return percentCached != 0 ? new CachedKeyProvider<Integer>(kp, percentCached) : kp;
        } else if(cls == String.class) {
            StringKeyProvider kp = new StringKeyProvider(start, keys);
            return percentCached != 0 ? new CachedKeyProvider<String>(kp, percentCached) : kp;
        } else if(cls == byte[].class) {
            ByteArrayKeyProvider kp = new ByteArrayKeyProvider(start, keys);
            return percentCached != 0 ? new CachedKeyProvider<byte[]>(kp, percentCached) : kp;
        } else {
            throw new IllegalArgumentException("No KeyProvider exists for class " + cls);
        }
    }

    public static void printUsage(PrintStream out, OptionParser parser) throws IOException {
        out.println("Usage: $VOLDEMORT_HOME/bin/remote-test.sh \\");
        out.println("          [options] bootstrapUrl storeName num-requests\n");
        parser.printHelpOn(out);
        System.exit(1);
    }

    public static Class<?> findKeyType(StoreDefinition storeDefinition) throws Exception {
        SerializerDefinition serializerDefinition = storeDefinition.getKeySerializer();
        if(serializerDefinition != null) {
            if("string".equals(serializerDefinition.getName())) {
                return String.class;
            } else if("json".equals(serializerDefinition.getName())) {
                if(serializerDefinition.getCurrentSchemaInfo().contains("int")) {
                    return Integer.class;
                } else if(serializerDefinition.getCurrentSchemaInfo().contains("string")) {
                    return String.class;
                }
            } else if("identity".equals(serializerDefinition.getName())) {
                return byte[].class;
            }
        }

        throw new Exception("Can't determine key type for key serializer "
                            + storeDefinition.getName());
    }

    public static void main(String[] args) throws Exception {

        OptionParser parser = new OptionParser();
        parser.accepts("r", "execute read operations");
        parser.accepts("w", "execute write operations");
        parser.accepts("d", "execute delete operations");
        parser.accepts("m", "generate a mix of read and write requests");
        parser.accepts("v", "verbose");
        parser.accepts("ignore-nulls", "ignore null values");
        parser.accepts("pipeline-routed-store", "Use the Pipeline RoutedStore");
        parser.accepts("node", "go to this node id").withRequiredArg().ofType(Integer.class);
        parser.accepts("interval", "print requests on this interval, -1 to disable")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("handshake", "perform a handshake");
        parser.accepts("verify", "verify values read");
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
        parser.accepts("percent-cached",
                       "percentage of requests to come from previously requested keys; valid values are in range [0..100]; 0 means caching disabled  Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("help");

        OptionSet options = parser.parse(args);

        List<String> nonOptions = options.nonOptionArguments();

        if(options.has("help")) {
            printUsage(System.out, parser);
        }

        if(nonOptions.size() != 3) {
            printUsage(System.err, parser);
        }

        String url = nonOptions.get(0);
        String storeName = nonOptions.get(1);
        int numRequests = Integer.parseInt(nonOptions.get(2));
        String ops = "";
        List<Integer> keys = null;

        Integer startNum = CmdUtils.valueOf(options, "start-key-index", 0);
        Integer valueSize = CmdUtils.valueOf(options, "value-size", 1024);
        Integer numIterations = CmdUtils.valueOf(options, "iterations", 1);
        Integer numThreads = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        Integer percentCached = CmdUtils.valueOf(options, "percent-cached", 0);

        if(percentCached < 0 || percentCached > 100) {
            printUsage(System.err, parser);
        }

        Integer nodeId = CmdUtils.valueOf(options, "node", 0);
        final Integer interval = CmdUtils.valueOf(options, "interval", 100000);
        final boolean verifyValues = options.has("verify");
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
        if(options.has("m")) {
            ops += "m";
        }
        if(ops.length() == 0) {
            ops = "rwd";
        }

        System.out.println("operations : " + ops);
        System.out.println("value size : " + valueSize);
        System.out.println("start index : " + startNum);
        System.out.println("iterations : " + numIterations);
        System.out.println("threads : " + numThreads);
        System.out.println("cache percentage : " + percentCached + "%");

        System.out.println("Bootstraping cluster data.");
        ClientConfig clientConfig = new ClientConfig().setMaxThreads(numThreads)
                                                      .setMaxTotalConnections(numThreads)
                                                      .setMaxConnectionsPerNode(numThreads)
                                                      .setBootstrapUrls(url)
                                                      .setConnectionTimeout(60, TimeUnit.SECONDS)
                                                      .setSocketTimeout(60, TimeUnit.SECONDS)
                                                      .setSocketBufferSize(4 * 1024)
                                                      .setEnablePipelineRoutedStore(options.has("pipeline-routed-store"));
        SocketStoreClientFactory factory = new SocketStoreClientFactory(clientConfig);
        final StoreClient<Object, Object> store = factory.getStoreClient(storeName);
        StoreDefinition storeDef = getStoreDefinition(factory, storeName);

        Class<?> keyType = findKeyType(storeDef);
        final String value = TestUtils.randomLetters(valueSize);
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        /*
         * send the store a value and then delete it - useful for the NOOP store
         * which will then use that value for other queries
         */

        if(options.has("handshake")) {
            final Object key = getKeyProvider(keyType, startNum, keys, 0).next();

            // We need to delete just in case there's an existing value there
            // that would otherwise cause the test run to bomb out.
            store.delete(key);
            store.put(key, new Versioned<String>(value));
            store.delete(key);
        }

        for(int loopCount = 0; loopCount < numIterations; loopCount++) {

            System.out.println("======================= iteration = " + loopCount
                               + " ======================================");

            if(ops.contains("d")) {
                System.out.println("Beginning delete test.");
                final AtomicInteger successes = new AtomicInteger(0);
                final KeyProvider<?> keyProvider0 = getKeyProvider(keyType,
                                                                   startNum,
                                                                   keys,
                                                                   percentCached);
                final CountDownLatch latch0 = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                for(int i = 0; i < numRequests; i++) {
                    final int j = i;
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                long startNs = System.nanoTime();
                                store.delete(keyProvider0.next());
                                requestTimes[j] = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                                successes.getAndIncrement();
                            } catch(Exception e) {
                                e.printStackTrace();
                            } finally {
                                latch0.countDown();
                                if(interval != -1 && j % interval == 0) {
                                    printStatistics("deletes", successes.get(), start);
                                }
                            }
                        }
                    });
                }
                latch0.await();
                printStatistics("deletes", successes.get(), start);
                System.out.println("95th percentile delete latency: "
                                   + TestUtils.quantile(requestTimes, .95) + " ms.");
                System.out.println("99th percentile delete latency: "
                                   + TestUtils.quantile(requestTimes, .99) + " ms.");
            }

            if(ops.contains("w")) {
                final AtomicInteger numWrites = new AtomicInteger(0);
                System.out.println("Beginning write test.");
                final KeyProvider<?> keyProvider1 = getKeyProvider(keyType,
                                                                   startNum,
                                                                   keys,
                                                                   percentCached);
                final CountDownLatch latch1 = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                for(int i = 0; i < numRequests; i++) {
                    final int j = i;
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                final Object key = keyProvider1.next();
                                store.applyUpdate(new UpdateAction<Object, Object>() {

                                    public void update(StoreClient<Object, Object> storeClient) {
                                        long startNs = System.nanoTime();
                                        storeClient.put(key, value);
                                        requestTimes[j] = (System.nanoTime() - startNs)
                                                          / Time.NS_PER_MS;
                                        numWrites.incrementAndGet();
                                    }
                                }, 64);
                            } catch(Exception e) {
                                if(verbose) {
                                    e.printStackTrace();
                                }
                            } finally {
                                latch1.countDown();
                                if(interval != -1 && j % interval == 0) {
                                    printStatistics("writes", numWrites.get(), start);
                                }
                            }
                        }
                    });
                }
                latch1.await();
                printStatistics("writes", numWrites.get(), start);
                System.out.println("95th percentile write latency: "
                                   + TestUtils.quantile(requestTimes, .95) + " ms.");
                System.out.println("99th percentile write latency: "
                                   + TestUtils.quantile(requestTimes, .99) + " ms.");
            }

            if(ops.contains("r")) {
                final boolean ignoreNulls = options.has("ignore-nulls");
                final AtomicInteger numReads = new AtomicInteger(0);
                final AtomicInteger numNulls = new AtomicInteger(0);
                System.out.println("Beginning read test.");
                final KeyProvider<?> keyProvider = getKeyProvider(keyType,
                                                                  startNum,
                                                                  keys,
                                                                  percentCached);
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final long[] requestTimes = new long[numRequests];
                final long start = System.nanoTime();
                keyProvider.next();
                for(int i = 0; i < numRequests; i++) {
                    final int j = i;
                    service.execute(new Runnable() {

                        public void run() {
                            try {
                                Object key = keyProvider.next();
                                long startNs = System.nanoTime();
                                Versioned<Object> v = store.get(key);
                                requestTimes[j] = (System.nanoTime() - startNs) / Time.NS_PER_MS;
                                numReads.incrementAndGet();
                                if(v == null) {
                                    numNulls.incrementAndGet();
                                    if(!ignoreNulls) {
                                        throw new Exception("value returned is null for key " + key);
                                    }
                                }

                                if(verifyValues && !value.equals(v.getValue())) {
                                    throw new Exception("value returned isn't same as set value for key "
                                                        + key);
                                }
                            } catch(Exception e) {
                                if(verbose) {
                                    e.printStackTrace();
                                }
                            } finally {
                                latch.countDown();
                                if(interval != -1 && j % interval == 0) {
                                    printStatistics("reads", numReads.get(), start);
                                    printNulls(numNulls.get(), start);
                                }
                            }
                        }
                    });
                }
                latch.await();
                printStatistics("reads", numReads.get(), start);
                System.out.println("95th percentile read latency: "
                                   + TestUtils.quantile(requestTimes, .95) + " ms.");
                System.out.println("99th percentile read latency: "
                                   + TestUtils.quantile(requestTimes, .99) + " ms.");
            }
        }

        if(ops.contains("m")) {
            final AtomicInteger numNulls = new AtomicInteger(0);
            final AtomicInteger numReads = new AtomicInteger(0);
            final AtomicInteger numWrites = new AtomicInteger(0);
            System.out.println("Beginning mixed test.");
            final KeyProvider<?> keyProvider = getKeyProvider(keyType,
                                                              startNum,
                                                              keys,
                                                              percentCached);
            final CountDownLatch latch = new CountDownLatch(numRequests);
            final long start = System.nanoTime();
            keyProvider.next();
            for(int i = 0; i < numRequests; i++) {
                final int j = i;
                service.execute(new Runnable() {

                    public void run() {
                        try {
                            final Object key = keyProvider.next();

                            store.applyUpdate(new UpdateAction<Object, Object>() {

                                public void update(StoreClient<Object, Object> storeClient) {
                                    Versioned<Object> v = store.get(key);
                                    numReads.incrementAndGet();
                                    if(v != null) {
                                        storeClient.put(key, v);
                                    } else {
                                        numNulls.incrementAndGet();
                                    }
                                    numWrites.incrementAndGet();
                                }
                            }, 64);
                        } catch(Exception e) {
                            if(verbose) {
                                e.printStackTrace();
                            }
                        } finally {
                            if(interval != -1 && j % interval == 0) {
                                printStatistics("reads", numReads.get(), start);
                                printStatistics("writes", numWrites.get(), start);
                                printNulls(numNulls.get(), start);
                                printStatistics("transactions", j, start);
                            }
                            latch.countDown();

                        }
                    }
                });
            }
            latch.await();
            printStatistics("reads", numReads.get(), start);
            printStatistics("writes", numWrites.get(), start);
        }

        System.exit(0);
    }

    private static StoreDefinition getStoreDefinition(AbstractStoreClientFactory factory,
                                                      String storeName) {
        String storesXml = factory.bootstrapMetadataWithRetries(MetadataStore.STORES_KEY);
        StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();
        List<StoreDefinition> storeDefinitionList = storeMapper.readStoreList(new StringReader(storesXml));

        StoreDefinition storeDef = null;
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            if(storeName.equals(storeDefinition.getName())) {
                storeDef = storeDefinition;
            }
        }
        return storeDef;
    }

    private static void printNulls(int nulls, long start) {
        long nullTime = System.nanoTime() - start;
        System.out.println((nulls / (float) nullTime * Time.NS_PER_SECOND) + " nulls/sec");
        System.out.println(nulls + " null values.");
    }

    private static void printStatistics(String noun, int successes, long start) {
        long queryTime = System.nanoTime() - start;
        System.out.println("Throughput: " + (successes / (float) queryTime * Time.NS_PER_SECOND)
                           + " " + noun + "/sec.");
        System.out.println(successes + " successful " + noun + ".");
    }

    public static List<Integer> loadKeys(String path) throws IOException {

        List<Integer> targets = new ArrayList<Integer>();
        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null) {
                targets.add(Integer.valueOf(text.replaceAll("\\s+", "")));
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

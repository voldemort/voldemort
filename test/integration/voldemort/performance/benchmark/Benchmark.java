/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.performance.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.ServerTestUtils;
import voldemort.StaticStoreClientFactory;
import voldemort.VoldemortException;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.StringSerializer;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.StorageConfiguration;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

public class Benchmark {

    private static final int MAX_WORKERS = 8;

    /**
     * Constants for the benchmark file
     */
    public static final String PROP_FILE = "prop-file";
    public static final String THREADS = "threads";
    public static final String ITERATIONS = "iterations";
    public static final String STORAGE_ENGINE_TYPE = "storage-engine";
    public static final String INTERVAL = "interval";
    public static final String KEY_TYPE = "keyType";
    public static final String STRING_KEY_TYPE = "string";
    public static final String JSONINT_KEY_TYPE = "json-int";
    public static final String JSONSTRING_KEY_TYPE = "json-string";
    public static final String IDENTITY_KEY_TYPE = "identity";
    public static final String HANDSHAKE = "handshake";
    public static final String URL = "url";
    public static final String PERCENT_CACHED = "percent-cached";
    public static final String VALUE_SIZE = "value-size";
    public static final String IGNORE_NULLS = "ignore-nulls";
    public static final String REQUEST_FILE = "request-file";
    public static final String START_KEY_INDEX = "start-key-index";
    public static final String READS = "r";
    public static final String WRITES = "w";
    public static final String DELETES = "d";
    public static final String MIXED = "m";
    public static final String RECORD_SELECTION = "record-selection";
    public static final String ZIPFIAN_RECORD_SELECTION = "zipfian";
    public static final String LATEST_RECORD_SELECTION = "latest";
    public static final String FILE_RECORD_SELECTION = "file";
    public static final String UNIFORM_RECORD_SELECTION = "uniform";
    public static final String TARGET_THROUGHPUT = "target-throughput";
    public static final String HELP = "help";
    public static final String STORE_NAME = "store-name";
    public static final String RECORD_COUNT = "record-count";
    public static final String OPS_COUNT = "ops-count";
    public static final String METRIC_TYPE = "metric-type";
    public static final String HISTOGRAM_METRIC_TYPE = "histogram";
    public static final String SUMMARY_METRIC_TYPE = "summary";
    public static final String VERBOSE = "v";
    public static final String VERIFY = "verify";
    private static final String DUMMY_DB = "benchmark_db";

    private StoreClient<Object, Object> storeClient;
    private StoreClientFactory factory;

    private int numThreads;
    private int numIterations;
    private int targetThroughput;
    private double perThreadThroughputPerMs;
    private int recordCount, opsCount;
    private Workload workLoad;
    private int statusIntervalSec;
    private boolean storeInitialized = false;
    private boolean verbose = false;
    private boolean verifyRead = false;
    private boolean ignoreNulls = false;
    private String keyType;

    class StatusThread extends Thread {

        private Vector<Thread> threads;
        private int intervalSec;
        private long startTime;

        public StatusThread(Vector<Thread> threads, int intervalSec, long startTime) {
            this.threads = threads;
            this.intervalSec = intervalSec;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            boolean testComplete = true;
            int totalOps = 0, prevTotalOps = 0;
            do {
                testComplete = true;
                totalOps = 0;
                for(Thread thread: this.threads) {
                    if(thread.getState() != Thread.State.TERMINATED) {
                        testComplete = false;
                    }
                    totalOps += ((ClientThread) thread).getOpsDone();
                }

                if(totalOps != 0 && totalOps != prevTotalOps) {
                    System.out.println("[status]\tThroughput(ops/sec): "
                                       + Time.MS_PER_SECOND
                                       * ((double) totalOps / (double) (System.currentTimeMillis() - startTime))
                                       + "\tOperations: " + totalOps);
                }
                prevTotalOps = totalOps;
                try {
                    sleep(intervalSec * Time.MS_PER_SECOND);
                } catch(InterruptedException e) {}
            } while(!testComplete);
        }
    }

    class ClientThread extends Thread {

        private VoldemortWrapper db;
        private boolean runBenchmark;
        private boolean verbose;
        private Workload workLoad;
        private int opsCount;
        private double targetThroughputPerMs;
        private int opsDone;

        public ClientThread(VoldemortWrapper db,
                            boolean runBenchmark,
                            Workload workLoad,
                            int opsCount,
                            double targetThroughputPerMs,
                            boolean verbose) {
            this.db = db;
            this.runBenchmark = runBenchmark;
            this.workLoad = workLoad;
            this.opsCount = opsCount;
            this.opsDone = 0;
            this.targetThroughputPerMs = targetThroughputPerMs;
            this.verbose = verbose;
        }

        public int getOpsDone() {
            return this.opsDone;
        }

        public void run() {
            long startTime = System.currentTimeMillis();
            while(opsDone < this.opsCount) {
                if(runBenchmark) {
                    if(!workLoad.doTransaction(this.db)) {
                        break;
                    }
                } else {
                    if(!workLoad.doWrite(this.db)) {
                        break;
                    }
                }
                opsDone++;
                if(targetThroughputPerMs > 0) {
                    while(System.currentTimeMillis() - startTime < ((double) opsDone)
                                                                   / targetThroughputPerMs) {
                        try {
                            sleep(1);
                        } catch(InterruptedException e) {}
                    }
                }
            }
        }
    }

    private StoreDefinition getStoreDefinition(AbstractStoreClientFactory factory, String storeName) {
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

    private Object getTempKey(String keyType) {
        if(keyType.compareTo(STRING_KEY_TYPE) == 0 || keyType.compareTo(JSONSTRING_KEY_TYPE) == 0) {
            return "" + 0;
        } else if(keyType.compareTo(JSONINT_KEY_TYPE) == 0) {
            return 0;
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(0);
            return bos.toByteArray();
        }
    }

    public String findKeyType(StoreDefinition storeDefinition) throws Exception {
        SerializerDefinition serializerDefinition = storeDefinition.getKeySerializer();
        if(serializerDefinition != null) {
            if("string".equals(serializerDefinition.getName())) {
                return Benchmark.STRING_KEY_TYPE;
            } else if("json".equals(serializerDefinition.getName())) {
                if(serializerDefinition.getCurrentSchemaInfo().contains("int")) {
                    return Benchmark.JSONINT_KEY_TYPE;
                } else if(serializerDefinition.getCurrentSchemaInfo().contains("string")) {
                    return Benchmark.JSONSTRING_KEY_TYPE;
                }
            } else if("identity".equals(serializerDefinition.getName())) {
                return Benchmark.IDENTITY_KEY_TYPE;
            }
        }

        throw new Exception("Can't determine key type for key serializer "
                            + storeDefinition.getName());
    }

    public Serializer<?> findKeyType(String keyType) throws Exception {
        if(keyType.compareTo(STRING_KEY_TYPE) == 0) {
            return new StringSerializer();
        } else if(keyType.compareTo(JSONINT_KEY_TYPE) == 0) {
            return new JsonTypeSerializer("\"int32\"");
        } else if(keyType.compareTo(JSONSTRING_KEY_TYPE) == 0) {
            return new JsonTypeSerializer("\"string\"");
        } else if(keyType.compareTo(IDENTITY_KEY_TYPE) == 0) {
            return new IdentitySerializer();
        }
        throw new Exception("Can't determine for keyType = " + keyType);
    }

    public void initializeWorkload(Props workloadProps) throws Exception {

        if(!this.storeInitialized) {
            throw new VoldemortException("Store not initialized correctly");
        }

        // Calculate perThreadThroughputPerMs = default unlimited (-1)
        this.targetThroughput = workloadProps.getInt(TARGET_THROUGHPUT, -1);
        this.perThreadThroughputPerMs = -1;
        if(targetThroughput > 0) {
            double targetPerThread = ((double) targetThroughput) / ((double) numThreads);
            this.perThreadThroughputPerMs = targetPerThread / 1000.0;
        }

        if(workloadProps.containsKey(OPS_COUNT)) {
            this.opsCount = workloadProps.getInt(OPS_COUNT);
        } else {
            throw new VoldemortException("Missing compulsory parameters - " + OPS_COUNT);
        }
        this.recordCount = workloadProps.getInt(RECORD_COUNT, -1);

        // Initialize measurement
        Metrics.setProperties(workloadProps);
        Metrics.getInstance().reset();

        // Initialize workload
        this.workLoad = new Workload();
        this.workLoad.init(workloadProps);

    }

    @SuppressWarnings("unchecked")
    public void initializeStore(Props benchmarkProps) throws Exception {

        this.numThreads = benchmarkProps.getInt(THREADS, MAX_WORKERS);
        this.numIterations = benchmarkProps.getInt(ITERATIONS, 1);
        this.statusIntervalSec = benchmarkProps.getInt(INTERVAL, 0);
        this.verbose = benchmarkProps.getBoolean(VERBOSE, false);
        this.verifyRead = benchmarkProps.getBoolean(VERIFY, false);
        this.ignoreNulls = benchmarkProps.getBoolean(IGNORE_NULLS, false);

        if(benchmarkProps.containsKey(URL)) {

            // Remote benchmark
            if(!benchmarkProps.containsKey(STORE_NAME)) {
                throw new VoldemortException("Missing storename");
            }

            String socketUrl = benchmarkProps.getString(URL);
            String storeName = benchmarkProps.getString(STORE_NAME);

            ClientConfig clientConfig = new ClientConfig().setMaxThreads(numThreads)
                                                          .setMaxTotalConnections(numThreads)
                                                          .setMaxConnectionsPerNode(numThreads)
                                                          .setBootstrapUrls(socketUrl)
                                                          .setConnectionTimeout(60,
                                                                                TimeUnit.SECONDS)
                                                          .setSocketTimeout(60, TimeUnit.SECONDS)
                                                          .setFailureDetectorRequestLengthThreshold(TimeUnit.SECONDS.toMillis(60))
                                                          .setSocketBufferSize(4 * 1024);

            SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
            this.storeClient = socketFactory.getStoreClient(storeName);
            StoreDefinition storeDef = getStoreDefinition(socketFactory, storeName);
            this.keyType = findKeyType(storeDef);
            benchmarkProps.put(Benchmark.KEY_TYPE, this.keyType);
            this.factory = socketFactory;

            // Send the store a value and then delete it
            if(benchmarkProps.getBoolean(HANDSHAKE, false)) {
                final Object key = getTempKey(this.keyType);
                this.storeClient.delete(key);
                this.storeClient.put(key, "123");
                this.storeClient.delete(key);
            }

        } else {

            // Local benchmark
            String storageEngineType = benchmarkProps.getString(STORAGE_ENGINE_TYPE);
            this.keyType = benchmarkProps.getString(KEY_TYPE, STRING_KEY_TYPE);
            Serializer serializer = findKeyType(this.keyType);
            Store<Object, Object> store = null;
            StorageConfiguration conf = null;

            if(storageEngineType.compareTo(BdbStorageConfiguration.TYPE_NAME) == 0) {
                conf = new BdbStorageConfiguration(ServerTestUtils.getVoldemortConfig());
            } else if(storageEngineType.compareTo(MysqlStorageConfiguration.TYPE_NAME) == 0) {
                conf = new MysqlStorageConfiguration(ServerTestUtils.getVoldemortConfig());
            } else if(storageEngineType.compareTo(InMemoryStorageConfiguration.TYPE_NAME) == 0) {
                conf = new InMemoryStorageConfiguration(ServerTestUtils.getVoldemortConfig());
            }
            store = SerializingStore.wrap(conf.getStore(DUMMY_DB),
                                          serializer,
                                          new StringSerializer());

            this.factory = new StaticStoreClientFactory(store);
            this.storeClient = factory.getStoreClient(store.getName());
        }

        this.storeInitialized = true;
    }

    public void initialize(Props benchmarkProps) throws Exception {
        if(!this.storeInitialized) {
            initializeStore(benchmarkProps);
        }
        initializeWorkload(benchmarkProps);
    }

    public void warmUpAndRun() throws Exception {
        if(this.recordCount > 0) {
            runTests(false);
            Metrics.getInstance().reset();
        }

        for(int index = 0; index < this.numIterations; index++) {
            System.out.println("======================= iteration = " + index
                               + " ======================================");
            runTests(true);
            Metrics.getInstance().reset();
        }

    }

    @SuppressWarnings("cast")
    public long runTests(boolean runBenchmark) throws Exception {

        int localOpsCounts = 0;
        String label = null;
        if(runBenchmark) {
            localOpsCounts = this.opsCount;
            label = new String("benchmark");
        } else {
            localOpsCounts = this.recordCount;
            label = new String("warmup");
        }
        Vector<Thread> threads = new Vector<Thread>();

        for(int index = 0; index < this.numThreads; index++) {
            VoldemortWrapper db = new VoldemortWrapper(storeClient,
                                                       this.verifyRead,
                                                       this.ignoreNulls);
            Thread clientThread = new ClientThread(db,
                                                   runBenchmark,
                                                   this.workLoad,
                                                   localOpsCounts / this.numThreads,
                                                   this.perThreadThroughputPerMs,
                                                   this.verbose);
            threads.add(clientThread);
        }

        long startRunBenchmark = System.currentTimeMillis();
        for(Thread currentThread: threads) {
            currentThread.start();
        }

        StatusThread statusThread = null;
        if(this.statusIntervalSec > 0) {
            statusThread = new StatusThread(threads, this.statusIntervalSec, startRunBenchmark);
            statusThread.start();
        }

        for(Thread currentThread: threads) {
            try {
                currentThread.join();
            } catch(InterruptedException e) {
                if(this.verbose)
                    e.printStackTrace();
            }
        }
        long endRunBenchmark = System.currentTimeMillis();

        if(this.statusIntervalSec > 0) {
            statusThread.interrupt();
        }

        System.out.println("[" + label + "]\tRunTime(ms): " + (endRunBenchmark - startRunBenchmark));
        double throughput = Time.MS_PER_SECOND * ((double) localOpsCounts)
                            / ((double) (endRunBenchmark - startRunBenchmark));
        System.out.println("[" + label + "]\tThroughput(ops/sec): " + throughput);

        if(runBenchmark) {
            Metrics.getInstance().printReport(System.out);
        }
        return (endRunBenchmark - startRunBenchmark);
    }

    public static void main(String args[]) throws IOException {
        // Logger.getRootLogger().removeAllAppenders();
        OptionParser parser = new OptionParser();
        parser.accepts(READS, "execute read operations").withRequiredArg().ofType(Integer.class);
        parser.accepts(WRITES, "execute write operations").withRequiredArg().ofType(Integer.class);
        parser.accepts(DELETES, "execute delete operations")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(MIXED, "generate a mix of read and write requests")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(VERBOSE, "verbose");
        parser.accepts(THREADS, "max number concurrent worker threads; Default = " + MAX_WORKERS)
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(ITERATIONS, "number of times to repeat the test; Default = 1")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(VERIFY, "verify values read");
        parser.accepts(HANDSHAKE, "perform a handshake");
        parser.accepts(PERCENT_CACHED,
                       "percentage of requests to come from previously requested keys; valid values are in range [0..100]; 0 means caching disabled. Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(START_KEY_INDEX, "starting point when using int keys; Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(INTERVAL, "print status at interval seconds; Default = 0")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(IGNORE_NULLS, "ignore null values");

        parser.accepts(PROP_FILE, "file containing all the properties").withRequiredArg();
        parser.accepts(STORAGE_ENGINE_TYPE, "file containing all the properties; Default = memory")
              .withRequiredArg();
        parser.accepts(KEY_TYPE, "which key type to support; Default = string").withRequiredArg();
        parser.accepts(REQUEST_FILE,
                       "execute specific requests in order; Overrides " + RECORD_SELECTION)
              .withRequiredArg();
        parser.accepts(VALUE_SIZE, "size in bytes for random value; Default = 1024")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(RECORD_SELECTION,
                       "how to select record [zipfian | latest | uniform]; Default = uniform")
              .withRequiredArg();
        parser.accepts(TARGET_THROUGHPUT, "fix the throughput")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(RECORD_COUNT, "number of records inserted during warmup phase")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(OPS_COUNT, "number of operations to do")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(URL, "url on which to run remote tests").withRequiredArg();
        parser.accepts(STORE_NAME, "store name on the remote URL").withRequiredArg();
        parser.accepts(METRIC_TYPE, "type of metric [histogram | summary]").withRequiredArg();
        parser.accepts(HELP);

        OptionSet options = parser.parse(args);

        if(options.has(HELP)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Props mainProps = null;
        if(options.has(PROP_FILE)) {
            String propFileDestination = (String) options.valueOf(PROP_FILE);
            File propertyFile = new File(propFileDestination);
            if(!propertyFile.exists()) {
                printUsage(parser, "Property file does not exist");
            }

            try {
                mainProps = new Props(propertyFile);
            } catch(Exception e) {
                printUsage(parser, "Unable to parse the property file");
            }
        } else {
            mainProps = new Props();

            if(options.has(REQUEST_FILE)) {
                mainProps.put(REQUEST_FILE, (String) options.valueOf(REQUEST_FILE));
                mainProps.put(RECORD_SELECTION, FILE_RECORD_SELECTION);
            } else {
                mainProps.put(RECORD_SELECTION, CmdUtils.valueOf(options,
                                                                 RECORD_SELECTION,
                                                                 UNIFORM_RECORD_SELECTION));

                if(options.has(RECORD_COUNT)) {
                    mainProps.put(RECORD_COUNT, (Integer) options.valueOf(RECORD_COUNT));
                } else {
                    mainProps.put(RECORD_COUNT, 0);
                }
            }

            if(!options.has(OPS_COUNT)) {
                printUsage(parser, "Missing " + OPS_COUNT);
            }
            mainProps.put(OPS_COUNT, (Integer) options.valueOf(OPS_COUNT));

            if(options.has(URL)) {
                mainProps.put(URL, (String) options.valueOf(URL));
                mainProps.put(HANDSHAKE, getCmdBoolean(options, HANDSHAKE));
                if(options.has(STORE_NAME)) {
                    mainProps.put(STORE_NAME, (String) options.valueOf(STORE_NAME));
                } else {
                    printUsage(parser, "Missing store name");
                }
            } else {
                mainProps.put(KEY_TYPE, CmdUtils.valueOf(options, KEY_TYPE, STRING_KEY_TYPE));
                mainProps.put(STORAGE_ENGINE_TYPE,
                              CmdUtils.valueOf(options,
                                               STORAGE_ENGINE_TYPE,
                                               InMemoryStorageConfiguration.TYPE_NAME));
            }

            mainProps.put(VERBOSE, getCmdBoolean(options, VERBOSE));
            mainProps.put(VERIFY, getCmdBoolean(options, VERIFY));
            mainProps.put(IGNORE_NULLS, getCmdBoolean(options, IGNORE_NULLS));
            mainProps.put(START_KEY_INDEX, CmdUtils.valueOf(options, START_KEY_INDEX, 0));
            mainProps.put(VALUE_SIZE, CmdUtils.valueOf(options, VALUE_SIZE, 1024));
            mainProps.put(ITERATIONS, CmdUtils.valueOf(options, ITERATIONS, 1));
            mainProps.put(THREADS, CmdUtils.valueOf(options, THREADS, MAX_WORKERS));
            mainProps.put(PERCENT_CACHED, CmdUtils.valueOf(options, PERCENT_CACHED, 0));
            mainProps.put(INTERVAL, CmdUtils.valueOf(options, INTERVAL, 0));
            mainProps.put(TARGET_THROUGHPUT, CmdUtils.valueOf(options, TARGET_THROUGHPUT, -1));
            mainProps.put(METRIC_TYPE, CmdUtils.valueOf(options, METRIC_TYPE, SUMMARY_METRIC_TYPE));
            mainProps.put(READS, CmdUtils.valueOf(options, READS, 0));
            mainProps.put(WRITES, CmdUtils.valueOf(options, WRITES, 0));
            mainProps.put(DELETES, CmdUtils.valueOf(options, DELETES, 0));
            mainProps.put(MIXED, CmdUtils.valueOf(options, MIXED, 0));
        }

        // Start the benchmark
        Benchmark benchmark = null;
        try {
            benchmark = new Benchmark();
            benchmark.initialize(mainProps);
            benchmark.warmUpAndRun();
            benchmark.close();
        } catch(Exception e) {
            if(options.has(VERBOSE)) {
                e.printStackTrace();
            }
            parser.printHelpOn(System.err);
            System.exit(-1);
        }
    }

    public void close() {
        this.factory.close();
    }

    private static void printUsage(OptionParser parser, String errorCommand) throws IOException {
        parser.printHelpOn(System.err);
        Utils.croak("Usage: $VOLDEMORT_HOME/bin/run-class.sh " + Benchmark.class.getName()
                    + " [options]\n " + errorCommand);
    }

    private static String getCmdBoolean(OptionSet option, String command) {
        if(option.has(command)) {
            return "true";
        } else {
            return "false";
        }
    }
}

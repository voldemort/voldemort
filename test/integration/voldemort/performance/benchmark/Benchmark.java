/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.NumberFormat;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.ServerTestUtils;
import voldemort.StaticStoreClientFactory;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.AbstractStoreClientFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.StringSerializer;
import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.store.views.ViewStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.xml.StoreDefinitionsMapper;

public class Benchmark {

    private static final int MAX_WORKERS = 8;
    private static final int MAX_CONNECTIONS_PER_NODE = 50;

    /**
     * Constants for the benchmark file
     */
    public static final String PROP_FILE = "prop-file";
    public static final String THREADS = "threads";
    public static final String NUM_CONNECTIONS_PER_NODE = "num-connections-per-node";
    public static final String ITERATIONS = "iterations";
    public static final String STORAGE_CONFIGURATION_CLASS = "storage-configuration-class";
    public static final String INTERVAL = "interval";

    public static final String KEY_TYPE = "keyType";
    public static final String STRING_KEY_TYPE = "string";
    public static final String JSONINT_KEY_TYPE = "json-int";
    public static final String JSONSTRING_KEY_TYPE = "json-string";
    public static final String IDENTITY_KEY_TYPE = "identity";

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
    public static final String PLUGIN_CLASS = "plugin-class";
    public static final String OPS_COUNT = "ops-count";

    public static final String METRIC_TYPE = "metric-type";
    public static final String HISTOGRAM_METRIC_TYPE = "histogram";
    public static final String SUMMARY_METRIC_TYPE = "summary";

    public static final String VERBOSE = "v";
    public static final String VERIFY = "verify";
    public static final String CLIENT_ZONE_ID = "client-zoneid";
    private static final String DUMMY_DB = "benchmark_db";
    public static final String STORE_TYPE = "view";
    public static final String VIEW_CLASS = "voldemort.store.views.UpperCaseView";
    public static final String HAS_TRANSFORMS = "true";
    public static final String SAMPLE_SIZE = "sample-size";

    public static final String ROUTING_TIMEOUT = "routing-timeout";
    public static final String SOCKET_TIMEOUT = "socket-timeout";

    private StoreClient<Object, Object> storeClient;
    private StoreClientFactory factory;

    private int numThreads, numConnectionsPerNode, numIterations, targetThroughput, recordCount,
            opsCount, statusIntervalSec;
    private double perThreadThroughputPerMs;
    private Workload workLoad;
    private String pluginName;
    private boolean storeInitialized = false;
    private boolean warmUpCompleted = false;
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
                    Metrics.getInstance().printReport(System.out);
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
        private boolean isVerbose;
        private Workload clientWorkLoad;
        private int operationsCount;
        private double targetThroughputPerMs;
        private int opsDone;
        private final WorkloadPlugin plugin;

        public ClientThread(VoldemortWrapper db,
                            boolean runBenchmark,
                            Workload workLoad,
                            int operationsCount,
                            double targetThroughputPerMs,
                            boolean isVerbose,
                            WorkloadPlugin plugin) {
            this.db = db;
            this.runBenchmark = runBenchmark;
            this.clientWorkLoad = workLoad;
            this.operationsCount = operationsCount;
            this.opsDone = 0;
            this.targetThroughputPerMs = targetThroughputPerMs;
            this.isVerbose = isVerbose;
            this.plugin = plugin;
        }

        public int getOpsDone() {
            return this.opsDone;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            while(opsDone < this.operationsCount) {
                try {
                    if(runBenchmark) {
                        if(!clientWorkLoad.doTransaction(this.db, plugin)) {
                            break;
                        }
                    } else {
                        if(!clientWorkLoad.doWrite(this.db, plugin)) {
                            break;
                        }
                    }
                } catch(Exception e) {
                    if(this.isVerbose)
                        e.printStackTrace();
                }
                opsDone++;

                if(targetThroughputPerMs > 0) {
                    double timePerOp = ((double) opsDone) / targetThroughputPerMs;
                    while(System.currentTimeMillis() - startTime < timePerOp) {
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
        this.pluginName = workloadProps.getString(PLUGIN_CLASS, null);

        // Initialize measurement
        Metrics.setProperties(workloadProps);
        Metrics.getInstance().reset();

        // Initialize workload
        this.workLoad = new Workload();
        this.workLoad.init(workloadProps);
        this.workLoad.loadSampleValues(storeClient);
    }

    @SuppressWarnings("unchecked")
    public void initializeStore(Props benchmarkProps) throws Exception {

        this.numThreads = benchmarkProps.getInt(THREADS, MAX_WORKERS);
        this.numConnectionsPerNode = benchmarkProps.getInt(NUM_CONNECTIONS_PER_NODE,
                                                           MAX_CONNECTIONS_PER_NODE);
        this.numIterations = benchmarkProps.getInt(ITERATIONS, 1);
        this.statusIntervalSec = benchmarkProps.getInt(INTERVAL, 0);
        this.verbose = benchmarkProps.getBoolean(VERBOSE, false);
        this.verifyRead = benchmarkProps.getBoolean(VERIFY, false);
        this.ignoreNulls = benchmarkProps.getBoolean(IGNORE_NULLS, false);
        int clientZoneId = benchmarkProps.getInt(CLIENT_ZONE_ID, -1);

        int routingTimeout = benchmarkProps.getInt(ROUTING_TIMEOUT, 1500);
        int socketTimeout = benchmarkProps.getInt(SOCKET_TIMEOUT, 1500);

        System.err.println("Using timeouts : " + routingTimeout + " and " + socketTimeout);

        if(benchmarkProps.containsKey(URL)) {

            // Remote benchmark
            if(!benchmarkProps.containsKey(STORE_NAME)) {
                throw new VoldemortException("Missing storename");
            }

            String socketUrl = benchmarkProps.getString(URL);
            String storeName = benchmarkProps.getString(STORE_NAME);

            ClientConfig clientConfig = new ClientConfig().setMaxThreads(numThreads)
                                                          .setMaxTotalConnections(numThreads)
                                                          .setMaxConnectionsPerNode(numConnectionsPerNode)
                                                          .setRoutingTimeout(routingTimeout,
                                                                             TimeUnit.MILLISECONDS)
                                                          .setSocketTimeout(socketTimeout,
                                                                            TimeUnit.MILLISECONDS)
                                                          .setConnectionTimeout(500,
                                                                                TimeUnit.MILLISECONDS)
                                                          .setRequestFormatType(RequestFormatType.VOLDEMORT_V3)
                                                          .setBootstrapUrls(socketUrl);
            // .enableDefaultClient(true);

            if(clientZoneId >= 0) {
                clientConfig.setClientZoneId(clientZoneId);
            }
            SocketStoreClientFactory socketFactory = new SocketStoreClientFactory(clientConfig);
            this.storeClient = socketFactory.getStoreClient(storeName);
            StoreDefinition storeDef = getStoreDefinition(socketFactory, storeName);
            this.keyType = findKeyType(storeDef);
            benchmarkProps.put(Benchmark.KEY_TYPE, this.keyType);
            this.factory = socketFactory;

        } else {

            // Local benchmark
            String storageEngineClass = benchmarkProps.getString(STORAGE_CONFIGURATION_CLASS);
            this.keyType = benchmarkProps.getString(KEY_TYPE, STRING_KEY_TYPE);
            Serializer serializer = findKeyType(this.keyType);
            Store<Object, Object, Object> store = null;

            StorageConfiguration conf = (StorageConfiguration) ReflectUtils.callConstructor(ReflectUtils.loadClass(storageEngineClass),
                                                                                            new Object[] { ServerTestUtils.getVoldemortConfig() });

            StorageEngine<ByteArray, byte[], byte[]> engine = conf.getStore(TestUtils.makeStoreDefinition(DUMMY_DB),
                                                                            TestUtils.makeSingleNodeRoutingStrategy());
            if(conf.getType().compareTo(ViewStorageConfiguration.TYPE_NAME) == 0) {
                engine = new ViewStorageEngine(STORE_NAME,
                                               engine,
                                               new StringSerializer(),
                                               new StringSerializer(),
                                               serializer,
                                               new StringSerializer(),
                                               null,
                                               BenchmarkViews.loadTransformation(benchmarkProps.getString(VIEW_CLASS)
                                                                                               .trim()));
            }
            store = SerializingStore.wrap(engine,
                                          serializer,
                                          new StringSerializer(),
                                          new IdentitySerializer());

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
            System.out.println("Running warmup");
            runTests(false);
            this.warmUpCompleted = true;
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
                                                       this.verifyRead && this.warmUpCompleted,
                                                       this.ignoreNulls);
            WorkloadPlugin plugin = null;
            if(this.pluginName != null && this.pluginName.length() > 0) {
                Class<?> cls = Class.forName(this.pluginName);
                try {
                    plugin = (WorkloadPlugin) cls.newInstance();
                } catch(IllegalAccessException e) {
                    System.err.println("Class not accessible ");
                    System.exit(1);
                } catch(InstantiationException e) {
                    System.err.println("Class not instantiable.");
                    System.exit(1);
                }
                plugin.setDb(db);
            }

            threads.add(new ClientThread(db,
                                         runBenchmark,
                                         this.workLoad,
                                         localOpsCounts / this.numThreads,
                                         this.perThreadThroughputPerMs,
                                         this.verbose,
                                         plugin));
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

        // Print the output
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(4);
        nf.setGroupingUsed(false);

        System.out.println("[" + label + "]\tRunTime(ms): "
                           + nf.format((endRunBenchmark - startRunBenchmark)));
        double throughput = Time.MS_PER_SECOND * ((double) localOpsCounts)
                            / ((double) (endRunBenchmark - startRunBenchmark));
        System.out.println("[" + label + "]\tThroughput(ops/sec): " + nf.format(throughput));

        if(runBenchmark) {
            Metrics.getInstance().printReport(System.out);
        }
        return (endRunBenchmark - startRunBenchmark);
    }

    public static void main(String args[]) throws IOException {
        // Logger.getRootLogger().removeAllAppenders();
        OptionParser parser = new OptionParser();
        parser.accepts(READS, "percentage of --ops-count to be reads; valid values [0-100]")
              .withRequiredArg()
              .describedAs("read-percent")
              .ofType(Integer.class);
        parser.accepts(WRITES, "percentage of --ops-count to be writes; valid values [0-100]")
              .withRequiredArg()
              .describedAs("write-percent")
              .ofType(Integer.class);
        parser.accepts(DELETES, "percentage of --ops-count to be deletes; valid values [0-100]")
              .withRequiredArg()
              .describedAs("delete-percent")
              .ofType(Integer.class);
        parser.accepts(MIXED, "percentage of --ops-count to be updates; valid values [0-100]")
              .withRequiredArg()
              .describedAs("update-percent")
              .ofType(Integer.class);
        parser.accepts(SAMPLE_SIZE,
                       "number of value samples to be obtained from the store for replay based on keys from request-file; 0 means no sample value replay. Default = 0")
              .withRequiredArg()
              .describedAs("sample-size")
              .ofType(Integer.class);
        parser.accepts(VERBOSE, "verbose");
        parser.accepts(THREADS, "max number concurrent worker threads; Default = " + MAX_WORKERS)
              .withRequiredArg()
              .describedAs("num-threads")
              .ofType(Integer.class);
        parser.accepts(NUM_CONNECTIONS_PER_NODE,
                       "max number of connections to any node; Default = "
                               + MAX_CONNECTIONS_PER_NODE)
              .withRequiredArg()
              .describedAs("num-connections-per-node")
              .ofType(Integer.class);
        parser.accepts(ITERATIONS, "number of times to repeat benchmark phase; Default = 1")
              .withRequiredArg()
              .describedAs("num-iter")
              .ofType(Integer.class);
        parser.accepts(VERIFY, "verify values read; runs only if warm-up phase is included");
        parser.accepts(PERCENT_CACHED,
                       "percentage of requests to come from previously requested keys; valid values are in range [0..100]; 0 means caching disabled. Default = 0")
              .withRequiredArg()
              .describedAs("percent")
              .ofType(Integer.class);
        parser.accepts(START_KEY_INDEX, "key index to start warm-up phase from; Default = 0")
              .withRequiredArg()
              .describedAs("index")
              .ofType(Integer.class);
        parser.accepts(INTERVAL, "print status at interval seconds; Default = 0")
              .withRequiredArg()
              .describedAs("sec")
              .ofType(Integer.class);
        parser.accepts(IGNORE_NULLS, "ignore null values in results");
        parser.accepts(PROP_FILE,
                       "file containing all the properties in key=value format; will override all other command line options specified")
              .withRequiredArg()
              .describedAs("prop-file");
        parser.accepts(STORAGE_CONFIGURATION_CLASS,
                       "class of the storage engine configuration to use [e.g. voldemort.store.bdb.BdbStorageConfiguration]")
              .withRequiredArg()
              .describedAs("class-name");
        parser.accepts(KEY_TYPE,
                       "for local tests; key type to support; [ " + IDENTITY_KEY_TYPE + " | "
                               + JSONINT_KEY_TYPE + " | " + JSONSTRING_KEY_TYPE + "|"
                               + STRING_KEY_TYPE + " <default> ]")
              .withRequiredArg()
              .describedAs("type");
        parser.accepts(REQUEST_FILE,
                       "file with limited list of keys to be used during benchmark phase; Overrides "
                               + RECORD_SELECTION).withRequiredArg();
        parser.accepts(VALUE_SIZE,
                       "size in bytes for random value; used during warm-up phase and write operation of benchmark phase; Default = 1024")
              .withRequiredArg()
              .describedAs("bytes")
              .ofType(Integer.class);
        parser.accepts(RECORD_SELECTION,
                       "record selection distribution [ " + ZIPFIAN_RECORD_SELECTION + " | "
                               + LATEST_RECORD_SELECTION + " | " + UNIFORM_RECORD_SELECTION
                               + " <default> ]").withRequiredArg();
        parser.accepts(TARGET_THROUGHPUT, "fix throughput")
              .withRequiredArg()
              .describedAs("ops/sec")
              .ofType(Integer.class);
        parser.accepts(RECORD_COUNT, "number of records inserted during warmup phase")
              .withRequiredArg()
              .describedAs("count")
              .ofType(Integer.class);
        parser.accepts(OPS_COUNT, "number of operations to do during benchmark phase")
              .withRequiredArg()
              .describedAs("count")
              .ofType(Integer.class);
        parser.accepts(URL, "for remote tests; url of remote server").withRequiredArg();
        parser.accepts(STORE_NAME, "for remote tests; store name on the remote " + URL)
              .withRequiredArg()
              .describedAs("name");
        parser.accepts(METRIC_TYPE,
                       "type of result metric [ " + HISTOGRAM_METRIC_TYPE + " | "
                               + SUMMARY_METRIC_TYPE + " <default> ]").withRequiredArg();
        parser.accepts(PLUGIN_CLASS,
                       "classname of implementation of WorkloadPlugin; used to run customized operations ")
              .withRequiredArg()
              .describedAs("class-name");
        parser.accepts(CLIENT_ZONE_ID, "zone id for client; enables zone routing")
              .withRequiredArg()
              .describedAs("zone-id")
              .ofType(Integer.class);
        parser.accepts(HELP);
        parser.accepts(ROUTING_TIMEOUT, "Routing timeout")
              .withRequiredArg()
              .describedAs("routing-timeout")
              .ofType(Integer.class);
        parser.accepts(SOCKET_TIMEOUT, "Socket timeout")
              .withRequiredArg()
              .describedAs("socket-timeout")
              .ofType(Integer.class);

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
                mainProps.put(RECORD_SELECTION,
                              CmdUtils.valueOf(options, RECORD_SELECTION, UNIFORM_RECORD_SELECTION));
            }

            if(options.has(RECORD_COUNT)) {
                mainProps.put(RECORD_COUNT, (Integer) options.valueOf(RECORD_COUNT));
            } else {
                mainProps.put(RECORD_COUNT, 0);
            }

            if(!options.has(OPS_COUNT)) {
                printUsage(parser, "Missing " + OPS_COUNT);
            }
            mainProps.put(OPS_COUNT, (Integer) options.valueOf(OPS_COUNT));

            if(options.has(URL)) {
                mainProps.put(URL, (String) options.valueOf(URL));
                if(options.has(STORE_NAME)) {
                    mainProps.put(STORE_NAME, (String) options.valueOf(STORE_NAME));
                } else {
                    printUsage(parser, "Missing store name");
                }
            } else {
                mainProps.put(KEY_TYPE, CmdUtils.valueOf(options, KEY_TYPE, STRING_KEY_TYPE));
                mainProps.put(STORAGE_CONFIGURATION_CLASS,
                              CmdUtils.valueOf(options,
                                               STORAGE_CONFIGURATION_CLASS,
                                               BdbStorageConfiguration.class.getName()));
            }

            mainProps.put(VERBOSE, getCmdBoolean(options, VERBOSE));
            mainProps.put(VERIFY, getCmdBoolean(options, VERIFY));
            mainProps.put(IGNORE_NULLS, getCmdBoolean(options, IGNORE_NULLS));
            mainProps.put(CLIENT_ZONE_ID, CmdUtils.valueOf(options, CLIENT_ZONE_ID, -1));
            mainProps.put(START_KEY_INDEX, CmdUtils.valueOf(options, START_KEY_INDEX, 0));
            mainProps.put(VALUE_SIZE, CmdUtils.valueOf(options, VALUE_SIZE, 1024));
            mainProps.put(ITERATIONS, CmdUtils.valueOf(options, ITERATIONS, 1));
            mainProps.put(THREADS, CmdUtils.valueOf(options, THREADS, MAX_WORKERS));
            mainProps.put(NUM_CONNECTIONS_PER_NODE, CmdUtils.valueOf(options,
                                                                     NUM_CONNECTIONS_PER_NODE,
                                                                     MAX_CONNECTIONS_PER_NODE));
            mainProps.put(PERCENT_CACHED, CmdUtils.valueOf(options, PERCENT_CACHED, 0));
            mainProps.put(INTERVAL, CmdUtils.valueOf(options, INTERVAL, 0));
            mainProps.put(TARGET_THROUGHPUT, CmdUtils.valueOf(options, TARGET_THROUGHPUT, -1));
            mainProps.put(METRIC_TYPE, CmdUtils.valueOf(options, METRIC_TYPE, SUMMARY_METRIC_TYPE));
            mainProps.put(READS, CmdUtils.valueOf(options, READS, 0));
            mainProps.put(WRITES, CmdUtils.valueOf(options, WRITES, 0));
            mainProps.put(DELETES, CmdUtils.valueOf(options, DELETES, 0));
            mainProps.put(MIXED, CmdUtils.valueOf(options, MIXED, 0));
            mainProps.put(PLUGIN_CLASS, CmdUtils.valueOf(options, PLUGIN_CLASS, ""));
            mainProps.put(SAMPLE_SIZE, CmdUtils.valueOf(options, SAMPLE_SIZE, 0));
            mainProps.put(ROUTING_TIMEOUT, CmdUtils.valueOf(options, ROUTING_TIMEOUT, 1500));
            mainProps.put(SOCKET_TIMEOUT, CmdUtils.valueOf(options, SOCKET_TIMEOUT, 1500));
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

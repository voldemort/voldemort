package voldemort.grandfather;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Pair;
import voldemort.utils.ReflectUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;

/**
 * Single node grand-fathering tool
 * 
 */
@SuppressWarnings("deprecation")
public class RWStoreBuilder {

    private static final Logger logger = Logger.getLogger(RWStoreBuilder.class);

    private HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeIdToRequest;
    private ExecutorService producers, consumers;
    private Cluster cluster;
    private StoreDefinition storeDef;
    private Class<? extends RWStoreParser> parserClass;
    private int vectorNodeId;
    private long vectorNodeVersion, jobStartTime;
    private HashSet<Future> producerResults, consumerResults;
    private Pair<ByteArray, Versioned<byte[]>> END = new Pair<ByteArray, Versioned<byte[]>>(null,
                                                                                            null);
    private AdminClient adminClient;
    private final Semaphore semaphore;
    private FileSystem fs;
    private FileStatus[] files;
    private final boolean verbose;
    private final RWStoreParser parser;

    public RWStoreBuilder(Class<? extends RWStoreParser> parserClass,
                          Path inputPath,
                          Cluster cluster,
                          final StoreDefinition storeDef,
                          int numThreads,
                          int vectorNodeId,
                          long vectorNodeVersion,
                          boolean verbose) throws IOException, URISyntaxException {
        this.producers = Executors.newFixedThreadPool(numThreads);
        this.consumers = Executors.newFixedThreadPool(cluster.getNumberOfNodes());
        this.cluster = cluster;
        this.storeDef = storeDef;
        this.parserClass = parserClass;
        this.vectorNodeId = vectorNodeId;
        this.vectorNodeVersion = vectorNodeVersion;
        this.jobStartTime = System.currentTimeMillis();
        this.producerResults = new HashSet<Future>();
        this.consumerResults = new HashSet<Future>();
        this.verbose = verbose;
        this.nodeIdToRequest = new HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>>();
        for(Node node: cluster.getNodes()) {
            this.nodeIdToRequest.put(node.getId(),
                                     new SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>());
        }
        logger.info("Starting admin client");
        this.adminClient = new AdminClient(cluster,
                                           new AdminClientConfig().setMaxThreads(cluster.getNumberOfNodes())
                                                                  .setMaxConnectionsPerNode(1));

        this.fs = new DistributedFileSystem();
        this.fs.initialize(new URI(inputPath.toString()), new Configuration());
        this.files = fs.listStatus(inputPath);
        this.semaphore = new Semaphore(-1 * files.length + 1, false);

        parser = (RWStoreParser) ReflectUtils.callConstructor(parserClass);
        logger.info("configuring parser");
        parser.configure(storeDef,
                         cluster,
                         vectorNodeId,
                         vectorNodeVersion,
                         jobStartTime,
                         nodeIdToRequest,
                         verbose);
    }

    public <K, V> void producer() {

        for(final FileStatus file: files) {

            this.producerResults.add(producers.submit(new Runnable() {

                public void run() {
                    int numProduced = 0;
                    int delta = verbose ? 1 : 10000;
                    try {
                        logger.info("Producer running " + file.getPath());
                        SequenceFile.Reader reader = null;
                        try {
                            reader = new SequenceFile.Reader(fs,
                                                             file.getPath(),
                                                             new Configuration());

                        } catch(Throwable t) {
                            logger.error("Unable to open sequence file ", t);
                            throw new RuntimeException(t);
                        }

                        try {
                            logger.info("calling parser constructor");
                            /*
                             * RWStoreParser parser = (RWStoreParser)
                             * ReflectUtils.callConstructor(parserClass);
                             * logger.info("configuring parser");
                             * parser.configure(storeDef, cluster, vectorNodeId,
                             * vectorNodeVersion, jobStartTime,
                             * nodeIdToRequest);
                             */
                            logger.info("parser configured");

                            boolean readSomething;
                            do {
                                BytesWritable key = new BytesWritable();
                                BytesWritable value = new BytesWritable();
                                if(readSomething = reader.next(key, value)) {
                                    parser.nextKeyValue(key, value);
                                    if(++numProduced % delta == 0) {
                                        logger.info("Producer produced: " + numProduced);
                                    }
                                }

                            } while(readSomething);

                            reader.close();
                            logger.info("Producer DONE");
                        } catch(Throwable t) {
                            logger.error(t);
                            t.printStackTrace();
                            throw new RuntimeException("producer failed", t);
                        }
                    } finally {
                        semaphore.release();
                        logger.info("Producer releasing permit produced [" + numProduced + "]");
                    }
                }
            }));
        }

    }

    public void consumer() {

        for(final Node node: cluster.getNodes()) {

            this.consumerResults.add(consumers.submit(new Callable<Boolean>() {

                public Boolean call() {
                    final String consumerId = "Consumer " + node.getId();
                    final AtomicInteger numConsumedByInnerClass = new AtomicInteger(0);
                    try {
                        logger.info("Running " + consumerId);

                        final SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> requestQueue = nodeIdToRequest.get(node.getId());
                        AbstractIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;
                        iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

                            public int numConsumed = 0;

                            @Override
                            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                                try {
                                    int delta = verbose ? 1 : 10000;
                                    Pair<ByteArray, Versioned<byte[]>> head = null;
                                    boolean shutDown = false;
                                    while(!shutDown) {
                                        head = requestQueue.poll();
                                        if(head == null) {
                                            // logger.info(consumerId +
                                            // " NULL ");
                                            continue;
                                        }
                                        if(head.equals(END)) {
                                            logger.info(consumerId + " END ");
                                            shutDown = true;
                                        } else {
                                            if(++numConsumed % delta == 0) {
                                                logger.info(consumerId + " consumed " + numConsumed
                                                            + " entries");
                                            }
                                            if(verbose) {
                                                logger.info(consumerId + " consumed: " + head);
                                            }
                                            return head;
                                        }
                                    }
                                    numConsumedByInnerClass.set(numConsumed);
                                    return endOfData();
                                } catch(Throwable t) {
                                    throw new RuntimeException("consumer failed inside iterator", t);
                                }
                            }
                        };

                        adminClient.updateEntries(node.getId(), storeDef.getName(), iterator, null);
                    } catch(Throwable t) {
                        logger.error("consumer failed while updating entries", t);
                        throw new RuntimeException("consumer failed while updating entries", t);
                    } finally {
                        logger.info(consumerId + " total consumption [" + numConsumedByInnerClass
                                    + "]");
                    }

                    logger.info("DONE WITH " + consumerId);
                    return true;
                }
            }));
        }
    }

    public void close() throws VoldemortException, InterruptedException {

        logger.info("Acquiring permit ");
        semaphore.acquire();
        logger.info("Got permit to put poison pill");

        for(SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>> requestQueue: nodeIdToRequest.values()) {
            try {
                requestQueue.put(END);
                logger.info("Inserting putting poison pill");
            } catch(InterruptedException e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                throw new VoldemortException("Interrupted Exception: " + sw.toString());
            }
        }

        for(Future result: consumerResults) {
            try {
                result.get();
            } catch(Exception e) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                throw new VoldemortException("Exception in consumer " + sw.toString());
            }
        }

        logger.info("Consumers completed");

        producers.shutdown();
        consumers.shutdown();
        adminClient.stop();
        logger.info("Admin client stopped");

    }

    public void build() throws VoldemortException, InterruptedException {
        producer();
        consumer();
        close();
    }

    private static void printUsage(OptionParser parser) throws IOException {
        System.err.println("Usage: bin/grandfather-readwrite.sh \\");
        System.err.println(" [genericOptions] [options]\n");
        System.err.println("Options:");
        parser.printHelpOn(System.err);
        System.err.println();
    }

    @SuppressWarnings("unchecked")
    public static void main(String args[]) throws IOException, VoldemortException,
            InterruptedException, URISyntaxException {

        OptionParser parser = new OptionParser();
        parser.accepts("input", "input file(s) on Hdfs").withRequiredArg();
        parser.accepts("mapper", "store builder class which maps file to K/V.").withRequiredArg();
        parser.accepts("cluster", "local path to cluster.xml.").withRequiredArg();
        parser.accepts("storedefinitions", "local path to stores.xml.").withRequiredArg();
        parser.accepts("storename", "store name from store definition.").withRequiredArg();
        parser.accepts("numthreads", "Number of concurrent files to read (default=10)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("vectornodeid", "node id whose vector clock to set (default=master)")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("vectorversion", "version of vector clock (default=1)")
              .withRequiredArg()
              .ofType(Long.class);
        parser.accepts("verbose", "be verbose in output");
        parser.accepts("help", "print usage information");

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            printUsage(parser);
            System.exit(0);
        }

        boolean verbose = false;
        if(options.has("verbose")) {
            verbose = true;
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "input",
                                               "mapper",
                                               "cluster",
                                               "storedefinitions",
                                               "storename");

        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing)
                               + "\n");
            printUsage(parser);
            System.exit(1);
        }

        File clusterFile = new File((String) options.valueOf("cluster"));
        Cluster cluster = new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile)));

        File storeDefFile = new File((String) options.valueOf("storedefinitions"));
        String storeName = (String) options.valueOf("storename");
        Path inputPath = new Path((String) options.valueOf("input"));

        List<StoreDefinition> stores;
        stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeDefFile)));
        StoreDefinition storeDef = null;
        for(StoreDefinition def: stores) {
            if(def.getName().equals(storeName))
                storeDef = def;
        }

        if(storeDef == null) {
            System.err.println("Missing store definition for store name '" + storeName + "'");
            printUsage(parser);
            System.exit(1);
        }

        int vectorNodeId;
        if(options.has("vectornodeid")) {
            vectorNodeId = (Integer) options.valueOf("vectornodeid");
        } else {
            vectorNodeId = -1; // To denote master
        }

        long vectorNodeVersion;
        if(options.has("vectorversion")) {
            vectorNodeVersion = (Long) options.valueOf("vectorversion");
        } else {
            vectorNodeVersion = 1L;
        }

        int numThreads;
        if(options.has("numthreads")) {
            numThreads = (Integer) options.valueOf("numthreads");
        } else {
            numThreads = 10;
        }

        String parserClassName = (String) options.valueOf("mapper");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class<? extends RWStoreParser> parserClass = (Class<? extends RWStoreParser>) ReflectUtils.loadClass(parserClassName,
                                                                                                             cl);

        if(parserClass == null) {
            System.err.println("Incorrect parser class name '" + parserClassName + "'");
            printUsage(parser);
            System.exit(1);
        }

        RWStoreBuilder builder = new RWStoreBuilder(parserClass,
                                                    inputPath,
                                                    cluster,
                                                    storeDef,
                                                    numThreads,
                                                    vectorNodeId,
                                                    vectorNodeVersion,
                                                    verbose);
        builder.build();

    }
}

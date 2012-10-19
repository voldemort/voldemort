package voldemort.performance;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.TestUtils;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.CmdUtils;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;

/**
 * An in-process performance test for voldemort storage engines
 * 
 * @author jay
 * 
 */
public class StorageEnginePerformanceTest {

    public static void main(String[] args) throws Exception {
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("help", "print usage information");
            parser.accepts("requests", "[REQUIRED] number of requests to execute")
                  .withRequiredArg()
                  .ofType(Integer.class);
            parser.accepts("num-values", "[REQUIRED] number of values in the store")
                  .withRequiredArg()
                  .ofType(Integer.class);
            parser.accepts("data-dir", "Data directory for storage data")
                  .withRequiredArg()
                  .describedAs("directory");
            parser.accepts("threads", "number of threads").withRequiredArg().ofType(Integer.class);
            parser.accepts("storage-configuration-class",
                           "[REQUIRED] class of the storage engine configuration to use [e.g. voldemort.store.bdb.BdbStorageConfiguration]")
                  .withRequiredArg()
                  .describedAs("class_name");
            parser.accepts("props", "Properties file with configuration for the engine")
                  .withRequiredArg()
                  .describedAs("config.properties");
            parser.accepts("value-size", "The size of the values in the store")
                  .withRequiredArg()
                  .describedAs("size")
                  .ofType(Integer.class);
            parser.accepts("cache-width", "Percentage of requests to save as possible re-requests")
                  .withRequiredArg()
                  .describedAs("width")
                  .ofType(Integer.class);
            parser.accepts("cache-hit-ratio",
                           "Percentage of requests coming from the last cache-width requests")
                  .withRequiredArg()
                  .describedAs("ratio")
                  .ofType(Double.class);
            parser.accepts("clean-up", "Delete data directory when done.");
            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }

            CmdUtils.croakIfMissing(parser, options, "requests");

            final int numThreads = CmdUtils.valueOf(options, "threads", 10);
            final int numRequests = (Integer) options.valueOf("requests");
            final int numValues = (Integer) options.valueOf("num-values");
            final int valueSize = CmdUtils.valueOf(options, "value-size", 1024);
            final int cacheWidth = CmdUtils.valueOf(options, "cache-width", 100000);
            final double cacheHitRatio = CmdUtils.valueOf(options, "cache-hit-ratio", 0.5);
            final String propsFile = (String) options.valueOf("props");
            final boolean cleanUp = options.has("clean-up");
            final String storageEngineClass = CmdUtils.valueOf(options,
                                                               "storage-configuration-class",
                                                               BdbStorageConfiguration.class.getName())
                                                      .trim();
            File dataDir = null;
            if(options.has("data-dir"))
                dataDir = new File((String) options.valueOf("data-dir"));
            else
                dataDir = TestUtils.createTempDir();
            System.out.println("Data dir: " + dataDir);

            // create the storage engine
            Props props = new Props();
            if(propsFile != null)
                props = new Props(new File(propsFile));
            props.put("node.id", 0);
            props.put("data.directory", dataDir.getAbsolutePath());
            props.put("voldemort.home", System.getProperty("user.dir"));
            VoldemortConfig config = new VoldemortConfig(props);
            StorageConfiguration storageConfig = (StorageConfiguration) ReflectUtils.callConstructor(ReflectUtils.loadClass(storageEngineClass),
                                                                                                     new Object[] { config });
            StorageEngine<ByteArray, byte[], byte[]> engine = storageConfig.getStore(TestUtils.makeStoreDefinition("test"));
            @SuppressWarnings("unchecked")
            final Store<String, byte[], byte[]> store = new SerializingStore(engine,
                                                                             new StringSerializer(),
                                                                             new IdentitySerializer(),
                                                                             null);

            final byte[] value = new byte[valueSize];
            new Random().nextBytes(value);

            // initialize test data
            for(int i = 0; i < numValues; i++)
                store.put(Integer.toString(i), Versioned.value(value), null);

            // initialize cache lookback data
            int[] recents = new int[cacheWidth];

            System.out.println("Write test:");
            CachedPerformanceTest writeTest = new CachedPerformanceTest(new PerformanceTest() {

                @Override
                public void doOperation(int index) {
                    try {
                        String key = Integer.toString(index);
                        List<Versioned<byte[]>> vs = store.get(key, null);
                        VectorClock version;
                        if(vs.size() == 0)
                            version = new VectorClock();
                        else
                            version = (VectorClock) vs.get(0).getVersion();
                        version.incrementVersion(0, 847584375);
                        store.put(key, Versioned.value(value, version), null);
                    } catch(ObsoleteVersionException e) {
                        // do nothing
                    } catch(RuntimeException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            }, recents, numValues, cacheHitRatio);
            writeTest.run(numRequests, numThreads);
            writeTest.printStats();
            System.out.println();

            System.out.println("Read test:");
            CachedPerformanceTest readTest = new CachedPerformanceTest(new PerformanceTest() {

                @Override
                public void doOperation(int index) {
                    store.get(Integer.toString(index), null);
                }
            }, recents, numValues, cacheHitRatio);
            readTest.run(numRequests, numThreads);
            readTest.printStats();

            if(cleanUp)
                Utils.rm(dataDir);

        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static class CachedPerformanceTest extends PerformanceTest {

        private RandomEngine random = new DRand(1);
        private PerformanceTest test;
        private int[] cache;
        private int size;
        private double cachePercent;
        private AtomicInteger uncached = new AtomicInteger(0);

        public CachedPerformanceTest(PerformanceTest test,
                                     int[] cache,
                                     int size,
                                     double cachePercent) {
            this.test = test;
            this.cache = cache;
            this.size = size;
            this.cachePercent = cachePercent;
        }

        @Override
        public void doOperation(int index) throws Exception {
            int req;
            if(random.nextFloat() < cachePercent && index > 1) {
                int maxValid = Math.min(Math.min(Math.max(cache.length, index - 1),
                                                 uncached.get() - 1), cache.length);
                int cacheIndex = Math.abs(random.nextInt()) % maxValid;
                req = cache[cacheIndex];
            } else {
                req = Math.abs(random.nextInt()) % size;
                cache[uncached.getAndIncrement() % cache.length] = req;
            }
            test.doOperation(req);
        }
    }
}

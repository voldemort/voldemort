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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.StoreClient;
import voldemort.performance.benchmark.generator.CounterGenerator;
import voldemort.performance.benchmark.generator.DiscreteGenerator;
import voldemort.performance.benchmark.generator.FileStringGenerator;
import voldemort.performance.benchmark.generator.Generator;
import voldemort.performance.benchmark.generator.IntegerGenerator;
import voldemort.performance.benchmark.generator.ScrambledZipfianGenerator;
import voldemort.performance.benchmark.generator.SkewedLatestGenerator;
import voldemort.performance.benchmark.generator.UniformIntegerGenerator;
import voldemort.utils.Props;
import voldemort.utils.UndefinedPropertyException;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class Workload {

    private static Logger logger = Logger.getLogger(Workload.class);

    public interface KeyProvider<T> {

        public T next();

        public T lastKey();

    }

    public abstract static class AbstractKeyProvider<T> implements KeyProvider<T> {

        protected Generator generator;

        private AbstractKeyProvider(Generator generator) {
            this.generator = generator;
        }

        @Override
        public abstract T next();

        @Override
        public abstract T lastKey();
    }

    public static class IntegerKeyProvider extends AbstractKeyProvider<Integer> {

        private IntegerKeyProvider(IntegerGenerator generator) {
            super(generator);
        }

        @Override
        public Integer next() {
            return ((IntegerGenerator) this.generator).nextInt();
        }

        @Override
        public Integer lastKey() {
            return ((IntegerGenerator) this.generator).lastInt();
        }
    }

    public static class StringKeyProvider extends AbstractKeyProvider<String> {

        private StringKeyProvider(Generator generator) {
            super(generator);
        }

        @Override
        public String next() {
            return this.generator.nextString();
        }

        @Override
        public String lastKey() {
            return this.generator.lastString();
        }
    }

    public static class ByteArrayKeyProvider extends AbstractKeyProvider<byte[]> {

        private ByteArrayKeyProvider(IntegerGenerator generator) {
            super(generator);
        }

        @Override
        public byte[] next() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(((IntegerGenerator) this.generator).nextInt());
            return bos.toByteArray();
        }

        @Override
        public byte[] lastKey() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(((IntegerGenerator) this.generator).lastInt());
            return bos.toByteArray();
        }
    }

    public static class CachedKeyProvider<T> implements KeyProvider<T> {

        private final KeyProvider<T> delegate;
        private final int percentCached;
        private final AtomicInteger totalRequests = new AtomicInteger(0);
        private final AtomicInteger cachedRequests = new AtomicInteger(0);
        private final List<T> visitedKeys = new ArrayList<T>();
        private T lastKey;

        private CachedKeyProvider(KeyProvider<T> delegate, int percentCached) {
            this.delegate = delegate;
            this.percentCached = percentCached;
        }

        private T getCachedRecord() {
            int expectedCacheCount = (totalRequests.getAndIncrement() * percentCached) / 100;
            if(expectedCacheCount >= cachedRequests.get()) {
                synchronized(visitedKeys) {
                    if(!visitedKeys.isEmpty()) {
                        cachedRequests.incrementAndGet();
                        return visitedKeys.get(new Random().nextInt(visitedKeys.size()));
                    }
                }
            }
            return null;
        }

        @Override
        public T next() {
            T cachedRecord = getCachedRecord();
            if(cachedRecord == null) {
                T value = delegate.next();
                synchronized(visitedKeys) {
                    visitedKeys.add(value);
                }
                lastKey = value;
                return value;
            } else {
                lastKey = cachedRecord;
                return cachedRecord;
            }
        }

        @Override
        public T lastKey() {
            return lastKey;
        }
    }

    public static KeyProvider<?> getKeyProvider(Class<?> cls, Generator generator, int percentCached) {
        if(cls == Integer.class) {
            IntegerKeyProvider kp = new IntegerKeyProvider((IntegerGenerator) generator);
            return percentCached != 0 ? new CachedKeyProvider<Integer>(kp, percentCached) : kp;
        } else if(cls == String.class) {
            StringKeyProvider kp = new StringKeyProvider(generator);
            return percentCached != 0 ? new CachedKeyProvider<String>(kp, percentCached) : kp;
        } else if(cls == byte[].class) {
            ByteArrayKeyProvider kp = new ByteArrayKeyProvider((IntegerGenerator) generator);
            return percentCached != 0 ? new CachedKeyProvider<byte[]>(kp, percentCached) : kp;
        } else {
            throw new IllegalArgumentException("No KeyProvider exists for class " + cls);
        }
    }

    public static Class<?> getKeyTypeClass(String keyType) {
        if(Benchmark.IDENTITY_KEY_TYPE.equals(keyType)) {
            return byte[].class;
        } else if(Benchmark.JSONINT_KEY_TYPE.equals(keyType)) {
            return Integer.class;
        } else if(Benchmark.JSONSTRING_KEY_TYPE.equals(keyType)) {
            return String.class;
        } else { // Default value
            return String.class;
        }
    }

    public List<Integer> loadKeys(File file) throws IOException {

        List<Integer> targets = new ArrayList<Integer>();
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

    public List<String> loadCustomKeys(File file) throws IOException {

        List<String> targets = new ArrayList<String>();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;
            while((text = reader.readLine()) != null) {
                targets.add(text.replaceAll("\\s+", ""));
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

    private DiscreteGenerator operationChooser;
    private DiscreteGenerator transformsChooser;
    private KeyProvider<?> warmUpKeyProvider;
    private KeyProvider<?> keyProvider;
    private String value;
    private ArrayList<Versioned<Object>> sampleValues;
    private Random randomSampler;
    private int sampleSize;

    /**
     * Initialize the workload. Called once, in the main client thread, before
     * any operations are started.
     */
    public void init(Props props) {
        int readPercent = props.getInt(Benchmark.READS, 0);
        int batchReadPercent = props.getInt(Benchmark.BATCH_READS, 0);
        int writePercent = props.getInt(Benchmark.WRITES, 0);
        int deletePercent = props.getInt(Benchmark.DELETES, 0);
        int mixedPercent = props.getInt(Benchmark.MIXED, 0);
        int valueSize = props.getInt(Benchmark.VALUE_SIZE, 1024);
        this.value = new String(TestUtils.randomBytes(valueSize));
        this.sampleSize = props.getInt(Benchmark.SAMPLE_SIZE, 0);
        int cachedPercent = props.getInt(Benchmark.PERCENT_CACHED, 0);
        String keyType = props.getString(Benchmark.KEY_TYPE, Benchmark.STRING_KEY_TYPE);
        String recordSelection = props.getString(Benchmark.RECORD_SELECTION,
                                                 Benchmark.UNIFORM_RECORD_SELECTION);

        boolean hasTransforms = props.getString(Benchmark.HAS_TRANSFORMS, "false")
                                     .compareTo("true") == 0;

        double readProportion = (double) readPercent / (double) 100;
        double batchReadProportion = (double) batchReadPercent / (double) 100;
        double writeProportion = (double) writePercent / (double) 100;
        double deleteProportion = (double) deletePercent / (double) 100;
        double mixedProportion = (double) mixedPercent / (double) 100;

        // Using default read only
        if(Math.abs(writeProportion + readProportion + batchReadProportion + mixedProportion
                    + deleteProportion) != 1.0) {
            throw new VoldemortException("The sum of all workload percentage is NOT 100% \n"
                                         + " Read=" + (double) readPercent / (double) 100
                                         + " BatchRead=" + (double) batchReadPercent / (double) 100
                                         + " Write=" + (double) writePercent / (double) 100
                                         + " Delete=" + (double) deletePercent / (double) 100
                                         + " Mixed=" + (double) mixedPercent / (double) 100);
        }

        List<String> keysFromFile = null;
        int recordCount = 0;
        if(props.containsKey(Benchmark.REQUEST_FILE)) {
            try {
                String fileRecordSelectionFile = props.getString(Benchmark.REQUEST_FILE);
                if(!new File(fileRecordSelectionFile).exists()) {
                    throw new UndefinedPropertyException("File does not exist");
                }
                // keysFromFile = loadKeys(new File(fileRecordSelectionFile));
                keysFromFile = loadCustomKeys(new File(fileRecordSelectionFile));
                recordSelection = new String(Benchmark.FILE_RECORD_SELECTION);
            } catch(Exception e) {
                // Falling back to default uniform selection
                recordSelection = new String(Benchmark.UNIFORM_RECORD_SELECTION);
            }
        } else {
            recordCount = props.getInt(Benchmark.RECORD_COUNT, -1);
        }

        // 1. request_file option is used when sample_size is specified;
        // 2. sample_size shall be no greater than # of keys in the request_file
        if(sampleSize > 0) {
            if(recordSelection.compareTo(Benchmark.FILE_RECORD_SELECTION) != 0) {
                sampleSize = 0;
                logger.warn(Benchmark.SAMPLE_SIZE + " will be ignored because "
                            + Benchmark.REQUEST_FILE + " is not specified.");
            } else if(keysFromFile.size() < sampleSize) {
                sampleSize = keysFromFile.size();
                logger.warn(Benchmark.SAMPLE_SIZE + " is reduced to " + sampleSize
                            + " because it was larger than number of keys in "
                            + Benchmark.REQUEST_FILE);
            }
        }

        int opCount = props.getInt(Benchmark.OPS_COUNT);

        Class<?> keyTypeClass = getKeyTypeClass(keyType);
        int insertStart = props.getInt(Benchmark.START_KEY_INDEX, 0);

        IntegerGenerator warmUpKeySequence = new CounterGenerator(insertStart);
        this.warmUpKeyProvider = getKeyProvider(keyTypeClass, warmUpKeySequence, 0);

        this.transformsChooser = null;
        if(hasTransforms) {
            this.transformsChooser = new DiscreteGenerator();
            List<String> transforms = BenchmarkViews.getTransforms();
            for(String transform: transforms) {
                this.transformsChooser.addValue(1.0, transform);
            }
        }

        operationChooser = new DiscreteGenerator();
        if(readProportion > 0) {
            operationChooser.addValue(readProportion, Benchmark.READS);
        }
        if(batchReadProportion > 0) {
            operationChooser.addValue(batchReadProportion, Benchmark.BATCH_READS);
        }
        if(mixedProportion > 0) {
            operationChooser.addValue(mixedProportion, Benchmark.MIXED);
        }
        if(writeProportion > 0) {
            operationChooser.addValue(writeProportion, Benchmark.WRITES);
        }
        if(deleteProportion > 0) {
            operationChooser.addValue(deleteProportion, Benchmark.DELETES);
        }

        CounterGenerator insertKeySequence = null;
        if(recordCount > 0) {
            insertKeySequence = new CounterGenerator(recordCount);
        } else {
            Random randomizer = new Random();
            insertKeySequence = new CounterGenerator(randomizer.nextInt(Integer.MAX_VALUE));

        }

        Generator keyGenerator = null;
        if(recordSelection.compareTo(Benchmark.UNIFORM_RECORD_SELECTION) == 0) {

            int keySpace = (recordCount > 0) ? recordCount : Integer.MAX_VALUE;
            keyGenerator = new UniformIntegerGenerator(0, keySpace - 1);

        } else if(recordSelection.compareTo(Benchmark.ZIPFIAN_RECORD_SELECTION) == 0) {

            int expectedNewKeys = (int) (opCount * writeProportion * 2.0);
            keyGenerator = new ScrambledZipfianGenerator(recordCount + expectedNewKeys);

        } else if(recordSelection.compareTo(Benchmark.LATEST_RECORD_SELECTION) == 0) {

            keyGenerator = new SkewedLatestGenerator(insertKeySequence);

        } else if(recordSelection.compareTo(Benchmark.FILE_RECORD_SELECTION) == 0) {

            // keyGenerator = new FileIntegerGenerator(0, keysFromFile);
            keyGenerator = new FileStringGenerator(0, keysFromFile);

        }
        this.keyProvider = getKeyProvider(keyTypeClass, keyGenerator, cachedPercent);
        this.randomSampler = new Random(System.currentTimeMillis());
    }

    public boolean doWrite(VoldemortWrapper db, WorkloadPlugin plugin) {
        Object key = warmUpKeyProvider.next();
        if(plugin != null) {
            return plugin.doWrite(key, this.value);
        }
        db.write(key, this.value, null);
        return true;
    }

    public boolean doTransaction(VoldemortWrapper db, WorkloadPlugin plugin) {
        String op = operationChooser.nextString();

        String transform = null;
        if(transformsChooser != null) {
            transform = transformsChooser.nextString();
        }

        if(plugin != null) {
            return plugin.doTransaction(op, transform);
        }

        Object key = keyProvider.next();
        if(op.compareTo(Benchmark.WRITES) == 0) {
            if(sampleSize > 0) {
                writeSampleValue(db, key);
            } else {
                db.write(key, this.value, transform);
            }
        } else if(op.compareTo(Benchmark.MIXED) == 0) {
            db.mixed(key, this.value, transform);
        } else if(op.compareTo(Benchmark.DELETES) == 0) {
            db.delete(key);
        } else if(op.compareTo(Benchmark.READS) == 0) {
            db.read(key, this.value, transform);
        } else if(op.compareTo(Benchmark.BATCH_READS) == 0) {
            List<Object> keys = new ArrayList<Object>();
            for(int i = 0; i < 10; i++) {
                keys.add(keyProvider.next());
            }
            db.batchread(keys);
        }
        return true;

    }

    private void writeSampleValue(VoldemortWrapper db, Object key) {
        db.write(key, getRandomSampleValue(), null);
    }

    public void loadSampleValues(StoreClient<Object, Object> client) {
        if(this.sampleSize > 0) {
            sampleValues = Lists.newArrayList();
            int ignored = 0;
            int newSize = 0;
            for(int i = 0; ((i - ignored) < sampleSize) && (ignored < sampleSize); i++, newSize++) {
                Object key = keyProvider.next();
                if (key != null) {
                    Versioned<Object> versioned = client.get(key);
                    if(null == versioned) {
                        //logger.error("NULL is sampled for key " + key);
                        //System.err.println("NULL is sampled for key " + key);
                        ignored++;
                    } else {
                        sampleValues.add(versioned);
                    }
                } else {
                    System.err.println("NULL Key");
                }
            }
            System.err.println("Ignored " + ignored + " keys");
            System.err.println("Sampled " + newSize + " keys");
            sampleSize = newSize;
        }
    }

    public Object getRandomSampleValue() {
        Object value = null;
        Versioned<Object> versioned = sampleValues.get(randomSampler.nextInt(sampleSize) % sampleValues.size());
        if(versioned != null) {
            value = versioned.getValue();
        }
        return value;
    }
}

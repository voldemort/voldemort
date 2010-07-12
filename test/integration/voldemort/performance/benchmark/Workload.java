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

import voldemort.performance.benchmark.generator.CounterGenerator;
import voldemort.performance.benchmark.generator.DiscreteGenerator;
import voldemort.performance.benchmark.generator.FileIntegerGenerator;
import voldemort.performance.benchmark.generator.IntegerGenerator;
import voldemort.performance.benchmark.generator.ScrambledZipfianGenerator;
import voldemort.performance.benchmark.generator.SkewedLatestGenerator;
import voldemort.performance.benchmark.generator.UniformIntegerGenerator;
import voldemort.utils.Props;
import voldemort.utils.UndefinedPropertyException;

public class Workload {

    public interface KeyProvider<T> {

        public T next();

        public T next(int maxNumber);

        public int lastInt();

    }

    public abstract static class AbstractKeyProvider<T> implements KeyProvider<T> {

        protected IntegerGenerator generator;

        private AbstractKeyProvider(IntegerGenerator generator) {
            this.generator = generator;
        }

        public abstract T next(int maxNumber);

        public abstract T next();

        public int lastInt() {
            return generator.lastInt();
        }

        private Integer nextLessThan(int maxNumber) {
            int nextNum = 0;
            do {
                nextNum = generator.nextInt();
            } while(nextNum > maxNumber);
            return nextNum;
        }

    }

    public static class IntegerKeyProvider extends AbstractKeyProvider<Integer> {

        private IntegerKeyProvider(IntegerGenerator generator) {
            super(generator);
        }

        @Override
        public Integer next() {
            return this.generator.nextInt();
        }

        @Override
        public Integer next(int maxNumber) {
            return super.nextLessThan(maxNumber);
        }

    }

    public static class StringKeyProvider extends AbstractKeyProvider<String> {

        private StringKeyProvider(IntegerGenerator generator) {
            super(generator);
        }

        @Override
        public String next() {
            return this.generator.nextString();
        }

        @Override
        public String next(int maxNumber) {
            return "" + super.nextLessThan(maxNumber);
        }

    }

    public static class ByteArrayKeyProvider extends AbstractKeyProvider<byte[]> {

        private ByteArrayKeyProvider(IntegerGenerator generator) {
            super(generator);
        }

        @Override
        public byte[] next() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(this.generator.nextInt());
            return bos.toByteArray();
        }

        @Override
        public byte[] next(int maxNumber) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(super.nextLessThan(maxNumber));
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

        public T next() {
            T cachedRecord = getCachedRecord();
            if(cachedRecord == null) {
                T value = delegate.next();
                synchronized(visitedKeys) {
                    visitedKeys.add(value);
                }
                return value;
            } else {
                return cachedRecord;
            }
        }

        public T next(int maxNumber) {
            T cachedRecord = getCachedRecord();
            if(cachedRecord == null) {
                T value = delegate.next(maxNumber);
                synchronized(visitedKeys) {
                    visitedKeys.add(value);
                }
                return value;
            } else {
                return cachedRecord;
            }
        }

        /**
         * Never used
         */
        public int lastInt() {
            return 0;
        }
    }

    public static KeyProvider<?> getKeyProvider(Class<?> cls,
                                                IntegerGenerator generator,
                                                int percentCached) {
        if(cls == Integer.class) {
            IntegerKeyProvider kp = new IntegerKeyProvider(generator);
            return percentCached != 0 ? new CachedKeyProvider<Integer>(kp, percentCached) : kp;
        } else if(cls == String.class) {
            StringKeyProvider kp = new StringKeyProvider(generator);
            return percentCached != 0 ? new CachedKeyProvider<String>(kp, percentCached) : kp;
        } else if(cls == byte[].class) {
            ByteArrayKeyProvider kp = new ByteArrayKeyProvider(generator);
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

    private DiscreteGenerator operationChooser;
    private KeyProvider<?> warmUpKeyProvider;
    private KeyProvider<?> insertKeyProvider;
    private KeyProvider<?> keyProvider;
    private String value;

    /**
     * Initialize the workload. Called once, in the main client thread, before
     * any operations are started.
     */
    public void init(Props props) {
        int readPercent = props.getInt(Benchmark.READS, 0);
        int writePercent = props.getInt(Benchmark.WRITES, 0);
        int deletePercent = props.getInt(Benchmark.DELETES, 0);
        int mixedPercent = props.getInt(Benchmark.MIXED, 0);
        int valueSize = props.getInt(Benchmark.VALUE_SIZE, 1024);
        this.value = BenchmarkUtils.ASCIIString(valueSize);
        int cachedPercent = props.getInt(Benchmark.PERCENT_CACHED, 0);
        String keyType = props.getString(Benchmark.KEY_TYPE, Benchmark.STRING_KEY_TYPE);
        String recordSelection = props.getString(Benchmark.RECORD_SELECTION,
                                                 Benchmark.UNIFORM_RECORD_SELECTION);

        double readProportion = (double) readPercent / (double) 100;
        double writeProportion = (double) writePercent / (double) 100;
        double deleteProportion = (double) deletePercent / (double) 100;
        double mixedProportion = (double) mixedPercent / (double) 100;

        if(Math.abs((writeProportion + readProportion + mixedProportion + deleteProportion) - 1.0) > 0.1) {
            readProportion = 1.0;
            mixedProportion = 0.0;
            writeProportion = 0.0;
            deleteProportion = 0.0;
        }

        List<Integer> keysFromFile = null;
        int recordCount = 0;
        if(props.containsKey(Benchmark.REQUEST_FILE)) {
            try {
                String fileRecordSelectionFile = props.getString(Benchmark.REQUEST_FILE);
                if(!new File(fileRecordSelectionFile).exists()) {
                    throw new UndefinedPropertyException("File does not exist");
                }
                keysFromFile = loadKeys(new File(fileRecordSelectionFile));
                recordSelection = new String(Benchmark.FILE_RECORD_SELECTION);
            } catch(Exception e) {
                // Falling back to default uniform selection
                recordSelection = new String(Benchmark.UNIFORM_RECORD_SELECTION);
            }
        } else {
            recordCount = props.getInt(Benchmark.RECORD_COUNT, -1);
        }
        int opCount = props.getInt(Benchmark.OPS_COUNT);

        Class<?> keyTypeClass = getKeyTypeClass(keyType);
        int insertStart = props.getInt(Benchmark.START_KEY_INDEX, 0);

        IntegerGenerator warmUpKeySequence = new CounterGenerator(insertStart);
        this.warmUpKeyProvider = getKeyProvider(keyTypeClass, warmUpKeySequence, 0);

        operationChooser = new DiscreteGenerator();
        if(readProportion > 0) {
            operationChooser.addValue(readProportion, Benchmark.READS);
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
        this.insertKeyProvider = getKeyProvider(keyTypeClass, insertKeySequence, 0);

        IntegerGenerator keyGenerator = null;
        if(recordSelection.compareTo(Benchmark.UNIFORM_RECORD_SELECTION) == 0) {

            int keySpace = (recordCount > 0) ? recordCount : Integer.MAX_VALUE;
            keyGenerator = new UniformIntegerGenerator(0, keySpace - 1);

        } else if(recordSelection.compareTo(Benchmark.ZIPFIAN_RECORD_SELECTION) == 0) {

            int expectedNewKeys = (int) (opCount * writeProportion * 2.0);
            keyGenerator = new ScrambledZipfianGenerator(recordCount + expectedNewKeys);

        } else if(recordSelection.compareTo(Benchmark.LATEST_RECORD_SELECTION) == 0) {

            keyGenerator = new SkewedLatestGenerator(insertKeySequence);

        } else if(recordSelection.compareTo(Benchmark.FILE_RECORD_SELECTION) == 0) {

            keyGenerator = new FileIntegerGenerator(insertStart, keysFromFile);

        }
        this.keyProvider = getKeyProvider(keyTypeClass, keyGenerator, cachedPercent);

    }

    public boolean doWrite(VoldemortWrapper db, WorkloadPlugin plugin) {
        Object key = warmUpKeyProvider.next();
        if (plugin != null) {
            return plugin.doWrite(key, this.value);
        }
        db.write(key, this.value);
        return true;
    }

    public boolean doTransaction(VoldemortWrapper db, WorkloadPlugin plugin) {
        String op = operationChooser.nextString();
        if (plugin != null) {
            return plugin.doTransaction(op);
        }
        if(op.compareTo(Benchmark.READS) == 0) {
            doTransactionRead(db);
        } else if(op.compareTo(Benchmark.MIXED) == 0) {
            doTransactionMixed(db);
        } else if(op.compareTo(Benchmark.WRITES) == 0) {
            doTransactionWrites(db);
        } else if(op.compareTo(Benchmark.DELETES) == 0) {
            doTransactionDelete(db);
        }
        return true;

    }

    public void doTransactionRead(VoldemortWrapper db) {
        Object key = keyProvider.next(insertKeyProvider.lastInt());
        db.read(key, this.value);
    }

    public void doTransactionDelete(VoldemortWrapper db) {
        Object key = keyProvider.next(insertKeyProvider.lastInt());
        db.delete(key);
    }

    public void doTransactionMixed(VoldemortWrapper db) {
        Object key = keyProvider.next(insertKeyProvider.lastInt());
        db.mixed(key, this.value);
    }

    public void doTransactionWrites(VoldemortWrapper db) {
        Object key = insertKeyProvider.next();
        db.write(key, this.value);
    }
}

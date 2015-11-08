package voldemort.performance.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.google.common.collect.Lists;

import voldemort.client.StoreClient;
import voldemort.performance.benchmark.generator.DiscreteGenerator;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * This class implements a workload whose key size distribution, value size
 * distribution and key access sequence are loaded from files. Thus it allows
 * real world workload to be replayed.
 */
public class TraceBasedWorkload implements BenchmarkWorkload {

    private DiscreteGenerator operationChooser;
    private DiscreteGenerator transformsChooser;
    private Random randomSampler;
    private int sampleSize;
    private ArrayList<Versioned<Object>> sampleValues;

    private StringKeyValueProvider kvProvider;
    private StringKeySequence keySequence;

    /**
     * This class loads the workload file which has multiple lines to represent
     * key-value pairs. Each line describes a key and the corresponding value
     * size.
     */
    class StringKeyValueProvider {

        // a list of keys
        private List<String> keys = new ArrayList<String>();
        
        // a list of value size
        private List<Integer> valueSizes = new ArrayList<Integer>();

        //
        private int cursor = 0;

        public StringKeyValueProvider(String workloadFile) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(workloadFile));
                String line = null;
                while((line = reader.readLine()) != null) {
                    String[] items = line.trim().split("\\s+");
                    if(items.length == 2) {
                        String curKey = items[0];
                        int valueSize = Integer.parseInt(items[1]);

                        keys.add(curKey);
                        valueSizes.add(valueSize);
                    }
                }
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch(IOException e) {}
                }
            }

        }

        public String getKey(int index) {
            return keys.get(index);
        }

        public String getValue(int index) {
            byte[] value = new byte[valueSizes.get(index)];
            Arrays.fill(value, (byte) 0);
            return new String(value);
        }

        public synchronized Map.Entry<String, String> nextKeyValue() {
            Map.Entry<String, String> kv = new AbstractMap.SimpleEntry<String, String>(keys.get(cursor),
                                                                                       getValue(cursor));
            cursor = (cursor + 1) % keys.size();
            return kv;
        }
    }

    /**
     * This class loads the key access sequence file with multiple lines. Each
     * line is a position that points to the entire key list.
     */
    class StringKeySequence {

        private List<Integer> indices = new ArrayList<Integer>();
        private int cursor = -1;
        private Random sampler = new Random(System.currentTimeMillis());

        public StringKeySequence(String keySeqFile) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(keySeqFile));
                String line = null;
                while((line = reader.readLine()) != null) {
                    line = line.trim();
                    if(line.length() > 0) {
                        int index = Integer.parseInt(line);
                        indices.add(index);
                    }
                }
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                if(reader != null) {
                    try {
                        reader.close();
                    } catch(IOException e) {}
                }
            }
        }

        public int nextKeyIndex() {
            return (++cursor) % indices.size();
        }

        public int randomIndex() {
            return indices.get(sampler.nextInt(indices.size()));
        }
    }

    private Object getRandomSampleValue() {
        Object value = null;
        Versioned<Object> versioned = sampleValues.get(randomSampler.nextInt(sampleSize));
        if(versioned != null) {
            value = versioned.getValue();
        }
        return value;
    }

    @Override
    public boolean doTransaction(VoldemortWrapper db, WorkloadPlugin plugin) {
        String op = operationChooser.nextString();
        String transform = null;
        if(transformsChooser != null) {
            transform = transformsChooser.nextString();
        }

        if(plugin != null) {
            return plugin.doTransaction(op, transform);
        }

        int index = keySequence.nextKeyIndex();
        String key = kvProvider.getKey(index);
        String value = kvProvider.getValue(index);
        if(op.compareTo(Benchmark.WRITES) == 0) {
            if(sampleSize > 0) {
                db.write(key, getRandomSampleValue(), null);
            } else {
                db.write(key, value, transform);
            }
        } else if(op.compareTo(Benchmark.MIXED) == 0) {
            db.mixed(key, value, transform);
        } else if(op.compareTo(Benchmark.DELETES) == 0) {
            db.delete(key);
        } else if(op.compareTo(Benchmark.READS) == 0) {
            db.read(key, value, transform);
        }
        return true;
    }

    @Override
    public boolean doWrite(VoldemortWrapper db, WorkloadPlugin plugin) {
        Entry<String, String> nextKeyValue = kvProvider.nextKeyValue();
        if(plugin != null) {
            return plugin.doWrite(nextKeyValue.getKey(), nextKeyValue.getValue());
        } else {
            db.write(nextKeyValue.getKey(), nextKeyValue.getValue(), null);
            return true;
        }
    }

    @Override
    public void init(Props props) {
        this.sampleSize = props.getInt(Benchmark.SAMPLE_SIZE, 0);

        double readProportion = (double) props.getInt(Benchmark.READS, 0) / (double) 100;
        double writeProportion = (double) props.getInt(Benchmark.WRITES, 0) / (double) 100;
        double deleteProportion = (double) props.getInt(Benchmark.DELETES, 0) / (double) 100;
        double mixedProportion = (double) props.getInt(Benchmark.MIXED, 0) / (double) 100;

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

        // load key and value pairs
        String workloadFile = props.getString(Benchmark.KEY_VALUE_FILE);
        this.kvProvider = new StringKeyValueProvider(workloadFile);
        System.out.println("Finished loading " + workloadFile);

        // load key sequence
        String keySeqFile = props.getString(Benchmark.KEY_SEQUENCE_FILE);
        this.keySequence = new StringKeySequence(keySeqFile);
        System.out.println("Finished loading " + keySeqFile);

        this.randomSampler = new Random(System.currentTimeMillis());
    }

    @Override
    public void loadSampleValues(StoreClient<Object, Object> client) {
        if(this.sampleSize > 0) {
            sampleValues = Lists.newArrayList();
            for(int i = 0; i < sampleSize; i++) {
                int index = keySequence.randomIndex();
                String key = kvProvider.getKey(index);
                Versioned<Object> versioned = client.get(key);
                if(null == versioned) {
                    System.err.println("NULL is sampled for key " + key);
                }
                sampleValues.add(versioned);
            }
        }
    }
}

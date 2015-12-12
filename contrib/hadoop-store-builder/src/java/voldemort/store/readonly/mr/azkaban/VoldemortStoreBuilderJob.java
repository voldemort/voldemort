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

package voldemort.store.readonly.mr.azkaban;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.AvroStoreBuilderMapper;
import voldemort.store.readonly.mr.HadoopStoreBuilder;
import voldemort.store.readonly.mr.JsonStoreBuilderMapper;
import voldemort.store.readonly.mr.serialization.JsonSequenceFileInputFormat;
import voldemort.utils.Props;

/**
 * Build a voldemort store from input data.
 */
public class VoldemortStoreBuilderJob extends AbstractHadoopJob {

    private VoldemortStoreBuilderConf conf;

    public VoldemortStoreBuilderJob(String name, Props props, VoldemortStoreBuilderConf conf)
            throws FileNotFoundException {
        super(name, props);
        this.conf = conf;
    }

    public static final class VoldemortStoreBuilderConf {

        private int replicationFactor;
        private int chunkSize;
        private Path tempDir;
        private Path outputDir;
        private Path inputPath;
        private Cluster cluster;
        private List<StoreDefinition> storeDefs;
        private String storeName;
        private CheckSumType checkSumType;
        private boolean saveKeys;
        private boolean reducerPerBucket;
        private int numChunks = -1;
        private String recSchema = null;
        private String keySchema = null;
        private String valSchema = null;
        private String keyField = null;
        private String valueField = null;
        private boolean isAvro = false;
        private long minNumberOfRecords;

        public VoldemortStoreBuilderConf(int replicationFactor,
                                         int chunkSize,
                                         Path tempDir,
                                         Path outputDir,
                                         Path inputPath,
                                         Cluster cluster,
                                         List<StoreDefinition> storeDefs,
                                         String storeName,
                                         CheckSumType checkSumType,
                                         boolean saveKeys,
                                         boolean reducerPerBucket,
                                         int numChunks,
                                         String keyField,
                                         String valueField,
                                         String recSchema,
                                         String keySchema,
                                         String valSchema,
                                         boolean isAvro,
                                         long minNumberOfRecords) {
            this.replicationFactor = replicationFactor;
            this.chunkSize = chunkSize;
            this.tempDir = tempDir;
            this.outputDir = outputDir;
            this.inputPath = inputPath;
            this.cluster = cluster;
            this.storeDefs = storeDefs;
            this.storeName = storeName;
            this.checkSumType = checkSumType;
            this.saveKeys = saveKeys;
            this.reducerPerBucket = reducerPerBucket;
            this.numChunks = numChunks;
            this.keyField = keyField;
            this.valueField = valueField;
            this.recSchema = recSchema;
            this.keySchema = keySchema;
            this.valSchema = valSchema;
            this.isAvro = isAvro;
            this.minNumberOfRecords = minNumberOfRecords;
        }

        public int getReplicationFactor() {
            return replicationFactor;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public Path getTempDir() {
            return tempDir;
        }

        public Path getOutputDir() {
            return outputDir;
        }

        public Path getInputPath() {
            return inputPath;
        }

        public String getStoreName() {
            return storeName;
        }

        public Cluster getCluster() {
            return cluster;
        }

        public List<StoreDefinition> getStoreDefs() {
            return storeDefs;
        }

        public CheckSumType getCheckSumType() {
            return checkSumType;
        }

        public boolean getSaveKeys() {
            return saveKeys;
        }

        public boolean getReducerPerBucket() {
            return reducerPerBucket;
        }

        public int getNumChunks() {
            return numChunks;
        }

        public String getRecSchema() {
            return recSchema;
        }

        public String getKeySchema() {
            return keySchema;
        }

        public String getValSchema() {
            return valSchema;
        }

        public String getValueField() {
            return valueField;
        }

        public String getKeyField() {
            return keyField;
        }

        public boolean isAvro() {
            return isAvro;
        }

        public long getMinNumberOfRecords() {
            return minNumberOfRecords;
        }

    }

    public void run() throws Exception {
        JobConf configuration = this.createJobConf();

        Class mapperClass;
        Class<? extends InputFormat> inputFormatClass;

        // Only if its a avro job we supply some additional fields
        // for the key value schema of the avro record
        if(conf.isAvro()) {
            configuration.set("avro.rec.schema", conf.getRecSchema());
            configuration.set("avro.key.schema", conf.getKeySchema());
            configuration.set("avro.val.schema", conf.getValSchema());
            configuration.set(VoldemortBuildAndPushJob.AVRO_KEY_FIELD, conf.getKeyField());
            configuration.set(VoldemortBuildAndPushJob.AVRO_VALUE_FIELD, conf.getValueField());
            mapperClass = AvroStoreBuilderMapper.class;
            inputFormatClass = AvroInputFormat.class;
        } else {
            mapperClass = JsonStoreBuilderMapper.class;
            inputFormatClass = JsonSequenceFileInputFormat.class;
        }

        String storeName = conf.getStoreName();
        StoreDefinition storeDef = null;
        for(StoreDefinition def: conf.getStoreDefs())
            if(storeName.equals(def.getName()))
                storeDef = def;
        if(storeDef == null)
            throw new IllegalArgumentException("Store '" + storeName + "' not found.");

        Path outputDir = conf.getOutputDir();
        FileSystem fs = outputDir.getFileSystem(configuration);
        if(fs.exists(outputDir)) {
            info("Deleting previous output in " + outputDir + " for building store " + storeName);
            fs.delete(outputDir, true);
        }

        HadoopStoreBuilder builder = new HadoopStoreBuilder(
                configuration,
                mapperClass,
                inputFormatClass,
                conf.getCluster(),
                storeDef,
                conf.getTempDir(),
                outputDir,
                conf.getInputPath(),
                conf.getCheckSumType(),
                conf.getSaveKeys(),
                conf.getReducerPerBucket(),
                conf.getChunkSize(),
                conf.getNumChunks(),
                conf.isAvro(),
                conf.getMinNumberOfRecords());
        builder.build();
    }

}

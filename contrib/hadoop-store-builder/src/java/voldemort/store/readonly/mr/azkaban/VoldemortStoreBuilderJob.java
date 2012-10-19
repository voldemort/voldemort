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
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.AvroStoreBuilderMapper;
import voldemort.store.readonly.mr.HadoopStoreBuilder;
import voldemort.store.readonly.mr.VoldemortStoreBuilderMapper;
import voldemort.store.readonly.mr.serialization.JsonSequenceFileInputFormat;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;
import azkaban.common.utils.Props;

/**
 * Build a voldemort store from input data.
 * 
 * @author jkreps
 * 
 */
public class VoldemortStoreBuilderJob extends AbstractHadoopJob {

    private VoldemortStoreBuilderConf conf;
    private boolean isAvro;

    public VoldemortStoreBuilderJob(String name, Props props) throws Exception {
        super(name, props);
        this.conf = new VoldemortStoreBuilderConf(createJobConf(VoldemortStoreBuilderMapper.class),
                                                  props);
        this.isAvro = false;
    }

    public VoldemortStoreBuilderJob(String name, Props props, VoldemortStoreBuilderConf conf)
                                                                                             throws FileNotFoundException {
        super(name, props);
        this.conf = conf;
        this.isAvro = false;
    }

    public VoldemortStoreBuilderJob(String name, Props props, boolean isAvro) throws Exception {
        super(name, props);
        this.conf = new VoldemortStoreBuilderConf(createJobConf(VoldemortStoreBuilderMapper.class),
                                                  props);
        this.isAvro = isAvro;
    }

    public VoldemortStoreBuilderJob(String name,
                                    Props props,
                                    VoldemortStoreBuilderConf conf,
                                    boolean isAvro) throws FileNotFoundException {
        super(name, props);
        this.conf = conf;
        this.isAvro = isAvro;
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
        private String keySelection;
        private String valSelection;
        private String keyTrans;
        private String valTrans;
        private CheckSumType checkSumType;
        private boolean saveKeys;
        private boolean reducerPerBucket;
        private int numChunks = -1;

        private String recSchema;
        private String keySchema;
        private String valSchema;

        private String keyField;
        private String valueField;

        public VoldemortStoreBuilderConf(int replicationFactor,
                                         int chunkSize,
                                         Path tempDir,
                                         Path outputDir,
                                         Path inputPath,
                                         Cluster cluster,
                                         List<StoreDefinition> storeDefs,
                                         String storeName,
                                         String keySelection,
                                         String valSelection,
                                         String keyTrans,
                                         String valTrans,
                                         CheckSumType checkSumType,
                                         boolean saveKeys,
                                         boolean reducerPerBucket,
                                         int numChunks) {
            this.replicationFactor = replicationFactor;
            this.chunkSize = chunkSize;
            this.tempDir = tempDir;
            this.outputDir = outputDir;
            this.inputPath = inputPath;
            this.cluster = cluster;
            this.storeDefs = storeDefs;
            this.storeName = storeName;
            this.keySelection = keySelection;
            this.valSelection = valSelection;
            this.keyTrans = keyTrans;
            this.valTrans = valTrans;
            this.checkSumType = checkSumType;
            this.saveKeys = saveKeys;
            this.reducerPerBucket = reducerPerBucket;
            this.numChunks = numChunks;
        }

        // requires job conf in order to get files from the filesystem
        public VoldemortStoreBuilderConf(JobConf configuration, Props props) throws Exception {
            this(props.getInt("replication.factor", 2),
                 props.getInt("chunk.size", 1024 * 1024 * 1024),
                 new Path(props.getString("temp.dir",
                                          "/tmp/vold-build-and-push-" + new Random().nextLong())),
                 new Path(props.getString("output.dir")),
                 new Path(props.getString("input.path")),
                 new ClusterMapper().readCluster(new InputStreamReader(new Path(props.getString("cluster.xml")).getFileSystem(configuration)
                                                                                                               .open(new Path(props.getString("cluster.xml"))))),
                 new StoreDefinitionsMapper().readStoreList(new InputStreamReader(new Path(props.getString("stores.xml")).getFileSystem(configuration)
                                                                                                                         .open(new Path(props.getString("stores.xml"))))),
                 props.getString("store.name"),
                 props.getString("key.selection", null),
                 props.getString("value.selection", null),
                 props.getString("key.transformation.class", null),
                 props.getString("value.transformation.class", null),
                 CheckSum.fromString(props.getString("checksum.type",
                                                     CheckSum.toString(CheckSumType.MD5))),
                 props.getBoolean("save.keys", true),
                 props.getBoolean("reducer.per.bucket", false),
                 props.getInt("num.chunks", -1));
        }

        // new constructor to include the key val and record schema for Avro job

        public VoldemortStoreBuilderConf(int replicationFactor,
                                         int chunkSize,
                                         Path tempDir,
                                         Path outputDir,
                                         Path inputPath,
                                         Cluster cluster,
                                         List<StoreDefinition> storeDefs,
                                         String storeName,
                                         String keySelection,
                                         String valSelection,
                                         String keyTrans,
                                         String valTrans,
                                         CheckSumType checkSumType,
                                         boolean saveKeys,
                                         boolean reducerPerBucket,
                                         int numChunks,
                                         String keyField,
                                         String valueField,
                                         String recSchema,
                                         String keySchema,
                                         String valSchema) {
            this.replicationFactor = replicationFactor;
            this.chunkSize = chunkSize;
            this.tempDir = tempDir;
            this.outputDir = outputDir;
            this.inputPath = inputPath;
            this.cluster = cluster;
            this.storeDefs = storeDefs;
            this.storeName = storeName;
            this.keySelection = keySelection;
            this.valSelection = valSelection;
            this.keyTrans = keyTrans;
            this.valTrans = valTrans;
            this.checkSumType = checkSumType;
            this.saveKeys = saveKeys;
            this.reducerPerBucket = reducerPerBucket;
            this.numChunks = numChunks;

            this.keyField = keyField;
            this.valueField = valueField;
            this.recSchema = recSchema;
            this.keySchema = keySchema;
            this.valSchema = valSchema;

        }

        // requires job conf in order to get files from the filesystem
        public VoldemortStoreBuilderConf(JobConf configuration,
                                         Props props,
                                         String keyField,
                                         String valueField,
                                         String recSchema,
                                         String keySchema,
                                         String valSchema) throws Exception {
            this(props.getInt("replication.factor", 2),
                 props.getInt("chunk.size", 1024 * 1024 * 1024),
                 new Path(props.getString("temp.dir",
                                          "/tmp/vold-build-and-push-" + new Random().nextLong())),
                 new Path(props.getString("output.dir")),
                 new Path(props.getString("input.path")),
                 new ClusterMapper().readCluster(new InputStreamReader(new Path(props.getString("cluster.xml")).getFileSystem(configuration)
                                                                                                               .open(new Path(props.getString("cluster.xml"))))),
                 new StoreDefinitionsMapper().readStoreList(new InputStreamReader(new Path(props.getString("stores.xml")).getFileSystem(configuration)
                                                                                                                         .open(new Path(props.getString("stores.xml"))))),
                 props.getString("store.name"),
                 props.getString("key.selection", null),
                 props.getString("value.selection", null),
                 props.getString("key.transformation.class", null),
                 props.getString("value.transformation.class", null),
                 CheckSum.fromString(props.getString("checksum.type",
                                                     CheckSum.toString(CheckSumType.MD5))),
                 props.getBoolean("save.keys", true),
                 props.getBoolean("reducer.per.bucket", false),
                 props.getInt("num.chunks", -1),
                 keyField,
                 valueField,
                 recSchema,
                 keySchema,
                 valSchema);
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

        public String getKeySelection() {
            return keySelection;
        }

        public String getValSelection() {
            return valSelection;
        }

        public String getKeyTrans() {
            return keyTrans;
        }

        public String getValTrans() {
            return valTrans;
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

        public void setRecSchema(String recSchema) {
            this.recSchema = recSchema;
        }

        public String getKeySchema() {
            return keySchema;
        }

        public void setKeySchema(String keySchema) {
            this.keySchema = keySchema;
        }

        public String getValSchema() {
            return valSchema;
        }

        public void setValSchema(String valSchema) {
            this.valSchema = valSchema;
        }

        public String getValueField() {
            return valueField;
        }

        public void setValueField(String valueField) {
            this.valueField = valueField;
        }

        public String getKeyField() {
            return keyField;
        }

        public void setKeyField(String keyField) {
            this.keyField = keyField;
        }

    }

    @Override
    public void run() throws Exception {
        JobConf configuration = this.createJobConf(VoldemortStoreBuilderMapper.class);

        // Only if its a avro job we supply some additional fields
        // for the key value schema of the avro record
        if(isAvro) {
            String recSchema = conf.getRecSchema();
            String keySchema = conf.getKeySchema();
            String valSchema = conf.getValSchema();

            String keyField = conf.getKeyField();
            String valueField = conf.getValueField();

            configuration.set("avro.rec.schema", recSchema);
            configuration.set("avro.key.schema", keySchema);
            configuration.set("avro.val.schema", valSchema);

            configuration.set("avro.key.field", keyField);
            configuration.set("avro.value.field", valueField);
        }
        int chunkSize = conf.getChunkSize();
        Path tempDir = conf.getTempDir();
        Path outputDir = conf.getOutputDir();
        Path inputPath = conf.getInputPath();
        Cluster cluster = conf.getCluster();
        List<StoreDefinition> storeDefs = conf.getStoreDefs();
        String storeName = conf.getStoreName();
        CheckSumType checkSumType = conf.getCheckSumType();
        boolean saveKeys = conf.getSaveKeys();
        boolean reducerPerBucket = conf.getReducerPerBucket();

        StoreDefinition storeDef = null;
        for(StoreDefinition def: storeDefs)
            if(storeName.equals(def.getName()))
                storeDef = def;
        if(storeDef == null)
            throw new IllegalArgumentException("Store '" + storeName + "' not found.");

        FileSystem fs = outputDir.getFileSystem(configuration);
        if(fs.exists(outputDir)) {
            info("Deleting previous output in " + outputDir + " for building store " + storeName);
            fs.delete(outputDir, true);
        }

        HadoopStoreBuilder builder = null;

        if(isAvro) {

            if(conf.getNumChunks() == -1) {
                builder = new HadoopStoreBuilder(configuration,

                                                 AvroStoreBuilderMapper.class,
                                                 (Class<? extends InputFormat>) AvroInputFormat.class,
                                                 cluster,
                                                 storeDef,
                                                 chunkSize,
                                                 tempDir,
                                                 outputDir,
                                                 inputPath,
                                                 checkSumType,
                                                 saveKeys,
                                                 reducerPerBucket);
            } else {
                builder = new HadoopStoreBuilder(configuration,
                                                 AvroStoreBuilderMapper.class,
                                                 (Class<? extends InputFormat>) AvroInputFormat.class,
                                                 cluster,
                                                 storeDef,
                                                 tempDir,
                                                 outputDir,
                                                 inputPath,
                                                 checkSumType,
                                                 saveKeys,
                                                 reducerPerBucket,
                                                 conf.getNumChunks());
            }

            builder.buildAvro();
            return;
        }

        if(conf.getNumChunks() == -1) {
            builder = new HadoopStoreBuilder(configuration,
                                             VoldemortStoreBuilderMapper.class,
                                             JsonSequenceFileInputFormat.class,
                                             cluster,
                                             storeDef,
                                             chunkSize,
                                             tempDir,
                                             outputDir,
                                             inputPath,
                                             checkSumType,
                                             saveKeys,
                                             reducerPerBucket);
        } else {
            builder = new HadoopStoreBuilder(configuration,
                                             VoldemortStoreBuilderMapper.class,
                                             JsonSequenceFileInputFormat.class,
                                             cluster,
                                             storeDef,
                                             tempDir,
                                             outputDir,
                                             inputPath,
                                             checkSumType,
                                             saveKeys,
                                             reducerPerBucket,
                                             conf.getNumChunks());
        }

        builder.build();
    }

}

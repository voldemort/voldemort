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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.mr.serialization.JsonConfigurable;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.utils.ByteUtils;
import azkaban.common.utils.Props;

/**
 * Creates Index and value files using Voldemort hash keys for easy batch
 * update.
 * <p>
 * Creates two files
 * <ul>
 * <li>Index File: Keeps the position Index for each key sorted by MD5(key)
 * chunk-size is KEY_HASH_SIZE(16 bytes) + POSITION_SIZE(8 bytes)</li>
 * <li>Values file: saves the value corrosponding to the Key MD5</li>
 * <ul>
 * <p>
 * Required Properties
 * <ul>
 * <li>voldemort.cluster.file</li>
 * <li>voldemort.storedef.file</li>
 * <li>voldemort.store.name</li>
 * <li>voldemort.store.version</li>
 * <li>input.data.check.percent</li>
 * </ul>
 * 
 * @author bbansal
 * 
 * @deprecated Use {@link VoldemortStoreBuilderJob} instead.
 * 
 */
@Deprecated
public class VoldemortBatchIndexJob extends AbstractHadoopJob {

    private Cluster _cluster = null;

    private static Logger logger = Logger.getLogger(VoldemortStoreBuilderJob.class);

    public VoldemortBatchIndexJob(String name, Props props) throws FileNotFoundException {
        super(name, props);
    }

    /**
     * @deprecated use
     *             {@link VoldemortStoreBuilderJob#execute(String, String, String, String, int)}
     *             the parameter voldemort store version is deprecated and no
     *             longer used. Version is read from the store definition
     *             istead.
     *             <p>
     * @param voldemortClusterLocalFile
     * @param storeName
     * @param inputPath
     * @param outputPath
     * @param voldemortStoreVersion
     * @param voldemortCheckDataPercent
     * @throws IOException
     * @throws URISyntaxException
     */
    @Deprecated
    public void execute(String voldemortClusterLocalFile,
                        String storeName,
                        String inputPath,
                        String outputPath,
                        int voldemortStoreVersion,
                        int voldemortCheckDataPercent) throws IOException, URISyntaxException {
        execute(voldemortClusterLocalFile,
                storeName,
                inputPath,
                outputPath,
                voldemortCheckDataPercent);
    }

    /**
     * Method to allow this process to be a instance call from another Job.
     * 
     * @storeName to dump the value
     * @inputFile to generate the VFILE
     * 
     * 
     */
    public void execute(String voldemortClusterLocalFile,
                        String storeName,
                        String inputPath,
                        String outputPath,
                        int voldemortCheckDataPercent) throws IOException, URISyntaxException {
        JobConf conf = createJobConf(VoldemortBatchIndexMapper.class,
                                     VoldemortBatchIndexReducer.class);

        try {
            // get the voldemort cluster definition
            // We need to use cluster.xml here where it not yet localized by
            // TaskRunner
            _cluster = HadoopUtils.readCluster(voldemortClusterLocalFile, conf);
        } catch(Exception e) {
            logger.error("Failed to read Voldemort cluster details", e);
            throw new RuntimeException("", e);
        }

        // set the partitioner
        conf.setPartitionerClass(VoldemortBatchIndexPartitoner.class);
        conf.setNumReduceTasks(_cluster.getNumberOfNodes());

        // Blow Away the O/p if force.overwirte is available

        FileInputFormat.setInputPaths(conf, inputPath);

        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        if(getProps().getBoolean("force.output.overwrite", false)) {
            FileSystem fs = FileOutputFormat.getOutputPath(conf).getFileSystem(conf);
            fs.delete(FileOutputFormat.getOutputPath(conf), true);
        }

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setMapOutputKeyClass(BytesWritable.class);
        conf.setMapOutputValueClass(BytesWritable.class);
        conf.setOutputKeyClass(BytesWritable.class);
        conf.setOutputValueClass(BytesWritable.class);

        conf.setNumReduceTasks(_cluster.getNumberOfNodes());

        // get the store information

        conf.setStrings("voldemort.index.filename", storeName + ".index");
        conf.setStrings("voldemort.data.filename", storeName + ".data");
        conf.setInt("input.data.check.percent", voldemortCheckDataPercent);
        conf.setStrings("voldemort.store.name", storeName);

        // run(conf);
        JobClient.runJob(conf);

    }

    @Override
    public void run() throws Exception {

        execute(getProps().get("voldemort.cluster.file"),
                getProps().get("voldemort.store.name"),
                getProps().get("input.path"),
                getProps().get("output.path"),
                getProps().getInt("input.data.check.percent", 0));

    }

    /**
     * TODO HIGH : Doesnot check with Voldemort schema should validate the
     * voldemort schema before writing.
     * 
     * @author bbansal
     * 
     */
    public static class VoldemortBatchIndexMapper extends JsonConfigurable implements
            Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

        private static Logger logger = Logger.getLogger(VoldemortBatchIndexMapper.class);
        private Cluster _cluster = null;
        private StoreDefinition _storeDef = null;
        private ConsistentRoutingStrategy _routingStrategy = null;
        private Serializer _keySerializer;
        private Serializer _valueSerializer;
        private int _checkPercent;
        private int _version;

        public void map(BytesWritable key,
                        BytesWritable value,
                        OutputCollector<BytesWritable, BytesWritable> output,
                        Reporter reporter) throws IOException {
            byte[] keyBytes = ByteUtils.copy(key.get(), 0, key.getSize());
            byte[] valBytes = ByteUtils.copy(value.get(), 0, value.getSize());

            ByteArrayOutputStream versionedKey = new ByteArrayOutputStream();
            DataOutputStream keyDin = new DataOutputStream(versionedKey);
            keyDin.write(_version);
            keyDin.write(keyBytes);
            keyDin.close();

            if(logger.isDebugEnabled()) {
                logger.debug("Original key: size:" + versionedKey.toByteArray().length + " val:"
                             + ByteUtils.toHexString(versionedKey.toByteArray()));
                logger.debug("MD5 val: size:" + ByteUtils.md5(versionedKey.toByteArray()).length
                             + " val:"
                             + ByteUtils.toHexString(ByteUtils.md5(versionedKey.toByteArray())));
                logger.debug(" value bytes:" + value.getSize() + " ["
                             + ByteUtils.toHexString(valBytes) + "]");
            }

            List<Node> nodes = _routingStrategy.routeRequest(keyBytes);
            for(Node node: nodes) {
                ByteArrayOutputStream versionedValue = new ByteArrayOutputStream();
                DataOutputStream valueDin = new DataOutputStream(versionedValue);
                valueDin.writeInt(node.getId());
                valueDin.write(_version);
                valueDin.write(valBytes);
                valueDin.close();

                // check input
                if(Math.ceil(Math.random() * 100.0) < _checkPercent) {
                    checkJsonType(versionedKey.toByteArray(),
                                  ByteUtils.copy(versionedValue.toByteArray(),
                                                 4,
                                                 versionedValue.size()));
                }

                BytesWritable outputKey = new BytesWritable(ByteUtils.md5(versionedKey.toByteArray()));
                BytesWritable outputVal = new BytesWritable(versionedValue.toByteArray());

                output.collect(outputKey, outputVal);
            }
        }

        public void checkJsonType(byte[] key, byte[] value) {
            try {
                _keySerializer.toObject(key);
                _valueSerializer.toObject(value);
            } catch(Exception e) {
                throw new RuntimeException("Failed to Serialize key/Value check data and config schema.",
                                           e);
            }
        }

        public void configure(JobConf conf) {
            Props props = HadoopUtils.getPropsFromJob(conf);

            // get the voldemort cluster.xml and store.xml files.
            try {
                _cluster = HadoopUtils.readCluster(props.get("voldemort.cluster.file"), conf);
                _storeDef = HadoopUtils.readStoreDef(props.get("voldemort.store.file"),
                                                     props.get("voldemort.store.name"),
                                                     conf);

                _checkPercent = conf.getInt("input.data.check.percent", 0);
                _routingStrategy = new ConsistentRoutingStrategy(_cluster,
                                                                 _storeDef.getReplicationFactor());
                _keySerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(_storeDef.getKeySerializer());
                _valueSerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(_storeDef.getValueSerializer());

                _version = _storeDef.getKeySerializer().getCurrentSchemaVersion();
                _routingStrategy = new ConsistentRoutingStrategy(_cluster,
                                                                 _storeDef.getReplicationFactor());

                if(_routingStrategy == null) {
                    throw new RuntimeException("Failed to create routing strategy");
                }
            } catch(Exception e) {
                logger.error("Failed to read Voldemort cluster/storeDef details", e);
                throw new RuntimeException("", e);
            }
        }
    }

    public static class VoldemortBatchIndexPartitoner extends
            HashPartitioner<BytesWritable, BytesWritable> {

        @Override
        public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
            // The partition id is first 4 bytes in the value.
            DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(value.get()));
            int nodeId = -2;
            try {
                nodeId = buffer.readInt();
            } catch(IOException e) {
                throw new RuntimeException("Failed to parse nodeId from buffer.", e);
            }
            return (nodeId) % numReduceTasks;
        }
    }

    public static class VoldemortBatchIndexReducer implements
            Reducer<BytesWritable, BytesWritable, Text, Text> {

        private DataOutputStream _indexFileStream = null;
        private DataOutputStream _valueFileStream = null;

        private long _position = 0;

        private JobConf _conf = null;
        private String _taskId = "dummy";
        private int _nodeId = -1;

        String indexFileName;
        String dataFileName;
        Path taskIndexFileName;
        Path taskValueFileName;
        String storeName;

        /**
         * Reduce should get sorted MD5 keys here with a single value (appended
         * in begining with 4 bits of nodeId)
         */
        public void reduce(BytesWritable key,
                           Iterator<BytesWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
            byte[] keyBytes = ByteUtils.copy(key.get(), 0, key.getSize());

            while(values.hasNext()) {
                BytesWritable value = values.next();
                byte[] valBytes = ByteUtils.copy(value.get(), 0, value.getSize());

                if(_nodeId == -1) {
                    DataInputStream buffer = new DataInputStream(new ByteArrayInputStream(valBytes));
                    _nodeId = buffer.readInt();
                }
                // strip first 4 bytes from value here added in mapper for
                // partitioner
                // convenience.
                byte[] value1 = ByteUtils.copy(valBytes, 4, valBytes.length);

                if(logger.isDebugEnabled()) {
                    logger.debug("Reduce Original key: size:" + keyBytes.length + " val:"
                                 + ByteUtils.toHexString(keyBytes));
                    logger.debug("Reduce value bytes:" + value1.length + " ["
                                 + ByteUtils.toHexString(value1) + "]");
                }

                // Write Index Key/ position
                _indexFileStream.write(keyBytes);
                _indexFileStream.writeLong(_position);

                _valueFileStream.writeInt(value1.length);
                _valueFileStream.write(value1);
                _position += value1.length + 4;

                if(_position < 0) {
                    logger.error("Position bigger than Integer size, split input files.");
                    System.exit(1);
                }
            }

        }

        public void configure(JobConf job) {
            Props props = HadoopUtils.getPropsFromJob(job);

            try {
                _position = 0;
                _conf = job;

                _taskId = job.get("mapred.task.id");

                storeName = props.get("voldemort.store.name");
                taskIndexFileName = new Path(FileOutputFormat.getOutputPath(_conf),
                                             _conf.get("voldemort.index.filename") + "_" + _taskId);
                taskValueFileName = new Path(FileOutputFormat.getOutputPath(_conf),
                                             _conf.get("voldemort.data.filename") + "_" + _taskId);

                FileSystem fs = taskIndexFileName.getFileSystem(job);

                _indexFileStream = fs.create(taskIndexFileName, (short) 1);
                _valueFileStream = fs.create(taskValueFileName, (short) 1);
            } catch(IOException e) {
                throw new RuntimeException("Failed to open Input/OutputStream", e);
            }
        }

        public void close() throws IOException {

            _indexFileStream.close();
            _valueFileStream.close();

            Path hdfsIndexFile = new Path(FileOutputFormat.getOutputPath(_conf), _nodeId + ".index");
            Path hdfsValueFile = new Path(FileOutputFormat.getOutputPath(_conf), _nodeId + ".data");

            FileSystem fs = hdfsIndexFile.getFileSystem(_conf);
            fs.rename(taskIndexFileName, hdfsIndexFile);
            fs.rename(taskValueFileName, hdfsValueFile);
        }
    }

}

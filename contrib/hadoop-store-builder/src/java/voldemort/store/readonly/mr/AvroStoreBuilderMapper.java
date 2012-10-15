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

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.avro.AvroGenericSerializer;
import voldemort.serialization.avro.versioned.AvroVersionedGenericSerializer;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import voldemort.store.readonly.mr.utils.MapperKeyValueWriter;
import voldemort.utils.ByteUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;
import azkaban.common.utils.Props;

/**
 * Avro container files are not sequence input format files they contain records
 * instead of k/v pairs to consume these files we use the AvroMapper
 */
public class AvroStoreBuilderMapper extends
        AvroMapper<GenericData.Record, Pair<ByteBuffer, ByteBuffer>> implements JobConfigurable {

    protected MessageDigest md5er;
    protected ConsistentRoutingStrategy routingStrategy;
    protected Serializer keySerializer;
    protected Serializer valueSerializer;

    private String keySchema;
    private String valSchema;

    private String keyField;
    private String valField;

    private CompressionStrategy valueCompressor;
    private CompressionStrategy keyCompressor;
    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;

    /**
     * Create the voldemort key and value from the input Avro record by
     * extracting the key and value and map it out for each of the responsible
     * voldemort nodes
     * 
     * 
     * The output value is the node_id & partition_id of the responsible node
     * followed by serialized value
     */
    @Override
    public void map(GenericData.Record record,
                    AvroCollector<Pair<ByteBuffer, ByteBuffer>> collector,
                    Reporter reporter) throws IOException {

        byte[] keyBytes = keySerializer.toBytes(record.get(keyField));
        byte[] valBytes = valueSerializer.toBytes(record.get(valField));

        MapperKeyValueWriter mapWriter = new MapperKeyValueWriter();

        List mapperList = mapWriter.map(routingStrategy,
                                        keySerializer,
                                        valueSerializer,
                                        valueCompressor,
                                        keyCompressor,
                                        keySerializerDefinition,
                                        valueSerializerDefinition,
                                        keyBytes,
                                        valBytes,
                                        getSaveKeys(),
                                        md5er);

        for(int i = 0; i < mapperList.size(); i++) {
            voldemort.utils.Pair<BytesWritable, BytesWritable> pair = (voldemort.utils.Pair<BytesWritable, BytesWritable>) mapperList.get(i);
            BytesWritable outputKey = pair.getFirst();
            BytesWritable outputVal = pair.getSecond();

            ByteBuffer keyBuffer = null, valueBuffer = null;

            byte[] md5KeyBytes = outputKey.getBytes();
            keyBuffer = ByteBuffer.allocate(md5KeyBytes.length);
            keyBuffer.put(md5KeyBytes);
            keyBuffer.rewind();

            byte[] outputValue = outputVal.getBytes();
            valueBuffer = ByteBuffer.allocate(outputValue.length);
            valueBuffer.put(outputValue);
            valueBuffer.rewind();

            Pair<ByteBuffer, ByteBuffer> p = new Pair<ByteBuffer, ByteBuffer>(keyBuffer,
                                                                              valueBuffer);

            collector.collect(p);
        }

        md5er.reset();
    }

    @Override
    public void configure(JobConf conf) {

        super.setConf(conf);
        // from parent code

        md5er = ByteUtils.getDigest("md5");

        this.cluster = new ClusterMapper().readCluster(new StringReader(conf.get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));

        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);

        this.numChunks = conf.getInt("num.chunks", -1);
        if(this.numChunks < 1)
            throw new VoldemortException("num.chunks not specified in the job conf.");

        this.saveKeys = conf.getBoolean("save.keys", true);
        this.reducerPerBucket = conf.getBoolean("reducer.per.bucket", false);

        keySerializerDefinition = getStoreDef().getKeySerializer();
        valueSerializerDefinition = getStoreDef().getValueSerializer();

        try {
            SerializerFactory factory = new DefaultSerializerFactory();

            if(conf.get("serializer.factory") != null) {
                factory = (SerializerFactory) Class.forName(conf.get("serializer.factory"))
                                                   .newInstance();
            }

            keySerializer = factory.getSerializer(keySerializerDefinition);
            valueSerializer = factory.getSerializer(valueSerializerDefinition);

            keyField = conf.get("avro.key.field");

            valField = conf.get("avro.value.field");

            keySchema = conf.get("avro.key.schema");
            valSchema = conf.get("avro.val.schema");

            if(keySerializerDefinition.getName().equals("avro-generic")) {
                keySerializer = new AvroGenericSerializer(keySchema);
                valueSerializer = new AvroGenericSerializer(valSchema);
            } else {

                if(keySerializerDefinition.hasVersion()) {
                    Map<Integer, String> versions = new HashMap<Integer, String>();
                    for(Map.Entry<Integer, String> entry: keySerializerDefinition.getAllSchemaInfoVersions()
                                                                                 .entrySet())
                        versions.put(entry.getKey(), entry.getValue());
                    keySerializer = new AvroVersionedGenericSerializer(versions);
                } else
                    keySerializer = new AvroVersionedGenericSerializer(keySerializerDefinition.getCurrentSchemaInfo());

                if(valueSerializerDefinition.hasVersion()) {
                    Map<Integer, String> versions = new HashMap<Integer, String>();
                    for(Map.Entry<Integer, String> entry: valueSerializerDefinition.getAllSchemaInfoVersions()
                                                                                   .entrySet())
                        versions.put(entry.getKey(), entry.getValue());
                    valueSerializer = new AvroVersionedGenericSerializer(versions);
                } else
                    valueSerializer = new AvroVersionedGenericSerializer(valueSerializerDefinition.getCurrentSchemaInfo());

            }

        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
        valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());

        routingStrategy = new ConsistentRoutingStrategy(getCluster().getNodes(),
                                                        getStoreDef().getReplicationFactor());

        // /
        Props props = HadoopUtils.getPropsFromJob(conf);

    }

    private int numChunks;
    private Cluster cluster;
    private StoreDefinition storeDef;
    private boolean saveKeys;
    private boolean reducerPerBucket;

    public Cluster getCluster() {
        checkNotNull(cluster);
        return cluster;
    }

    public boolean getSaveKeys() {
        return this.saveKeys;
    }

    public boolean getReducerPerBucket() {
        return this.reducerPerBucket;
    }

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
    }

    public String getStoreName() {
        checkNotNull(storeDef);
        return storeDef.getName();
    }

    private final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }

    public int getNumChunks() {
        return this.numChunks;
    }

}
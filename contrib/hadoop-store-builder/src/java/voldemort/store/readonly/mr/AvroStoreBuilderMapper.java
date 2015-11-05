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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;

import voldemort.VoldemortException;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.avro.AvroGenericSerializer;
import voldemort.serialization.avro.versioned.AvroVersionedGenericSerializer;
import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Avro container files are not sequence input format files they contain records
 * instead of k/v pairs to consume these files we use the AvroMapper
 */
public class AvroStoreBuilderMapper extends
        AvroMapper<GenericData.Record, Pair<ByteBuffer, ByteBuffer>> implements JobConfigurable {

    protected BuildAndPushMapper mapper = new BuildAndPushMapper();
    private AvroCollectorWrapper collectorWrapper = new AvroCollectorWrapper();

    protected ConsistentRoutingStrategy routingStrategy;
    protected Serializer keySerializer;
    protected Serializer valueSerializer;

    private String keySchema;
    private String valSchema;

    private String keyField;
    private String valField;

    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;

    class AvroCollectorWrapper implements AbstractCollector {
        private AvroCollector<Pair<ByteBuffer, ByteBuffer>> collector;

        public void setInnerCollector(AvroCollector<Pair<ByteBuffer, ByteBuffer>> collector) {
            if (this.collector != collector) {
                this.collector = collector;
            }
        }

        ByteBuffer keyBB, valueBB;
        Pair<ByteBuffer, ByteBuffer> pairToCollect = new Pair<ByteBuffer, ByteBuffer>(keyBB, valueBB);

        @Override
        public void collect(byte[] key, byte[] value) throws IOException {
            if (keyBB == null || keyBB.limit() < key.length) {
                keyBB = ByteBuffer.wrap(key);
            } else {
                keyBB.limit(key.length);
                keyBB.put(key);
            }
            if (valueBB == null || valueBB.limit() < value.length) {
                valueBB = ByteBuffer.wrap(value);
            } else {
                valueBB.limit(value.length);
                valueBB.put(value);
            }
            pairToCollect.set(keyBB, valueBB);
            collector.collect(pairToCollect);
        }
    }

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

        this.collectorWrapper.setInnerCollector(collector);
        this.collectorWrapper.collect(keyBytes, valBytes);
    }

    @Override
    public void configure(JobConf conf) {
        super.setConf(conf);

        this.mapper.configure(conf);

        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));

        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);
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
    }
    private StoreDefinition storeDef;

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
    }

    private final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }
}
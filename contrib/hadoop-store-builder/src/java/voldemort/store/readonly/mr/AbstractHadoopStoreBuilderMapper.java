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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;

/**
 * A base class that can be used for building voldemort read-only stores. To use
 * it you need to override the makeKey and makeValue methods which specify how
 * to construct the key and value from the values given in map().
 * 
 * The values given by makeKey and makeValue will then be serialized with the
 * appropriate voldemort Serializer.
 */

public abstract class AbstractHadoopStoreBuilderMapper<K, V> extends
        AbstractStoreBuilderConfigurable implements Mapper<K, V, BytesWritable, BytesWritable> {

    protected BuildAndPushMapper mapper = new BuildAndPushMapper();
    private HadoopCollectorWrapper collectorWrapper = new HadoopCollectorWrapper();

    class HadoopCollectorWrapper
            extends AbstractCollectorWrapper<OutputCollector<BytesWritable, BytesWritable>> {
        BytesWritable keyBW = new BytesWritable(), valueBW = new BytesWritable();

        @Override
        public void collect(byte[] key, byte[] value) throws IOException {
            keyBW.setSize(key.length);
            keyBW.set(key, 0, key.length);
            valueBW.setSize(value.length);
            valueBW.set(value, 0, value.length);
            getCollector().collect(keyBW, valueBW);
        }
    }

    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;
    protected Serializer<Object> keySerializer;
    protected Serializer<Object> valueSerializer;

    public abstract Object makeKey(K key, V value);

    public abstract Object makeValue(K key, V value);

    /**
     * Create the voldemort key and value from the input key and value and map
     * it out for each of the responsible voldemort nodes
     * 
     * The output key is the md5 of the serialized key returned by makeKey().
     * The output value is the node_id & partition_id of the responsible node
     * followed by serialized value returned by makeValue() OR if we have
     * setKeys flag on the serialized key and serialized value
     */
    public void map(K key,
                    V value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {

        byte[] keyBytes = keySerializer.toBytes(makeKey(key, value));
        byte[] valBytes = valueSerializer.toBytes(makeValue(key, value));

        this.collectorWrapper.setCollector(output);
        this.mapper.map(keyBytes, valBytes, this.collectorWrapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        super.configure(conf);
        mapper.configure(conf);

        keySerializerDefinition = getStoreDef().getKeySerializer();
        valueSerializerDefinition = getStoreDef().getValueSerializer();

        try {
            SerializerFactory factory = new DefaultSerializerFactory();

            if(conf.get("serializer.factory") != null) {
                factory = (SerializerFactory) Class.forName(conf.get("serializer.factory"))
                                                   .newInstance();
            }

            keySerializer = (Serializer<Object>) factory.getSerializer(keySerializerDefinition);
            valueSerializer = (Serializer<Object>) factory.getSerializer(valueSerializerDefinition);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}

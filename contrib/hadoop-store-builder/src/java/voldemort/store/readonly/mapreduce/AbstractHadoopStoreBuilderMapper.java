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

package voldemort.store.readonly.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.security.MessageDigest;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * A base class that can be used for building voldemort read-only stores. To use
 * it you need to override the makeKey and makeValue methods which specify how
 * to construct the key and value from the values given in map().
 * 
 * The values given by makeKey and makeValue will then be serialized with the
 * appropriate voldemort Serializer.
 * 
 * 
 */
public abstract class AbstractHadoopStoreBuilderMapper<K, V> extends
        Mapper<K, V, BytesWritable, BytesWritable> {

    private MessageDigest md5er;
    private ConsistentRoutingStrategy routingStrategy;
    private Serializer<Object> keySerializer;
    private Serializer<Object> valueSerializer;
    private CompressionStrategy valueCompressor;
    private CompressionStrategy keyCompressor;
    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;
    private boolean saveKeys;

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
    @Override
    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        byte[] keyBytes = keySerializer.toBytes(makeKey(key, value));
        byte[] valBytes = valueSerializer.toBytes(makeValue(key, value));

        // compress key and values if required
        if(keySerializerDefinition.hasCompression()) {
            keyBytes = keyCompressor.deflate(keyBytes);
        }

        if(valueSerializerDefinition.hasCompression()) {
            valBytes = valueCompressor.deflate(valBytes);
        }

        // Generate partition and node list this key is destined for
        List<Integer> partitionList = routingStrategy.getPartitionList(keyBytes);
        Node[] partitionToNode = routingStrategy.getPartitionToNode();

        byte[] outputValue;
        BytesWritable outputKey;
        if(saveKeys) {
            // 4 ( for node id ) + 4 ( partition id ) + 4 ( value size ) + 4 (
            // key size )
            outputValue = new byte[valBytes.length + keyBytes.length + 4 + 4 + 4 + 4];

            // copy key
            ByteUtils.writeInt(outputValue, keyBytes.length, 4 + 4);
            System.arraycopy(keyBytes, 0, outputValue, 4 + 4 + 4, keyBytes.length);

            // copy value
            ByteUtils.writeInt(outputValue, valBytes.length, 4 + 4 + 4 + keyBytes.length);
            System.arraycopy(valBytes,
                             0,
                             outputValue,
                             4 + 4 + 4 + keyBytes.length + 4,
                             valBytes.length);

            // generate key - upper 8 bytes of 16 byte md5
            outputKey = new BytesWritable(ByteUtils.copy(md5er.digest(keyBytes),
                                                         0,
                                                         2 * ByteUtils.SIZE_OF_INT));

        } else {
            // 4 ( for node id ) + 4 ( partition id )
            outputValue = new byte[valBytes.length + 4 + 4];
            System.arraycopy(valBytes, 0, outputValue, 8, valBytes.length);

            // generate key - 16 byte md5
            outputKey = new BytesWritable(md5er.digest(keyBytes));
        }

        for(Integer partition: partitionList) {
            ByteUtils.writeInt(outputValue, partitionToNode[partition].getId(), 0);
            ByteUtils.writeInt(outputValue, partition, 4);
            BytesWritable outputVal = new BytesWritable(outputValue);

            context.write(outputKey, outputVal);
        }
        md5er.reset();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setup(Context context) {

        Cluster cluster = new ClusterMapper().readCluster(new StringReader(context.getConfiguration()
                                                                                  .get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(context.getConfiguration()
                                                                                                             .get("stores.xml")));
        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");

        StoreDefinition storeDef = storeDefs.get(0);

        md5er = ByteUtils.getDigest("md5");
        keySerializerDefinition = storeDef.getKeySerializer();
        valueSerializerDefinition = storeDef.getValueSerializer();
        saveKeys = context.getConfiguration().getBoolean("save.keys", false);

        try {
            SerializerFactory factory = new DefaultSerializerFactory();

            keySerializer = (Serializer<Object>) factory.getSerializer(keySerializerDefinition);
            valueSerializer = (Serializer<Object>) factory.getSerializer(valueSerializerDefinition);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
        valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());

        routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                        storeDef.getReplicationFactor());
    }
}

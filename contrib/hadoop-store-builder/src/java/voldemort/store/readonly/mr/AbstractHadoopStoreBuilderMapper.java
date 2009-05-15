package voldemort.store.readonly.mr;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.utils.ByteUtils;

/**
 * Mapper reads input data and translates it into data serialized with the
 * appropriate Serializer for the given store. Override makeKey() and
 * makeValue() to create the appropriate objects to pass into the Serializer.
 * 
 * This mapper expects the store name to be defined by the property
 * voldemort.store.name, and it expects to find distributed cache files
 * cluster.xml and stores.xml.
 * 
 * @author bbansal, jay
 * 
 */
public abstract class AbstractHadoopStoreBuilderMapper<K, V> extends HadoopStoreBuilderBase
        implements Mapper<K, V, BytesWritable, BytesWritable> {

    private ConsistentRoutingStrategy routingStrategy;
    private Serializer<Object> keySerializer;
    private Serializer<Object> valueSerializer;

    public abstract Object makeKey(K key, V value);

    public abstract Object makeValue(K key, V value);

    /**
     * Create the voldemort key and value from the input key and value and map
     * it out for each of the responsible voldemort nodes
     * 
     * The output key is the md5 of the serialized key returned by makeKey().
     * The output value is the nodeid of the responsible node followed by
     * serialized value returned by makeValue().
     */
    public void map(K key,
                    V value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {
        byte[] keyBytes = keySerializer.toBytes(makeKey(key, value));
        byte[] valBytes = valueSerializer.toBytes(makeValue(key, value));

        List<Node> nodes = routingStrategy.routeRequest(keyBytes);
        for(Node node: nodes) {
            ByteArrayOutputStream versionedValue = new ByteArrayOutputStream(keyBytes.length + 4);
            DataOutputStream valueStream = new DataOutputStream(versionedValue);
            valueStream.writeInt(node.getId());
            valueStream.write(valBytes);
            valueStream.close();
            BytesWritable outputKey = new BytesWritable(ByteUtils.md5(keyBytes));
            BytesWritable outputVal = new BytesWritable(versionedValue.toByteArray());

            output.collect(outputKey, outputVal);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        super.configure(conf);
        keySerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(getStoreDef().getKeySerializer());
        valueSerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(getStoreDef().getValueSerializer());

        routingStrategy = new ConsistentRoutingStrategy(getCluster().getNodes(),
                                                        getStoreDef().getReplicationFactor());
    }

}

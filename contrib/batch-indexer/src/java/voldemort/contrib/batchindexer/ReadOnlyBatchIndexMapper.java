package voldemort.contrib.batchindexer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.utils.ContribUtils;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;

/**
 * Mapper code for Read-Only store batch Indexer Reads following properties from
 * JobConf
 * <p>
 * <li><strong>voldemort.store.name </strong></li>
 * <li><strong>voldemort.store.version </strong></li>
 * <p>
 * Assumes Distributed cache have files with names
 * <li><strong>cluster.xml </strong></li>
 * <li><strong>stores.xml</strong></li>
 * 
 * @author bbansal
 * 
 */
public abstract class ReadOnlyBatchIndexMapper<K, V> implements
        Mapper<K, V, BytesWritable, BytesWritable> {

    private Cluster cluster = null;
    private StoreDefinition storeDef = null;
    private ConsistentRoutingStrategy routingStrategy = null;
    private Serializer<Object> keySerializer;
    private Serializer<Object> valueSerializer;

    public abstract Object getKeyBytes(K key, V value);

    public abstract Object getValueBytes(K key, V value);

    public void map(K key,
                    V value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {
        byte[] keyBytes = keySerializer.toBytes(getKeyBytes(key, value));
        byte[] valBytes = valueSerializer.toBytes(getValueBytes(key, value));

        List<Node> nodes = routingStrategy.routeRequest(keyBytes);
        for(Node node: nodes) {
            ByteArrayOutputStream versionedValue = new ByteArrayOutputStream();
            DataOutputStream valueDin = new DataOutputStream(versionedValue);
            valueDin.writeInt(node.getId());
            valueDin.write(valBytes);
            valueDin.close();
            BytesWritable outputKey = new BytesWritable(ByteUtils.md5(keyBytes));
            BytesWritable outputVal = new BytesWritable(versionedValue.toByteArray());

            output.collect(outputKey, outputVal);
        }
    }

    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {

        try {

            // get the voldemort cluster.xml and store.xml files.
            String clusterFilePath = ContribUtils.getFileFromCache(conf, "cluster.xml");
            String storeFilePath = ContribUtils.getFileFromCache(conf, "store.xml");

            if(null == clusterFilePath || null == storeFilePath) {
                throw new RuntimeException("Mapper expects cluster.xml / stores.xml passed through Distributed cache.");
            }

            // get Cluster and Store details
            cluster = ContribUtils.getVoldemortClusterDetails(clusterFilePath);
            storeDef = ContribUtils.getVoldemortStoreDetails(storeFilePath,
                                                             conf.get("voldemort.store.name"));

            keySerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(storeDef.getKeySerializer());
            valueSerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(storeDef.getValueSerializer());

            routingStrategy = new ConsistentRoutingStrategy(cluster.getNodes(),
                                                            storeDef.getReplicationFactor());

            if(routingStrategy == null) {
                throw new RuntimeException("Failed to create routing strategy");
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {}
}

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
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.contrib.utils.ContribUtils;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
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

    private static Logger logger = Logger.getLogger(ReadOnlyBatchIndexMapper.class);
    private Cluster _cluster = null;
    private StoreDefinition _storeDef = null;
    private ConsistentRoutingStrategy _routingStrategy = null;
    private Serializer<Object> _keySerializer;
    private Serializer<Object> _valueSerializer;

    public abstract Object getKeyBytes(K key, V value);

    public abstract Object getValueBytes(K key, V value);

    public void map(K key,
                    V value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {
        byte[] keyBytes = _keySerializer.toBytes(getKeyBytes(key, value));
        byte[] valBytes = _valueSerializer.toBytes(getValueBytes(key, value));

        List<Node> nodes = _routingStrategy.routeRequest(keyBytes);
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

    public void configure(JobConf conf) {

        try {

            // get the voldemort cluster.xml and store.xml files.
            String clusterFilePath = ContribUtils.getFileFromCache(conf, "cluster.xml");
            String storeFilePath = ContribUtils.getFileFromCache(conf, "store.xml");

            if(null == clusterFilePath || null == storeFilePath) {
                throw new RuntimeException("Mapper expects cluster.xml / stores.xml passed through Distributed cache.");
            }

            // get Cluster and Store details
            _cluster = ContribUtils.getVoldemortClusterDetails(clusterFilePath);
            _storeDef = ContribUtils.getVoldemortStoreDetails(storeFilePath,
                                                              conf.get("voldemort.store.name"));

            SerializerDefinition serDef = new SerializerDefinition("string", "UTF-8");

            _keySerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(_storeDef.getKeySerializer());
            _valueSerializer = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(_storeDef.getValueSerializer());

            _routingStrategy = new ConsistentRoutingStrategy(_cluster.getNodes(),
                                                             _storeDef.getReplicationFactor());

            if(_routingStrategy == null) {
                throw new RuntimeException("Failed to create routing strategy");
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {}
}

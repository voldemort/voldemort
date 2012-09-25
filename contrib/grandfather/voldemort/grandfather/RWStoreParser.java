package voldemort.grandfather;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class RWStoreParser {

    private final static Logger _log = Logger.getLogger(RWStoreParser.class);

    protected RoutingStrategy routingStrategy;
    protected Serializer<Object> keySerializer;
    protected Serializer<Object> valueSerializer;
    protected CompressionStrategy valueCompressor;
    protected CompressionStrategy keyCompressor;
    protected SerializerDefinition keySerializerDefinition;
    protected SerializerDefinition valueSerializerDefinition;
    protected int vectorNodeId;
    protected long vectorNodeVersion, jobStartTime;
    protected VectorClock vectorClock;
    protected HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeIdToRequest;
    protected boolean verbose = false;

    public abstract Object makeKey(BytesWritable key, BytesWritable value);

    public abstract Object makeValue(BytesWritable key, BytesWritable value);

    public void nextKeyValue(BytesWritable key, BytesWritable value) throws IOException,
            InterruptedException {
        Object keyObject = makeKey(key, value);
        Object valueObject = makeValue(key, value);

        if(keyObject == null || valueObject == null) {
            if(verbose) {
                _log.info("key or value created by store parser was null");
            }
            return;
        }

        if(verbose) {
            _log.info("key = " + keyObject);
            _log.info("value = " + valueObject);
        }
        
        byte[] keyBytes = keySerializer.toBytes(keyObject);
        byte[] valueBytes = valueSerializer.toBytes(valueObject);

        // compress key and values if required
        if(keySerializerDefinition.hasCompression()) {
            keyBytes = keyCompressor.deflate(keyBytes);
        }

        if(valueSerializerDefinition.hasCompression()) {
            valueBytes = valueCompressor.deflate(valueBytes);
        }

        List<Node> nodeList = routingStrategy.routeRequest(keyBytes);

        // Insert the elements in the respective blocking queues
        for(Node node: nodeList) {
            // Generate vector clock
            List<ClockEntry> versions = new ArrayList<ClockEntry>();
            if(vectorNodeId < 0) {
                // Use master node
                versions.add(0, new ClockEntry((short) nodeList.get(0).getId(), vectorNodeVersion));
            } else {
                // Use node id specified
                versions.add(0, new ClockEntry((short) vectorNodeId, vectorNodeVersion));
            }

            vectorClock = new VectorClock(versions, jobStartTime);

            ByteArray outputKey = new ByteArray(keyBytes);
            Versioned<byte[]> outputValue = Versioned.value(valueBytes, vectorClock);
            Pair<ByteArray, Versioned<byte[]>> pair = Pair.create(outputKey, outputValue);
            if(verbose) {
                _log.info("Putting pair in nodeIdToRequest for " + node.getId());
            }
            nodeIdToRequest.get(node.getId()).put(pair);
        }
    }

    public void configure(StoreDefinition storeDef,
                          Cluster cluster,
                          int nodeId,
                          long nodeVersion,
                          long jobStartTime,
                          HashMap<Integer, SynchronousQueue<Pair<ByteArray, Versioned<byte[]>>>> nodeIdToRequest,
                          boolean verbose) {
        this.keySerializerDefinition = storeDef.getKeySerializer();
        this.valueSerializerDefinition = storeDef.getValueSerializer();

        try {
            SerializerFactory factory = new DefaultSerializerFactory();
            this.keySerializer = (Serializer<Object>) factory.getSerializer(this.keySerializerDefinition);
            this.valueSerializer = (Serializer<Object>) factory.getSerializer(this.valueSerializerDefinition);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        this.keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
        this.valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());

        RoutingStrategyFactory factory = new RoutingStrategyFactory();
        this.routingStrategy = factory.updateRoutingStrategy(storeDef, cluster);

        this.vectorNodeId = nodeId;
        this.vectorNodeVersion = nodeVersion;
        this.jobStartTime = jobStartTime;
        this.nodeIdToRequest = nodeIdToRequest;
        this.verbose = verbose;
    }

}

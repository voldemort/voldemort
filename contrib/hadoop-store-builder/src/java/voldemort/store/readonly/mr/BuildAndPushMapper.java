package voldemort.store.readonly.mr;

import org.apache.hadoop.mapred.JobConf;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteUtils;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

/**
 * A class which encapsulates the serialization logic for data shuffled
 * between Mappers and Reducers of a Build and Push job.
 *
 * The actual mapper classes leveraging this code include:
 * - {@link AvroStoreBuilderMapper}
 * - {@link AbstractHadoopStoreBuilderMapper}
 *
 * The above classes use this one by composition, since they need to extend
 * some specific classes in order to be fed records by the MapReduce framework.
 */
public class BuildAndPushMapper extends AbstractStoreBuilderConfigurable {
    protected MessageDigest md5er;
    protected ConsistentRoutingStrategy routingStrategy;
    private CompressionStrategy valueCompressor;
    private CompressionStrategy keyCompressor;
    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        super.configure(conf);

        md5er = ByteUtils.getDigest("md5");
        keySerializerDefinition = getStoreDef().getKeySerializer();
        valueSerializerDefinition = getStoreDef().getValueSerializer();

        keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
        valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());

        routingStrategy = new ConsistentRoutingStrategy(getCluster(),
                getStoreDef().getReplicationFactor());
    }

    /**
     * Create the voldemort key and value from the input key and value and map
     * it out for each of the responsible voldemort nodes
     *
     * The output key is the md5 of the serialized key returned by makeKey().
     * The output value is the node_id & partition_id of the responsible node
     * followed by serialized value returned by makeValue() OR if we have
     * setKeys flag on the serialized key and serialized value
     */
    public void map(byte[] keyBytes, byte[] valBytes, AbstractCollectorWrapper collector) throws IOException {
        // Compress key and values if required
        if(keySerializerDefinition.hasCompression()) {
            keyBytes = keyCompressor.deflate(keyBytes);
        }

        if(valueSerializerDefinition.hasCompression()) {
            valBytes = valueCompressor.deflate(valBytes);
        }

        // Get the output byte arrays ready to populate
        byte[] outputValue;
        byte[] outputKey;

        // Leave initial offset for (a) node id (b) partition id
        // since they are written later
        int offsetTillNow = 2 * ByteUtils.SIZE_OF_INT;

        if(getSaveKeys()) {

            // In order - 4 ( for node id ) + 4 ( partition id ) + 1 ( replica
            // type - primary | secondary | tertiary... ] + 4 ( key size )
            // size ) + 4 ( value size ) + key + value
            outputValue = new byte[valBytes.length + keyBytes.length + ByteUtils.SIZE_OF_BYTE + 4
                    * ByteUtils.SIZE_OF_INT];

            // Write key length - leave byte for replica type
            offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            ByteUtils.writeInt(outputValue, keyBytes.length, offsetTillNow);

            // Write value length
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            ByteUtils.writeInt(outputValue, valBytes.length, offsetTillNow);

            // Write key
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            System.arraycopy(keyBytes, 0, outputValue, offsetTillNow, keyBytes.length);

            // Write value
            offsetTillNow += keyBytes.length;
            System.arraycopy(valBytes, 0, outputValue, offsetTillNow, valBytes.length);

            // Generate MR key - upper 8 bytes of 16 byte md5
            outputKey = ByteUtils.copy(md5er.digest(keyBytes),
                    0,
                    2 * ByteUtils.SIZE_OF_INT);

        } else {

            // In order - 4 ( for node id ) + 4 ( partition id ) + value
            outputValue = new byte[valBytes.length + 2 * ByteUtils.SIZE_OF_INT];

            // Write value
            System.arraycopy(valBytes, 0, outputValue, offsetTillNow, valBytes.length);

            // Generate MR key - 16 byte md5
            outputKey = md5er.digest(keyBytes);

        }

        // Generate partition and node list this key is destined for
        List<Integer> partitionList = routingStrategy.getPartitionList(keyBytes);
        Node[] partitionToNode = routingStrategy.getPartitionToNode();

        // In buildPrimaryReplicasOnly mode, we want to push out no more than a single replica
        // for each key. Otherwise (in vintage mode), we push out one copy per replica.
        int numberOfReplicasToPushTo = getBuildPrimaryReplicasOnly() ? 1 : partitionList.size();

        for(int replicaType = 0; replicaType < numberOfReplicasToPushTo; replicaType++) {

            // Node id
            ByteUtils.writeInt(outputValue,
                               partitionToNode[partitionList.get(replicaType)].getId(),
                               0);

            if(getSaveKeys()) {
                // Primary partition id
                ByteUtils.writeInt(outputValue, partitionList.get(0), ByteUtils.SIZE_OF_INT);

                // Replica type
                ByteUtils.writeBytes(outputValue,
                                     replicaType,
                                     2 * ByteUtils.SIZE_OF_INT,
                                     ByteUtils.SIZE_OF_BYTE);
            } else {
                // Partition id
                ByteUtils.writeInt(outputValue,
                                   partitionList.get(replicaType),
                                   ByteUtils.SIZE_OF_INT);
            }

            collector.collect(outputKey, outputValue);
        }
        md5er.reset();
    }
}

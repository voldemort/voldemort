package voldemort.store.routed;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import voldemort.VoldemortException;
import voldemort.client.TimeoutConfig;
import voldemort.client.ZoneAffinity;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.common.VoldemortOpCode;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreRequest;
import voldemort.store.StoreUtils;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.action.AbstractConfigureNodes;
import voldemort.store.routed.action.PerformParallelRequests;
import voldemort.store.routed.action.PerformSerialRequests;
import voldemort.store.routed.action.PerformZoneSerialRequests;
import voldemort.store.routed.action.ReadRepair;
import voldemort.store.slop.Slop;
import voldemort.store.venice.OperationType;
import voldemort.store.venice.VeniceMessage;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.SystemTime;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Class which writes to Kafka as a part of the Venice propagation layer,
 * and uses PipelineRoutedStore for reads
 */
public class KafkaRoutedStore extends PipelineRoutedStore {

    private Producer<ByteArray, VeniceMessage> producer;

    private SerializerDefinition valueSerializerDef;

    public KafkaRoutedStore(Map<Integer, Store<ByteArray, byte[], byte[]>> innerStores,
                               Map<Integer, NonblockingStore> nonblockingStores,
                               Map<Integer, Store<ByteArray, Slop, byte[]>> slopStores,
                               Map<Integer, NonblockingStore> nonblockingSlopStores,
                               Cluster cluster,
                               StoreDefinition storeDef,
                               FailureDetector failureDetector,
                               boolean repairReads,
                               TimeoutConfig timeoutConfig,
                               int clientZoneId,
                               boolean isJmxEnabled,
                               String identifierString,
                               ZoneAffinity zoneAffinity) {
        super(innerStores,
                nonblockingStores,
                slopStores,
                nonblockingSlopStores,
                cluster,
                storeDef,
                failureDetector,
                repairReads,
                timeoutConfig,
                clientZoneId,
                isJmxEnabled,
                identifierString,
                zoneAffinity);

        this.producer = getKafkaProducer(storeDef.getKafkaTopic().getBrokerListString());
        this.valueSerializerDef = storeDef.getValueSerializer();
    }

    private Producer<ByteArray, VeniceMessage> getKafkaProducer(String metadataBrokerList) {

        Properties kafkaProducerProperties = new Properties();

        kafkaProducerProperties.setProperty("metadata.broker.list", metadataBrokerList);

        // set custom serializer for key and value
        kafkaProducerProperties.setProperty("key.serializer.class", "voldemort.store.venice.VeniceKeySerializer");
        kafkaProducerProperties.setProperty("serializer.class", "voldemort.store.venice.VeniceSerializer");

        // set custom partitioner
        kafkaProducerProperties.setProperty("partitioner.class", "voldemort.routing.ConsistentRoutingStrategy");

        ProducerConfig config = new ProducerConfig(kafkaProducerProperties);
        return new Producer<ByteArray, VeniceMessage>(config);

    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {

        // clfung: Why does SerializerDefinition return an int when the value is later put in a ByteArray?
        int schema = valueSerializerDef.hasSchemaInfo() ?
                valueSerializerDef.getCurrentSchemaVersion() : VeniceMessage.DEFAULT_SCHEMA_VERSION;

        VeniceMessage vm = new VeniceMessage(OperationType.PUT, versioned.getValue(), schema);

        // TODO: when partial puts are enabled, insert the partial put byte (1) and the sub-schema id.
        ByteArray kafkaKey = new ByteArray(VeniceMessage.FULL_OPERATION_BYTEARRAY).append(key);

        KeyedMessage<ByteArray, VeniceMessage> message
                = new KeyedMessage<ByteArray, VeniceMessage>(storeDef.getKafkaTopic().getName(), kafkaKey, vm);
        producer.send(message);

    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {

        VeniceMessage vm = new VeniceMessage(OperationType.DELETE);
        ByteArray kafkaKey = new ByteArray(VeniceMessage.FULL_OPERATION_BYTEARRAY).append(key);

        KeyedMessage<ByteArray, VeniceMessage> message
                = new KeyedMessage<ByteArray, VeniceMessage>(storeDef.getKafkaTopic().getName(), kafkaKey, vm);
        producer.send(message);
        return true;
    }

    @Override
    public List<Versioned<byte[]>> get(final ByteArray key, final byte[] transforms) {
        return super.get(key, transforms, timeoutConfig.getOperationTimeout(VoldemortOpCode.GET_OP_CODE));
    }
}

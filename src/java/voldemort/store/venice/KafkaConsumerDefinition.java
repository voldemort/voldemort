package voldemort.store.venice;


/**
 * Contains all the configuration information for a Kafka Consumer on a single store.
 * Read from the stores.xml files
 *
 */
public class KafkaConsumerDefinition {

    private int kafkaPartitionReplicaCount;
    private String kafkaTopicName;

    public KafkaConsumerDefinition(String kafkaTopicName, int kafkaPartitionReplicaCount) {
        this.kafkaTopicName = kafkaTopicName;
        this.kafkaPartitionReplicaCount = kafkaPartitionReplicaCount;
    }

    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

}

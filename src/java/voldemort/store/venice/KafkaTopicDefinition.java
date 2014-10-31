package voldemort.store.venice;


/**
 * Contains all the configuration information for a Kafka Consumer on a single store.
 * Read from the stores.xml files
 *
 */
public class KafkaTopicDefinition {

    private int kafkaPartitionReplicaCount;
    private String name;

    public KafkaTopicDefinition(String kafkaTopicName, int kafkaPartitionReplicaCount) {
        this.name = kafkaTopicName;
        this.kafkaPartitionReplicaCount = kafkaPartitionReplicaCount;
    }

    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    public String getName() {
        return name;
    }

}

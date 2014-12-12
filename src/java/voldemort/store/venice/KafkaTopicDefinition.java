package voldemort.store.venice;

/**
 * Contains all the configuration information for a Kafka Consumer on a single store.
 * Read from the stores.xml files
 *
 */
public class KafkaTopicDefinition {

    private String name;
    private int kafkaPartitionReplicaCount;
    private String brokerListString;

    public KafkaTopicDefinition(String kafkaTopicName, int kafkaPartitionReplicaCount, String brokerListString) {
        this.name = kafkaTopicName;
        this.kafkaPartitionReplicaCount = kafkaPartitionReplicaCount;
        this.brokerListString = brokerListString;
    }

    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    public String getName() {
        return name;
    }

    public String getBrokerListString() {
        return brokerListString;
    }

}

package voldemort.store.venice;

import voldemort.utils.Props;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Class which stores configurable options which are specific to Kafka Consumer tasks
 */
public class KafkaConsumerConfig {

    // tuning variables
    private int numberOfRetriesBeforeFailure;
    private int requestTimeout;
    private int requestFetchSize;
    private int requestBufferSize;

    private List<String> kafkaBrokers;
    private int kafkaBrokerPort;
    private int kafkaPartitionReplicaCount;
    private int kafkaPartitionNumThreads;
    private String kafkaTopicName;

    public KafkaConsumerConfig(Properties properties) {
        this(new Props(properties));
    }

    public KafkaConsumerConfig(Props props) {

        // constants used in kafka consumption
        numberOfRetriesBeforeFailure = props.getInt("venice.kafka.consumer.num.retries", 3);
        requestTimeout = props.getInt("venice.kafka.consumer.request.timeout", 100000);
        requestFetchSize = props.getInt("venice.kafka.consumer.request.fetch.size", 100000);
        requestBufferSize = props.getInt("venice.kafka.consumer.request.buffer.size", 64 * 1024);

        // for connections to Kafka
        kafkaBrokers = props.getList("venice.kafka.broker", Arrays.asList("localhost"));
        kafkaBrokerPort = props.getInt("venice.kafka.broker.port", 9092);
        kafkaTopicName = props.getString("venice.kafka.topic.name", "default_topic");

        // for venice layer routing
        kafkaPartitionReplicaCount = props.getInt("venice.propagation.replication", 5);
        kafkaPartitionNumThreads = props.getInt("venice.propagation.threads.per.partition", 1);

    }

    /**
     * On a failed attempt, number of times Venice will retry before declaring a failure
     *
     * <ul>
     * <li>Property :"venice.kafka.consumer.num.retries"</li>
     * <li>Default : 3</li>
     * </ul>
     *
     * @return
     */
    public int getNumberOfRetriesBeforeFailure() {
        return numberOfRetriesBeforeFailure;
    }

    /**
     * Time in ms before a consumer request to Kafka times out.
     *
     * <ul>
     * <li>Property :"venice.kafka.consumer.request.timeout"</li>
     * <li>Default : 100000 </li>
     * </ul>
     *
     * @return
     */
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Number of bytes to retrieve in Kafka fetch requests
     *
     * <ul>
     * <li>Property :"venice.kafka.consumer.request.fetch.size"</li>
     * <li>Default : 100000 </li>
     * </ul>
     *
     * @return
     */
    public int getRequestFetchSize() {
        return requestFetchSize;
    }

    /**
     * Buffer size for Kafka fetch requests
     *
     * <ul>
     * <li>Property :"venice.kafka.consumer.request.buffer.size"</li>
     * <li>Default : 100000 </li>
     * </ul>
     *
     * @return
     */
    public int getRequestBufferSize() {
        return requestBufferSize;
    }

    /**
     * Name of the Kafka Broker Host
     *
     * <ul>
     * <li>Property :"venice.kafka.broker"</li>
     * <li>Default : "localhost"</li>
     * </ul>
     *
     * @return
     */
    public List<String> getKafkaBrokers() {
        return kafkaBrokers;
    }

    /**
     * Port on the Kafka Broker to read Kafka from
     *
     * <ul>
     * <li>Property :"venice.kafka.broker.port"</li>
     * <li>Default : 9092</li>
     * </ul>
     *
     * @return
     */
    public int getKafkaBrokerPort() {
        return kafkaBrokerPort;
    }

    /**
     * Number of Kafka Partitions to be used for the Venice Store Layer
     *
     * <ul>
     * <li>Property :"venice.propagation.replication"</li>
     * <li>Default : 2 </li>
     * </ul>
     *
     * @return
     */
    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    /**
     * Number of Threads to use for consumption in each Kafka Partition
     *
     * <ul>
     * <li>Property :"venice.propagation.threads.per.partition"</li>
     * <li>Default : 1 </li>
     * </ul>
     *
     * @return
     */
    public int getKafkaPartitionNumThreads() {
        return kafkaPartitionNumThreads;
    }

    /**
     * Name of the Kafka topic to consume from
     *
     * <ul>
     * <li>Property :"venice.kafka.topic.name"</li>
     * <li>Default : "default_topic"</li>
     * </ul>
     *
     * @return
     */
    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

}

package voldemort.store.venice;

import java.util.List;

/**
 * Contains all the configuration information for a Kafka Consumer
 *
 */
public class KafkaConsumerDefinition {

    private static final int DEFAULT_NUM_RETRIES = 3;
    private static final int DEFAULT_REQUEST_TIMEOUT = 100000;
    private static final int DEFAULT_REQUEST_FETCH_SIZE = 100000;
    private static final int DEFAULT_REQUEST_BUFFER_SIZE = 64 * 1024;
    private static final int DEFAULT_NUM_THREADS_PER_PARTITION = 1;

    // tuning variables
    private int numberOfRetriesBeforeFailure;
    private int requestTimeout;
    private int requestFetchSize;
    private int requestBufferSize;
    private int numberOfThreadsPerPartition;

    private List<String> kafkaBrokers;
    private int kafkaBrokerPort;
    private int kafkaPartitionReplicaCount;
    private String kafkaTopicName;

    public KafkaConsumerDefinition(List<String> kafkaBrokers,
                                   int kafkaBrokerPort,
                                   int kafkaPartitionReplicaCount,
                                   String kafkaTopicName) {
        this(kafkaBrokers, kafkaBrokerPort, kafkaPartitionReplicaCount, kafkaTopicName,
                DEFAULT_NUM_THREADS_PER_PARTITION, DEFAULT_NUM_RETRIES, DEFAULT_REQUEST_TIMEOUT,
                DEFAULT_REQUEST_FETCH_SIZE, DEFAULT_REQUEST_BUFFER_SIZE);
    }

    public KafkaConsumerDefinition(List<String> kafkaBrokers,
                                   int kafkaBrokerPort,
                                   int kafkaPartitionReplicaCount,
                                   String kafkaTopicName,
                                   int numberOfThreadsPerPartition,
                                   int numberOfRetriesBeforeFailure,
                                   int requestTimeout,
                                   int requestFetchSize,
                                   int requestBufferSize) {

        this.kafkaBrokers = kafkaBrokers;
        this.kafkaBrokerPort = kafkaBrokerPort;
        this.kafkaPartitionReplicaCount = kafkaPartitionReplicaCount;
        this.kafkaTopicName = kafkaTopicName;
        this.numberOfThreadsPerPartition = numberOfThreadsPerPartition;
        this.numberOfRetriesBeforeFailure = numberOfRetriesBeforeFailure;
        this.requestTimeout = requestTimeout;
        this.requestFetchSize = requestFetchSize;
        this.requestBufferSize = requestBufferSize;

    }

    public int getNumberOfRetriesBeforeFailure() {
        return numberOfRetriesBeforeFailure;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getRequestFetchSize() {
        return requestFetchSize;
    }

    public int getRequestBufferSize() {
        return requestBufferSize;
    }

    public List<String> getKafkaBrokers() {
        return kafkaBrokers;
    }

    public int getKafkaBrokerPort() {
        return kafkaBrokerPort;
    }

    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    public int getNumberOfThreadsPerPartition() {
        return numberOfThreadsPerPartition;
    }

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

}

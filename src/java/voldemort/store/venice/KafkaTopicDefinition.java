package voldemort.store.venice;


import com.google.common.collect.ImmutableList;
import kafka.cluster.Broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains all the configuration information for a Kafka Consumer on a single store.
 * Read from the stores.xml files
 *
 */
public class KafkaTopicDefinition {

    private String name;
    private int kafkaPartitionReplicaCount;
    private List<Broker> brokerList;
    private String brokerListString;

    public KafkaTopicDefinition(String kafkaTopicName, int kafkaPartitionReplicaCount, String brokerListString) {
        this.name = kafkaTopicName;
        this.kafkaPartitionReplicaCount = kafkaPartitionReplicaCount;
        this.brokerListString = brokerListString;

        // In the producer API, the broker list is given as one string,
        // a comma separated string of host:port pairs.
        List<Broker> brokerArrayList = new ArrayList<Broker>();
        String[] brokerArray = brokerListString.split(",");
        for (String brokerUrl : brokerArray) {
            String[] b = brokerUrl.split(":");
            if (b.length == 2) {
                brokerArrayList.add(new Broker(0, b[0], new Integer(b[1])));
            }
        }
        this.brokerList = ImmutableList.copyOf(brokerArrayList);
    }

    public int getKafkaPartitionReplicaCount() {
        return kafkaPartitionReplicaCount;
    }

    public String getName() {
        return name;
    }

    public List<Broker> getBrokerList() {
        return brokerList;
    }

    public String getBrokerListString() {
        return brokerListString;
    }

}

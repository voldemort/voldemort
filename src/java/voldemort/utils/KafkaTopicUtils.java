package voldemort.utils;

import kafka.cluster.Broker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Utility Functions for any Kafka-related operations
 */
public class KafkaTopicUtils {

    static final Logger logger = Logger.getLogger(KafkaTopicUtils.class.getName());

    /**
     * Converts a comma delimited list of brokers into its List<Broker> form.
     * i.e. Convert "host1:port1,host2:port2" into a list of two Broker objects.
     *
     * Note: The broker.id field of each Broker object is set to zero.
     * Do not use this list in contexts outside of the SimpleConsumer,
     * and don't pass it directly to Kafka!
     *
     * @param brokerListString A comma delimited list of colon separated broker strings
     * @return A list of Kafka Broker objects, or null if the string format is incorrect
     * */
    public static List<Broker> brokerStringToList(String brokerListString) {

        List<Broker> brokerArrayList = new ArrayList<Broker>();
        String[] brokerArray = brokerListString.split(",");

        try {
            for (String brokerUrl : brokerArray) {
                String[] hostAndPort = brokerUrl.split(":");
                if (hostAndPort.length == 2) {
                    brokerArrayList.add(new Broker(0, hostAndPort[0], new Integer(hostAndPort[1])));
                } else {
                    throw new IllegalArgumentException();
                }
            }
        } catch (Exception e) {
            logger.error("Cannot perform conversion: " + brokerListString + " is of an illegal format");
            logger.error("Expected format: host1:port1,host2:port2");
            return null; // This will cause KafkaConsumer to fail.
        }

        return brokerArrayList;
    }

    /**
     * Converts an offset value into an array of Bytes
     * */
    public static byte[] longToBytes(long offset) {

        ByteArrayOutputStream bytesOut = null;
        ObjectOutputStream oos = null;
        byte[] output = new byte[0];

        try {
            bytesOut = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bytesOut);
            oos.writeLong(offset);
            oos.flush();
            output = bytesOut.toByteArray();
        } catch (IOException e) {
            logger.error("Error while performing serialization from Long to bytes.");
            return null;
        }
        return output;
    }

    /**
     * Converts a serialized byteArray into its corresponding offset value
     * */
    public static long bytestoLong(byte[] bytes) {

        ByteArrayInputStream bytesIn = null;
        ObjectInputStream ois = null;
        long output = 0;

        try {
            bytesIn = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bytesIn);
            output = ois.readLong();
        } catch (IOException e) {
            logger.error("Error while performing serialization from Long to bytes.");
            return -1;
        }
        return output;
    }
}

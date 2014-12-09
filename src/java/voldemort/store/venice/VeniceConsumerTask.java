package voldemort.store.venice;

import kafka.common.LeaderNotAvailableException;
import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.api.PartitionMetadata;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.VerifiableProperties;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import voldemort.serialization.SerializerDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;


/**
 * Runnable class which performs Kafka consumption from the Simple Consumer API.
 * Consumption is performed on a single, defined Kafka partition
 */
public class VeniceConsumerTask implements Runnable {

    static final Logger logger = Logger.getLogger(VeniceConsumerTask.class.getName());

    private final String LEADER_ELECTION_TASK_NAME = "Voldemort_Venice_LeaderLookup";
    private final int FIND_LEADER_CYCLE_DELAY = 2000;
    private final int READ_CYCLE_DELAY = 500;

    private String storeName;
    private VeniceConsumerConfig veniceConsumerConfig;

    // schema validation
    private SerializerDefinition valueSerializer;

    // kafka metadata
    private String topic;
    private int partition;
    private List<Broker> seedBrokers;
    private List<Broker> replicaBrokers;
    private String consumerClientName;

    // offset management
    private long startingOffset;

    // speed and throttling
    private VeniceConsumerState consumerState;

    // serialization
    private VeniceStore store;

    public VeniceConsumerTask(VeniceStore store,
                              List<Broker> seedBrokers,
                              String topic,
                              int partition,
                              long startingOffset,
                              SerializerDefinition valueSerializer,
                              VeniceConsumerConfig veniceConsumerConfig) {

        this.store = store;
        this.storeName = store.getName();

        this.veniceConsumerConfig = veniceConsumerConfig;

        this.valueSerializer = valueSerializer;

        // consumer metadata
        this.replicaBrokers = new ArrayList<Broker>();
        this.startingOffset = startingOffset;
        this.consumerState = VeniceConsumerState.RUNNING;

        // consumer configurables
        this.partition = partition;
        this.topic = topic;
        this.seedBrokers = seedBrokers;

        // a unique client name for Kafka debugging
        this.consumerClientName = "Voldemort_Venice_" + storeName + "_" + topic + "_" + partition;

    }

    /**
     *  Parallelized method which performs Kafka consumption and relays messages to the Store
     * */
    public void run() {

        SimpleConsumer consumer = null;

        try {

            // find the meta data
            PartitionMetadata metadata = findLeader(seedBrokers, topic, partition);
            validateConsumerMetadata(metadata);

            Broker leadBroker = metadata.leader().get();
            consumer = new SimpleConsumer(leadBroker.host(),
                    leadBroker.port(),
                    veniceConsumerConfig.getRequestTimeout(),
                    veniceConsumerConfig.getRequestBufferSize(),
                    consumerClientName);

            // read from the last available offset if not given
            long readOffset = (startingOffset == -1) ? getLastOffset(consumer,
                    topic,
                    partition,
                    kafka.api.OffsetRequest.LatestTime(),
                    consumerClientName) : startingOffset;

            // execute this thread infinitely until a Venice exception is thrown
            while (true) {

                if (null == consumer) {
                    consumer = new SimpleConsumer(leadBroker.host(),
                            leadBroker.port(),
                            veniceConsumerConfig.getRequestTimeout(),
                            veniceConsumerConfig.getRequestBufferSize(),
                            consumerClientName);
                }

                long numReadsInIteration = 0;

                // flag that may be set to false if consumer is being throttled/paused
                if (consumerState != VeniceConsumerState.PAUSED) {

                    Iterator<MessageAndOffset> messageAndOffsetIterator = null;
                    try {

                        messageAndOffsetIterator = getMessageAndOffsetIterator(leadBroker, consumer, readOffset);
                        while (messageAndOffsetIterator.hasNext()) {

                            MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
                            long currentOffset = messageAndOffset.offset();

                            // Due to Kafka compression, fetch request may return a bulk of messages,
                            // including some that have already be read. Thus, push forward to find the one we want.
                            if (currentOffset < readOffset) {
                                continue;
                            }

                            // Read message, notify and iterate
                            readMessage(messageAndOffset.message());
                            store.updatePartitionOffset(partition, currentOffset);
                            readOffset = messageAndOffset.nextOffset();
                            numReadsInIteration++;
                        }

                    } catch (LeaderNotAvailableException e) {
                        logger.error("Kafka error found! Finding new leader....");
                        logger.error(e);
                        consumer.close();
                        consumer = null;

                        try {
                            leadBroker = findNewLeader(leadBroker, topic, partition);
                        } catch (Exception leaderException) {

                            // Even leader re-election has failed, the task needs to die
                            logger.error("Error while finding new leader: " + leaderException);
                            throw new VoldemortVeniceException(leaderException);
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error(e + " Skipping inputted message.");
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipping message at: [ Topic " + topic + ", Partition "
                                    + partition + ", Offset " + readOffset + " ]");
                        }
                        // forcefully skip over this bad offset
                        readOffset++;
                    }
                }

                // Nothing was read in the last iteration, slow down and reduce load on Consumer
                // Can occur due to throttling or lack of inputs
                if (0 == numReadsInIteration) {
                    try {
                        logger.debug("Empty read cycle. Waiting....");
                        Thread.sleep(READ_CYCLE_DELAY);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Killing Consumer Task on [" + topic + ", " + partition + "]");
            logger.error(e);
            e.printStackTrace();

        } finally {

            // TODO: find a safe way to restart consumer tasks after they die
            if (consumer != null) {
                logger.warn("Closing consumer on [" + topic + ", " + partition + "]");
                consumer.close();
            }
        }
    }

    /**
     *  Returns an iterator object for the current position in the Kafka log.
     *  Handles Kafka request/response semantics
     *
     *  @param leadBroker - The current leader of the Kafka Broker
     *  @param consumer - A SimpleConsumer object tied to the Kafka instance
     *  @param readOffset - The offset in the Kafka log to begin reading from
     * */
    private Iterator<MessageAndOffset> getMessageAndOffsetIterator(Broker leadBroker,
                                                                   SimpleConsumer consumer, long readOffset) {

        FetchRequest req = new FetchRequestBuilder()
                .clientId(consumerClientName)
                .addFetch(topic, partition, readOffset, veniceConsumerConfig.getRequestFetchSize())
                .build();

        Iterator<MessageAndOffset> messageAndOffsetIterator;
        try {

            FetchResponse fetchResponse = consumer.fetch(req);

            // This is a separate case from the one below. The fetch worked but returned an error.
            if (fetchResponse.hasError()) {
                throw new Exception("FetchResponse error code: "
                        + fetchResponse.errorCode(topic, partition));
            }

            messageAndOffsetIterator = fetchResponse.messageSet(topic, partition).iterator();

        } catch (Exception e) {
            logger.error("FetchResponse could not perform fetch on [" + topic + ", " + partition + "]");
            throw new LeaderNotAvailableException(e.getMessage());
        }

        return messageAndOffsetIterator;
    }

    /**
     *  Validates that a given PartitionMetadata is valid: it is non-null and a leader is defined.
     * */
    private void validateConsumerMetadata(PartitionMetadata metadata) throws VoldemortVeniceException {

        if (null == metadata) {
            throw new VoldemortVeniceException("Cannot find metadata for ["
                    + topic + ", " + partition + "]");
        }

        if (null == metadata.leader()) {
            throw new VoldemortVeniceException("Cannot find leader for ["
                    + topic + ", " + partition + "]");
        }

    }

    private void readMessage(Message msg) {

        // Get the Venice Key
        ByteBuffer key = msg.key();
        ByteBuffer payload = msg.payload();

        if (null == key || null == payload) {
            throw new IllegalArgumentException("Received a Kafka Message with No Key.");
        }

        // Read Key
        byte[] keyBytes = new byte[key.limit()];
        key.get(keyBytes);

        // Read Payload
        byte[] payloadBytes = new byte[payload.limit()];
        payload.get(payloadBytes);

        // De-serialize payload into Venice Message format
        VeniceSerializer serializer = new VeniceSerializer(new VerifiableProperties());
        readVeniceMessage(keyBytes, serializer.fromBytes(payloadBytes));

    }

    /**
     * Given the attached store, interpret the VeniceMessage and perform the required action
     * */
    private void readVeniceMessage(byte[] key, VeniceMessage msg) throws VoldemortVeniceException {

        // Note that vector clocks are not to be used with the Venice implementation,
        // as Kafka log serves the same purpose of ordering
        ByteArray keyBytes = parseKeyForFullPut(key);
        Versioned<byte[]> versionedMessage = parsePayload(msg);

        switch (msg.getOperationType()) {
            case PUT:
                if (logger.isDebugEnabled()) {
                    logger.debug("Store: " + storeName +
                            " partition: " + partition +
                            " Putting: " + keyBytes + ", " + msg.getPayload());
                }
                store.putFromKafka(keyBytes, versionedMessage, null);
                break;

            case DELETE:
                if (logger.isDebugEnabled()) {
                    logger.debug("Store: " + storeName +
                            " partition: " + partition +
                            " Deleting: " + keyBytes + ", " + msg.getPayload());
                }
                store.deleteFromKafka(keyBytes, null);
                break;

            case PARTIAL_PUT:
                throw new IllegalArgumentException("Partial puts not yet implemented");

            case ERROR:
                throw new IllegalArgumentException("Error while creating Venice Message.");

            default:
                throw new IllegalArgumentException("Unrecognized operation type submitted: " + msg.getOperationType());
        }
    }

    /**
     * A utility function that verifies that a byte[] is a valid full put,
     * and converts it to a ByteArray.
     * @param key - An array of bytes from Kafka
     * @return A ByteArray if the array is valid, null otherwise.
     *
     * */
    private ByteArray parseKeyForFullPut(byte[] key) {
        if (VeniceMessage.FULL_OPERATION_BYTE == key[0]) {
            // remove the header once it has been verified
            key = Arrays.copyOfRange(key, 1, key.length);
            return new ByteArray(key);
        } else {
            throw new IllegalArgumentException("Received a Kafka message that is missing the correct identifier byte.");
        }
    }

    private Versioned<byte[]> parsePayload(VeniceMessage msg) {

        // check for null inputs
        if (null == msg) {
            throw new IllegalArgumentException("Given null Venice Message.");
        }

        if (null == msg.getOperationType()) {
            throw new IllegalArgumentException("Venice Message does not have operation type!");
        }

        // check for Magic Byte
        if (msg.getMagicByte() != VeniceMessage.DEFAULT_MAGIC_BYTE) {
            throw new IllegalArgumentException("Venice Message does not have correct magic byte!");
        }

        // validate schema version for PUT
        if (msg.getOperationType() == OperationType.PUT &&
                valueSerializer.hasSchemaInfo() &&
                !valueSerializer.getAllSchemaInfoVersions().containsKey(msg.getSchemaVersion())) {
            throw new IllegalArgumentException("Unrecognized schema version for value-serializer");
        }

        return new Versioned<byte[]>(msg.getPayload());
    }

    /**
     * Finds the latest offset after a given time
     * @param consumer - A SimpleConsumer object for Kafka consumption
     * @param topic - Kafka topic
     * @param partition - Partition number within the topic
     * @param whichTime - Time at which to being reading offsets
     * @param clientName - Name of the client (combination of topic + partition)
     * @return long - last offset after the given time
     * */
    public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
                                     String clientName) {

        TopicAndPartition tp = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap
                = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

        requestInfoMap.put(tp, new PartitionOffsetRequestInfo(whichTime, 1));

        // TODO: Investigate if the conversion can be done in a cleaner way
        kafka.javaapi.OffsetRequest req = new kafka.javaapi.OffsetRequest(requestInfoMap,
                kafka.api.OffsetRequest.CurrentVersion(),
                clientName);

        kafka.api.OffsetResponse scalaResponse = consumer.getOffsetsBefore(req.underlying());
        kafka.javaapi.OffsetResponse javaResponse = new kafka.javaapi.OffsetResponse(scalaResponse);

        if (javaResponse.hasError()) {
            throw new KafkaException("Error fetching data offset for [" + topic + ", " + partition + "]");
        }

        long[] offsets = javaResponse.offsets(topic, partition);
        logger.info("From Kafka metadata, partition " + partition + " last offset at: " + offsets[0]);
        store.updatePartitionOffset(partition, offsets[0]);

        return offsets[0];

    }

    /**
     * This method taken from Kafka 0.8 SimpleConsumer Example
     * Used when the lead Kafka partition dies, and the new leader needs to be elected
     * */
    private Broker findNewLeader(Broker oldLeader, String topic, int partition) throws Exception {

        for (int i = 0; i < veniceConsumerConfig.getNumberOfRetriesBeforeFailure(); i++) {

            boolean goToSleep;
            PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);

            // can't find the leader partition
            if (null == metadata || null == metadata.leader()) {
                goToSleep = true;

            // old leader is same as new leader, this may happen on the first pass if ZK is not updated yet
            } else if (oldLeader.getConnectionString().equals(metadata.leader().get().getConnectionString()) && i == 0) {
                goToSleep = true;

            } else {
                logger.warn("Electing " + metadata.leader().get() + " as new leader for ["
                        + topic + "," + partition + "].");
                return metadata.leader().get();
            }

            // Let the thread go to sleep so that ZooKeeper/Other external services can recover
            if (goToSleep) {
                try {
                    Thread.sleep(FIND_LEADER_CYCLE_DELAY);
                } catch (InterruptedException ie) {
                }
            }
        }

        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * Finds the leader for a given Kafka topic and partition
     * @param seedBrokers - List of all Kafka Brokers
     * @param topic - String name of the topic to search for
     * @param partition - Partition Number to search for
     * @return A PartitionMetadata Object for the partition found
     * */
    private PartitionMetadata findLeader(List<Broker> seedBrokers, String topic, int partition) {

        PartitionMetadata returnMetaData = null;

        loop:
        /* Iterate through all the Brokers, Topics and their Partitions */
        for (Broker broker : seedBrokers) {

            SimpleConsumer consumer = null;

            try {

                consumer = new SimpleConsumer(broker.host(),
                        broker.port(),
                        veniceConsumerConfig.getRequestTimeout(),
                        veniceConsumerConfig.getRequestBufferSize(),
                        LEADER_ELECTION_TASK_NAME);

                Seq<String> topics = JavaConversions.asScalaBuffer(Collections.singletonList(topic));
                TopicMetadataRequest request = new TopicMetadataRequest(topics, 17);
                TopicMetadataResponse resp = consumer.send(request);

                Seq<TopicMetadata> metaData = resp.topicsMetadata();
                Iterator<TopicMetadata> it = metaData.iterator();

                while (it.hasNext()) {
                    TopicMetadata item = it.next();

                    Seq<PartitionMetadata> partitionMetaData = item.partitionsMetadata();
                    Iterator<PartitionMetadata> innerIt = partitionMetaData.iterator();

                    while (innerIt.hasNext()) {
                        PartitionMetadata pm = innerIt.next();
                        if (pm.partitionId() == partition) {
                            returnMetaData = pm;
                            break loop;
                        }

                    } /* End of Partition Loop */

                } /* End of Topic Loop */

            } catch (Exception e) {

                logger.warn("Error communicating with " + broker + " to find [" + topic + ", " + partition + "]");
                logger.error(e);

            } finally {

                // safely close consumer
                if (consumer != null) {
                    consumer.close();
                }
            }

        } /* End of Broker Loop */

        if (returnMetaData != null) {

            // A leader was found; now find its replicas
            replicaBrokers.clear();

            Seq<Broker> replicasSequence = returnMetaData.replicas();
            Iterator<Broker> replicaIterator = replicasSequence.iterator();

            while (replicaIterator.hasNext()) {
                Broker replica = replicaIterator.next();
                replicaBrokers.add(new Broker(0, replica.host(), replica.port()));
            }

        }

        return returnMetaData;

    }

    public void setConsumerState(VeniceConsumerState state) {
        this.consumerState = state;
    }

    public VeniceConsumerState getConsumerState() {
        return consumerState;
    }

}

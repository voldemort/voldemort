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
import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.nio.ByteBuffer;

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

    private final String ENCODING = "UTF-8";
    private final String LEADER_ELECTION_TASK_NAME = "Voldemort_Venice_LeaderLookup";
    private final int FIND_LEADER_CYCLE_DELAY = 2000;
    private final int READ_CYCLE_DELAY = 500;

    private VeniceConsumerTuning veniceConsumerTuning;

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
    private VeniceMessage vm;
    private VeniceSerializer serializer;
    private VeniceStore store;

    public VeniceConsumerTask(VeniceStore store, List<Broker> seedBrokers, String topic, int partition,
                              long startingOffset, VeniceConsumerTuning veniceConsumerTuning) {

        this.store = store;
        this.veniceConsumerTuning = veniceConsumerTuning;

        // static serialization service for Venice Messages
        this.serializer = new VeniceSerializer(new VerifiableProperties());

        // consumer metadata
        this.replicaBrokers = new ArrayList<Broker>();
        this.startingOffset = startingOffset;
        this.consumerState = VeniceConsumerState.RUNNING;

        // consumer configurables
        this.partition = partition;
        this.topic = topic;
        this.seedBrokers = seedBrokers;

        // a unique client name for Kafka debugging
        this.consumerClientName = "Voldemort_Venice_" + topic + "_" + partition;

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
                .addFetch(topic, partition, readOffset, veniceConsumerTuning.getRequestFetchSize())
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
                    veniceConsumerTuning.getRequestTimeout(),
                    veniceConsumerTuning.getRequestBufferSize(),
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
                            veniceConsumerTuning.getRequestTimeout(),
                            veniceConsumerTuning.getRequestBufferSize(),
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
                    }
                }
                // Nothing was read in the last iteration, slow down and reduce load on Consumer
                // Can occur due to throttling or lack of inputs
                if (0 == numReadsInIteration) {
                    try {
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
        byte[] keyBytes = new byte[key.limit()];
        key.get(keyBytes);
        String keyString = new String(keyBytes);

        // Read Payload
        ByteBuffer payload = msg.payload();
        byte[] payloadBytes = new byte[payload.limit()];
        payload.get(payloadBytes);

        // De-serialize payload into Venice Message format
        vm = serializer.fromBytes(payloadBytes);

        readVeniceMessage(keyString, vm);

    }

    /**
     * Given the attached store, interpret the VeniceMessage and perform the required action
     * */
    private void readVeniceMessage(String key, VeniceMessage msg) throws VoldemortVeniceException {

        // check for invalid inputs
        if (null == msg) {
            throw new VoldemortVeniceException("Given null Venice Message.");
        }

        if (null == msg.getOperationType()) {
            throw new VoldemortVeniceException("Venice Message does not have operation type!");
        }


        // Provide an empty vector clock for all writes from Kafka
        VectorClock clock = new VectorClock();
        ByteArray voldemortKey = new ByteArray(key.getBytes());;

        switch (msg.getOperationType()) {

            // Note that vector clocks are not to be used with the Venice implementation,
            // as Kafka log serves the same purpose of ordering
            case PUT:
                logger.info("Partition: " + partition + " Putting: " + key + ", " + msg.getPayload());
                Versioned<byte[]> versionedMessage = new Versioned<byte[]>(msg.getPayload(), clock);
                store.putFromKafka(voldemortKey, versionedMessage, null);
                break;

            // deleting values
            case DELETE:
                logger.info("Partition: " + partition + " Deleting: " + key);
                store.deleteFromKafka(voldemortKey, clock);
                break;

            // partial update
            case PARTIAL_PUT:
                throw new UnsupportedOperationException("Partial puts not yet implemented");

                // error
            case ERROR:
                throw new VoldemortVeniceException("Error while creating Venice Message.");

            default:
                throw new VoldemortVeniceException("Unrecognized operation type submitted: " + msg.getOperationType());
        }

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
        logger.info("Partition " + partition + " last offset at: " + offsets[0]);
        store.updatePartitionOffset(partition, offsets[0]);

        return offsets[0];

    }

    /**
     * This method taken from Kafka 0.8 SimpleConsumer Example
     * Used when the lead Kafka partition dies, and the new leader needs to be elected
     * */
    private Broker findNewLeader(Broker oldLeader, String topic, int partition) throws Exception {

        for (int i = 0; i < veniceConsumerTuning.getNumberOfRetriesBeforeFailure(); i++) {

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
                        veniceConsumerTuning.getRequestTimeout(),
                        veniceConsumerTuning.getRequestBufferSize(),
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

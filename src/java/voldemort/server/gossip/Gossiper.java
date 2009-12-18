package voldemort.server.gossip;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author afeinberg
 * 
 *         This implements a gossip protocol for metadata. The algorithm is
 *         fairly simple. Until the service is stopped on a node, that node
 *         will:
 *         <p>
 *         <ol>
 *         <li>Select a random peer that isn't myself.</li>
 *         <li>For each key in a list of metadata keys:
 *         <p>
 *         <ol>
 *         <li>Pull the value and it's vector clock from the randomly selected
 *         peer.</li>
 *         <li>If the pulled vector clock/timestamp is newer, set my metadata
 *         for this key to the pulled value.</li>
 *         <li>If the pulled vector clock/timestamp is the same or older, do
 *         nothing.</li>
 *         <li>If the vector clocks aren't causaully related, log an error (in
 *         future, perhaps do reconciliation).</li>
 *         </ol>
 *         </p>
 *         </li>
 *         <li>Sleep for a specified interval, then go to step 1.</li>
 *         </ol>
 *         </p>
 */
public class Gossiper implements Runnable {

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();
    private final MetadataStore metadataStore;
    private final AdminClient adminClient;
    private final int gossipInterval;

    private final static Logger logger = Logger.getLogger(Gossiper.class);

    /**
     * Create a Gossiper object, to be submitted to an Executor or anything else
     * which will invoke the run method).
     * 
     * @param metadataStore The instance of {@link MetadataStore} for the
     *        specific node.
     * @param adminClient A created instance of {@link AdminClient}
     * @param gossipInterval Interval in <em>milliseconds</em> at which we want
     *        to gossip.
     */
    public Gossiper(MetadataStore metadataStore, AdminClient adminClient, int gossipInterval) {
        this.metadataStore = metadataStore;
        this.gossipInterval = gossipInterval;
        this.adminClient = adminClient;
    }

    /**
     * If the run method was to be invoked, then we should gossip.
     */
    public void start() {
        logger.info("Gossip started");
        running.set(true);
    }

    /**
     * After the current operation finishes, no longer gossip.
     */
    public void stop() {
        logger.info("Gossip stopped");
        running.set(false);
    }

    public void run() {
        while(running.get()) {
            try {
                Thread.sleep(gossipInterval);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            Node node = selectPeer();
            logger.info(metadataStore.getNodeId() + " starting gossip with " + node);
            for(String key: MetadataStore.GOSSIP_KEYS) {
                try {
                    gossip(node, key);
                } catch(VoldemortException e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * Randomly, select a node that isn't myself.
     * 
     * @return A node that isn't myself
     */
    protected Node selectPeer() {
        Cluster cluster = metadataStore.getCluster();
        int nodes = cluster.getNumberOfNodes();

        Node node;
        do {
            node = cluster.getNodeById(random.nextInt(nodes));
        } while(node.getId() == metadataStore.getNodeId());

        return node;
    }

    private void gossip(Node node, String key) {
        if (logger.isDebugEnabled())
            logger.debug(metadataStore.getNodeId() + " pulling " + key + " from " + node);

        /**
         * First check the version that we have internally and the version on
         * the remote server. Using {@link AdminClient#getRemotedata} as to
         * avoid having to convert between Versioned<byte[]> and
         * Versioned<String>
         */
        Versioned<String> remoteVersioned = adminClient.getRemoteMetadata(node.getId(), key);
        Versioned<String> localVersioned = adminClient.getRemoteMetadata(metadataStore.getNodeId(),
                                                                         key);
        Version localVersion = localVersioned.getVersion();
        Version remoteVersion = remoteVersioned.getVersion();

        switch(remoteVersion.compare(localVersion)) {
            /**
             * {@link Occured.BEFORE} can indicate two conditions. Either the
             * timestamps are the same, or one came before the other. In case
             * the localVersion occurred before remoteVersion (in terms of
             * timestamps) we want to update with remoteVersion.
             */
            case AFTER: {
                VectorClock remoteVectorClock = (VectorClock) remoteVersion;
                VectorClock localVectorClock = (VectorClock) localVersion;
                adminClient.updateRemoteMetadata(metadataStore.getNodeId(), key, remoteVersioned);
                logger.info("My " + key + " occured BEFORE the key from " + node + ". Accepted theirs.");
                break;
            }
            /**
             * Do nothing if they have older data, wait for them to pull from us.
             */
            case BEFORE: {
                break;
            }
            /**
             * {@link Occured.CONCURRENTLY} indicates that there is a
             * conflict. In this case, ideally, we should do sensible
             * reconciliation (e.g. for failure detection we should exclude
             * nodes that one machine thinks failed and the other machines
             * do not).For the sake of simplicity, right now we will just
             * log an error.
             */
            case CONCURRENTLY: {
                logger.error("My " + key + " occured CONCURRENTLY. My value: " + localVersioned + "; Their value "
                        + remoteVersioned);
                break;
            }
        }
    }
}

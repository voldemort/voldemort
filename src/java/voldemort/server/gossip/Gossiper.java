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
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author afeinberg
 * 
 *         Implementation of a gossip (epidemic) protocol for metadata. The algorithm is
 *         fairly simple. Until the service is stopped on a node, that node will:
 *
 *         <p>
 *         <ol>
 *         <li>Select a random peer that isn't myself.</li>
 *         <li>For each key in a list of metadata keys (defined in {@link voldemort.store.metadata.MetadataStore}):
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
     * Create a <code>Gossiper</code> object, to be submitted to a {@link java.util.concurrent.Executor} or any
     * other object which can invoke the {@link #run()} method.
     * 
     * @param metadataStore The instance of {@link MetadataStore} for the
     *        specific node.
     * @param adminClient A created instance of {@link AdminClient}
     * @param gossipInterval Interval in <em>milliseconds</em> at which we want
     *        to gossipKey.
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

    /**
     * Perform gossip: until receiving a shutdown signal, pick a node at random, gossip with that node
     * and then sleep for a pre-defined interval. 
     */
    public void run() {
        while(running.get()) {
            try {
                Thread.sleep(gossipInterval);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Node node = selectPeer();
            adminClient.setAdminClientCluster(metadataStore.getCluster());

            if (logger.isDebugEnabled())
                logger.debug(metadataStore.getNodeId() + " starting gossip with " + node);

            for(String key: MetadataStore.GOSSIP_KEYS) {
                try {
                    gossipKey(node, key);
                } catch(VoldemortException e) {
                    logger.error("gossipKey(): " + e);
                }
            }
        }
    }

    /**
     * Randomly select a node that isn't myself.
     * 
     * @return A node that isn't myself
     */
    private Node selectPeer() {
        Cluster cluster = metadataStore.getCluster();
        int nodes = cluster.getNumberOfNodes();
        Node node;
        do {
            node = cluster.getNodeById(random.nextInt(nodes));
        } while(node.getId() == metadataStore.getNodeId());

        return node;
    }

    private void gossipKey(Node node, String key) {
        if (logger.isDebugEnabled())
            logger.debug(metadataStore.getNodeId() + " gossiping " + key + " with " + node);

        /**
         * First check the version that we have internally and the version on
         * the remote server. Using {@link AdminClient#getRemoteMetadata(int, String)} to
         * avoid having to convert between <code>Versioned<byte[]></code> and
         * <code>Versioned<String></code>
         */
        Versioned<String> remoteVersioned = adminClient.getRemoteMetadata(node.getId(), key);
        Versioned<String> localVersioned = adminClient.getRemoteMetadata(metadataStore.getNodeId(), key);
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
                adminClient.updateRemoteMetadata(metadataStore.getNodeId(), key, remoteVersioned);
                logger.info("My " + key + " occurred BEFORE the version at " + node + ". Accepted theirs.");
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
                logger.error("My " + key + " occurred CONCURRENTLY with " + node + ". My value is " + localVersioned
                             + ", their value is " + remoteVersioned);
                break;
            }
        }
    }
}

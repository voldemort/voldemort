package voldemort.server.gossip;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author afeinberg
 *
 * Want to keep a list of keys that should be synchronized. Use vector clocks for
 * synchronization.
 */
public class Gossiper implements Runnable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();
    private final MetadataStore metadataStore;
    private final AdminClient adminClient;
    private final int gossipInterval;

    private final static Logger logger = Logger.getLogger(Gossiper.class);

    public Gossiper(MetadataStore metadataStore, AdminClient adminClient, int gossipInterval) {
        this.metadataStore = metadataStore;
        this.gossipInterval = gossipInterval;
        this.adminClient = adminClient;
    }

    public void start() {
        running.set(true);
    }
    
    public void stop() {
        running.set(false);
    }

    public void run() {
        while (running.get()) {
            Node node = selectPeer();
            logger.info("Starting gossip with " + node);
            for (String key: MetadataStore.GOSSIP_KEYS) {
                try {
                    doPull(node, key);
                } catch (VoldemortException e) {
                    logger.error(e);
                }
            }
            try {
                Thread.sleep(gossipInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Node selectPeer() {
        Cluster cluster = metadataStore.getCluster();
        int nodes = cluster.getNumberOfNodes();

        return cluster.getNodeById(random.nextInt(nodes));
    }

    private void doPull(Node node, String key) {
        Versioned<String> remoteVersioned = adminClient.getRemoteMetadata(node.getId(), key);
        List<Versioned<byte[]>> versionedList = metadataStore.get(new ByteArray(ByteUtils.getBytes(key, "UTF-8")));
        Versioned<byte[]> localVersioned = versionedList.get(0);
        Version localVersion = localVersioned.getVersion();
        Version remoteVersion = remoteVersioned.getVersion();

        switch (localVersion.compare(remoteVersion)) {
            case BEFORE: {
                metadataStore.put(key, remoteVersioned);
                logger.info("My " + key + " occured BEFORE the key from " + node + ". Accepted theirs.");
                break;
            }
            case AFTER: {
                break;
            }
            case CONCURRENTLY: {
                logger.error("My " + key + " occured CONCURRENTLY. My value: " + localVersioned + "; Their value " +
                        remoteVersioned);
                break;
            }
        }
    }
}

/*
 * Copyright 2008-2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.gossip;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.Versioned;

/**
 * Implementation of a Gossip protocol for metadata. Periodically, choose a
 * random peer and Gossip for all metadata keys specified in
 * {@link voldemort.store.metadata.MetadataStore#GOSSIP_KEYS} with that peer.
 * <p>
 * Gossip between nodes A and B for a metadata key K starts with node A
 * retrieving the key K and its vector clock from node B. The retrieved vector
 * clock is then compared with the local vector clock for K. If the value at
 * node B is determined to have come after the value at node A, node A will
 * accept the value at node B. If the value at node B is determined to have come
 * before the value at node A, node A will do nothing and allow node B to
 * initiate Gossip. If the two vector clocks are found to be concurrent i.e.,
 * <em>causally unrelated</em>, an error is logged.
 * </p>
 */
public class Gossiper implements Runnable {

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();
    private final MetadataStore metadataStore;
    private final AdminClient adminClient;
    private final int gossipInterval;

    private final static Logger logger = Logger.getLogger(Gossiper.class);

    /**
     * Create a <code>Gossiper</code> object, which implements {@link Runnable}
     * allowing it to be run as a thread or be submitted to an Executor.
     * 
     * @param metadataStore The instance of
     *        {@link voldemort.store.metadata.MetadataStore} for the local node.
     * @param adminClient Instance of
     *        {@link voldemort.client.protocol.admin.AdminClient}
     * @param gossipInterval Interval in <em>milliseconds</em> at which we want
     *        to gossip.
     */
    public Gossiper(MetadataStore metadataStore, AdminClient adminClient, int gossipInterval) {
        this.metadataStore = metadataStore;
        this.gossipInterval = gossipInterval;
        this.adminClient = adminClient;
    }

    /**
     * Allow Gossip to start if {@link #run()} is invoked.
     */
    public void start() {
        logger.info("Gossip started");
        running.set(true);
    }

    /**
     * After the current operation finishes, stop Gossip.
     */
    public void stop() {
        logger.info("Gossip stopped");
        running.set(false);
    }

    /**
     * Perform Gossip: until receiving a shutdown signal, pick a peer at random,
     * Gossip all keys listed in
     * {@link voldemort.store.metadata.MetadataStore#GOSSIP_KEYS} with that peer
     * and then sleep for a specified interval.
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

            if(logger.isDebugEnabled())
                logger.debug("Starting gossip with " + node);

            for(String key: MetadataStore.GOSSIP_KEYS) {
                try {
                    gossipKey(node, key);
                } catch(VoldemortException e) {
                    logger.warn("Unable to Gossip key " + key + " with " + node, e);
                }
            }
        }
    }

    /**
     * Randomly select a distinct peer. Method is <code>protected</code> rather
     * than <code>private</code>, so that it may be overridden if peer selection
     * logic is to be changed e.g., to add datacenter/rack awareness.
     * 
     * @return Peer for Gossip.
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

    /**
     * Perform Gossip on a specified metadata key with a remote node. As
     * metadata is data, vector clocks are used to determine causality.
     * <p>
     * Method is <code>protected</code> rather than <code>private</code>, so
     * that it may be overridden if the behaviour for handling concurrent values
     * of the same key was to be changed e.g., if two differently named stores
     * were added during a network split, merge appropriate metadata to include
     * both stores.
     * </p>
     * 
     * @param node Node to Gossip with.
     * @param key Metadata key to exchange by Gossip.
     */
    protected void gossipKey(Node node, String key) {
        if(logger.isDebugEnabled()) {
            logger.debug("Gossiping key " + key);
        }

        /*
         * Retrieve local and remote versions of the key. Uses AdminClient for
         * remote as well as local operations (rather than going directly to
         * MetadataStore for local operations), to avoid having to convert back
         * and forth between byte[] and String.
         */
        Versioned<String> remoteVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(node.getId(),
                                                                                   key);
        Versioned<String> localVersioned = adminClient.metadataMgmtOps.getRemoteMetadata(metadataStore.getNodeId(),
                                                                                  key);
        switch(remoteVersioned.getVersion().compare(localVersioned.getVersion())) {

        // If remote version came after local version, update with remote
        // version
            case AFTER: {
                logger.info("Updating key " + key + " from " + node);
                adminClient.metadataMgmtOps.updateRemoteMetadata(metadataStore.getNodeId(),
                                                          key,
                                                          remoteVersioned);
                if(logger.isDebugEnabled()) {
                    logger.debug("Updated key " + key + ": " + remoteVersioned);
                }

                break;
            }

            // If remote version came before the local version, do nothing and
            // wait for the other
            // node to gossip with us.
            case BEFORE: {
                if(logger.isDebugEnabled()) {
                    logger.debug("Remote(" + remoteVersioned
                                 + ") came before, allowing them to initiate Gossip");
                }

                break;
            }

            /*
             * If we can't establish a causal relationship between two versions,
             * there's a conflict. Ideally we should perform sensible
             * reconciliation, but for simplicity's sake we will just log an
             * error.
             */
            case CONCURRENTLY: {
                logger.error(key + " is concurrent between local node(" + localVersioned
                             + ") and remote at " + node + "(" + remoteVersioned + ")");

                break;
            }
        }
    }
}

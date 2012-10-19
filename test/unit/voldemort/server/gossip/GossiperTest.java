/*
 * Copyright 2012 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.Attempt;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Tests {@link voldemort.server.gossip.Gossiper}
 */
@RunWith(Parameterized.class)
public class GossiperTest {

    private static final Logger logger = Logger.getLogger(GossiperTest.class.getName());

    private List<VoldemortServer> servers = new ArrayList<VoldemortServer>();
    private Cluster cluster;
    private static final int socketBufferSize = 4096;
    private static final int adminSocketBufferSize = 8192;
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  socketBufferSize);
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private final boolean useNio;
    private CountDownLatch countDownLatch;
    final private Properties props = new Properties();

    public GossiperTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    private void attemptParallelClusterStart(ExecutorService executorService) {
        // Start all servers in parallel to avoid exceptions during gossip.
        cluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 },
                { 8, 9, 10, 11 } });

        for(int i = 0; i < 3; i++) {
            final int j = i;
            executorService.submit(new Runnable() {

                public void run() {
                    try {
                        servers.add(ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                                            j,
                                                                                                            TestUtils.createTempDir()
                                                                                                                     .getAbsolutePath(),
                                                                                                            null,
                                                                                                            storesXmlfile,
                                                                                                            props),
                                                                         cluster));
                    } catch(IOException ioe) {
                        logger.error("Caught IOException during parallel server start: "
                                     + ioe.getMessage());
                        RuntimeException re = new RuntimeException();
                        re.initCause(ioe);
                        throw re;
                    } finally {
                        // Ensure setup progresses in face of errors
                        countDownLatch.countDown();
                    }
                }
            });
        }
    }

    @Before
    public void setUp() {
        props.put("enable.gossip", "true");
        props.put("gossip.interval.ms", "250");
        props.put("socket.buffer.size", String.valueOf(socketBufferSize));
        props.put("admin.streams.buffer.size", String.valueOf(adminSocketBufferSize));

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        countDownLatch = new CountDownLatch(3);

        boolean clusterStarted = false;
        while(!clusterStarted) {
            try {
                attemptParallelClusterStart(executorService);
                clusterStarted = true;
            } catch(RuntimeException re) {
                logger.info("Some server thread threw a RuntimeException. Will print out stacktrace and then try again. Assumption is that the RuntimeException is due to BindException that in turn is due to TOCTOU issue with getLocalCluster");
                re.printStackTrace();
            }
        }

        try {
            countDownLatch.await();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    private AdminClient getAdminClient(Cluster newCluster) {
        return new AdminClient(newCluster, new AdminClientConfig());
    }

    private Cluster attemptStartAdditionalServer() throws IOException {
        // Set up a new cluster that is one bigger than the original cluster

        int originalSize = cluster.getNumberOfNodes();
        int numOriginalPorts = originalSize * 3;
        int ports[] = new int[numOriginalPorts + 3];
        for(int i = 0, j = 0; i < originalSize; i++, j += 3) {
            Node node = cluster.getNodeById(i);
            System.arraycopy(new int[] { node.getHttpPort(), node.getSocketPort(),
                                     node.getAdminPort() },
                             0,
                             ports,
                             j,
                             3);
        }

        System.arraycopy(ServerTestUtils.findFreePorts(3), 0, ports, numOriginalPorts, 3);

        // Create a new partitioning scheme with room for a new server
        final Cluster newCluster = ServerTestUtils.getLocalCluster(originalSize + 1,
                                                                   ports,
                                                                   new int[][] { { 0, 4, 8 },
                                                                           { 1, 5, 9 },
                                                                           { 2, 6, 10 },
                                                                           { 3, 7, 11 } });

        // Create a new server
        VoldemortServer newServer = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                         ServerTestUtils.createServerConfig(useNio,
                                                                                                            3,
                                                                                                            TestUtils.createTempDir()
                                                                                                                     .getAbsolutePath(),
                                                                                                            null,
                                                                                                            storesXmlfile,
                                                                                                            props),
                                                                         newCluster);
        // This step is only reached if startVoldemortServer does *not* throw a
        // BindException due to TOCTOU problem with getLocalCluster
        servers.add(newServer);
        return newCluster;
    }

    // Protect against this test running forever until the root cause of running
    // forever is found.
    @Test(timeout = 1800)
    public void testGossiper() throws Exception {
        Cluster newCluster = null;

        boolean startedAdditionalServer = false;
        while(!startedAdditionalServer) {
            try {
                newCluster = attemptStartAdditionalServer();
                startedAdditionalServer = true;
            } catch(IOException ioe) {
                logger.warn("Caught an IOException when attempting to start additional server. Will print stacktrace and then attempt to start additional server again.");
                ioe.printStackTrace();
            }
        }

        // Get the new cluster.xml
        AdminClient localAdminClient = getAdminClient(newCluster);

        Versioned<String> versionedClusterXML = localAdminClient.getRemoteMetadata(3,
                                                                                   MetadataStore.CLUSTER_KEY);

        // Increment the version, let what would be the "donor node" know about
        // it to seed the Gossip.
        Version version = versionedClusterXML.getVersion();
        ((VectorClock) version).incrementVersion(3, ((VectorClock) version).getTimestamp() + 1);
        ((VectorClock) version).incrementVersion(0, ((VectorClock) version).getTimestamp() + 1);

        localAdminClient.updateRemoteMetadata(0, MetadataStore.CLUSTER_KEY, versionedClusterXML);
        localAdminClient.updateRemoteMetadata(3, MetadataStore.CLUSTER_KEY, versionedClusterXML);

        try {
            Thread.sleep(500);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Wait up to five seconds for Gossip to spread
        final Cluster newFinalCluster = newCluster;
        try {
            TestUtils.assertWithBackoff(5000, new Attempt() {

                public void checkCondition() {
                    int serversSeen = 0;
                    // Now verify that we have gossiped correctly
                    for(VoldemortServer server: servers) {
                        Cluster clusterAtServer = server.getMetadataStore().getCluster();
                        int nodeId = server.getMetadataStore().getNodeId();
                        assertEquals("server " + nodeId + " has heard "
                                             + " the gossip about number of nodes",
                                     clusterAtServer.getNumberOfNodes(),
                                     newFinalCluster.getNumberOfNodes());
                        assertEquals("server " + nodeId + " has heard "
                                             + " the gossip about partitions",
                                     clusterAtServer.getNodeById(nodeId).getPartitionIds(),
                                     newFinalCluster.getNodeById(nodeId).getPartitionIds());
                        serversSeen++;
                    }
                    assertEquals("saw all servers", serversSeen, servers.size());
                }
            });
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

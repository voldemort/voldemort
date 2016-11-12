/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.Security;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.VectorClockUtils;

import com.google.common.collect.Lists;


/**
 * Unit test for VoldemortServer.
 */
public class VoldemortServerTest {

    private VoldemortServer server;

    private AdminClient oldAdminClient;

    @After
    public void tearDown() throws Exception {
        if(oldAdminClient != null) {
            oldAdminClient.close();
        }

        if(server != null) {
            ServerTestUtils.stopVoldemortServer(server);
        }

    }

    private VoldemortConfig getVoldemortConfig(Properties properties) throws IOException {
        properties.setProperty(VoldemortConfig.ENABLE_NODE_ID_DETECTION, Boolean.toString(true));
        VoldemortConfig config = ServerTestUtils.createServerConfig(true,
                                                                    -1,
                                           TestUtils.createTempDir().getAbsolutePath(),
                                           null,
                                           null,
                                           properties);
        return config;
    }

    private VoldemortServer getVoldemortServer(Properties properties) throws IOException {
        VoldemortConfig config = getVoldemortConfig(properties);
        Cluster cluster = ServerTestUtils.getLocalCluster(1);
        return new VoldemortServer(config, cluster);
    }

    @Test
    public void testSSLProviders() throws IOException {
        // The SSL providers changes the state of the JVM
        // if the methods are run in parallel or the ordering
        // changes the test fails. TestBouncyCastle is the
        // only test to enable this SSL. If other tests introduce
        // them the test could fails as well.
        testJCEProvider();
        testBouncyCastleProvider();
    }
    private void testJCEProvider() throws IOException {
        Properties properties = new Properties();

        // Default configuration. Bouncy castle provider will not be used.
        server = getVoldemortServer(properties);
        assertNull(Security.getProvider(BouncyCastleProvider.PROVIDER_NAME));
    }

    private void testBouncyCastleProvider() throws IOException {
        Properties properties = new Properties();
        // Use bouncy castle as first choice of JCE provider.
        properties.setProperty("use.bouncycastle.for.ssl", "true");

        server = getVoldemortServer(properties);
        assertEquals(BouncyCastleProvider.PROVIDER_NAME, Security.getProviders()[0].getName());
    }

    @Test
    public void testNodeIdDetection() throws IOException {
        final int NUM_NODES = 10;
        final int NODE_ID = 5;
        VoldemortConfig config = getVoldemortConfig(new Properties());
        final String SOMEHOST = "host" + NODE_ID;
        config.setNodeIdImplementation(new MockHostMatcher(SOMEHOST));
        
        List<String> hostNames = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i ++) {
            hostNames.add("host"+i);
        }
        
        Assert.assertEquals("At first no node Id", -1, config.getNodeId());
        Cluster cluster = HostMatcherTest.getCluster(hostNames);
        server = new VoldemortServer(config, cluster);
        server.start();

        Assert.assertEquals("Node id is not auto detected", NODE_ID, config.getNodeId());
        Assert.assertEquals("Node id is not auto detected", NODE_ID, server.getMetadataStore()
                                                                           .getNodeId());
    }

    @Test
    public void testInvalidNodeIdUpdateFails() throws IOException {
        Cluster localCluster = ServerTestUtils.getLocalCluster(1);
        VoldemortConfig config = getVoldemortConfig(new Properties());
        server = new VoldemortServer(config, localCluster);
        server.start();

        final int UPDATED_NODE_ID = 3;
        Node oldNode = localCluster.getNodes().iterator().next();
        // For single local node, host matcher is not used.
        config.setNodeIdImplementation(new NodeIdHostMatcher(UPDATED_NODE_ID));

        oldAdminClient = new AdminClient(localCluster);
        try {
            oldAdminClient.metadataMgmtOps.updateRemoteMetadata(oldNode.getId(),
                                                            MetadataStore.NODE_ID_KEY,
                                                            Integer.toString(UPDATED_NODE_ID));
            Assert.fail("Invalid node id should have failed");
        } catch(VoldemortException ex) {
            // Expected, ignore
        }
    }
    
    @Test
    public void testClusterWithDifferentStateFails() throws IOException {
        Cluster localCluster = ServerTestUtils.getLocalCluster(1);
        VoldemortConfig config = getVoldemortConfig(new Properties());
        server = new VoldemortServer(config, localCluster);
        server.start();

        final int UPDATED_NODE_ID = 3;
        Node oldNode = localCluster.getNodes().iterator().next();
        // For single local node, host matcher is not used.
        config.setNodeIdImplementation(new NodeIdHostMatcher(UPDATED_NODE_ID));
        Cluster updatedCluster = ServerTestUtils.getLocalCluster(UPDATED_NODE_ID + 1);

        oldAdminClient = new AdminClient(localCluster);
        try {
            oldAdminClient.metadataMgmtOps.updateRemoteCluster(oldNode.getId(),
                                                           updatedCluster,
                                                           VectorClockUtils.makeClockWithCurrentTime(localCluster.getNodeIds()));
            Assert.fail("Invalid node id should have failed");
        } catch(VoldemortException ex) {
            // Expected, ignore
        }
    }

    @Test
    public void testClusterUpdateWithAutoDetection() throws IOException {
        Cluster localCluster = ServerTestUtils.getLocalCluster(1);
        VoldemortConfig config = getVoldemortConfig(new Properties());
        server = new VoldemortServer(config, localCluster);
        server.start();

        final int UPDATED_NODE_ID = 3;
        Node oldNode = localCluster.getNodes().iterator().next();
        // For single local node, host matcher is not used.
        config.setNodeIdImplementation(new NodeIdHostMatcher(UPDATED_NODE_ID));
        Cluster updatedCluster = ServerTestUtils.getLocalCluster(UPDATED_NODE_ID + 1);

        oldAdminClient = new AdminClient(localCluster);
        List<Node> newNodes = Lists.newArrayList();
        for(Node node: updatedCluster.getNodes()) {
            if(node.getId() != UPDATED_NODE_ID) {
                newNodes.add(node);
            }
        }
        Node nodeToBeReplaced = updatedCluster.getNodeById(UPDATED_NODE_ID);
        Node updatedNode = new Node(UPDATED_NODE_ID,
                                    oldNode.getHost(),
                                    oldNode.getHttpPort(),
                                    oldNode.getSocketPort(),
                                    oldNode.getAdminPort(),
                                    nodeToBeReplaced.getPartitionIds());

        newNodes.add(updatedNode);

        Cluster updatedClusterWithCorrectNode = new Cluster("updated-cluster", newNodes);

        oldAdminClient.metadataMgmtOps.updateRemoteCluster(oldNode.getId(),
                                                           updatedClusterWithCorrectNode,
                                                           VectorClockUtils.makeClockWithCurrentTime(localCluster.getNodeIds()));

        Assert.assertEquals("Identity node is not auto detected",
                            UPDATED_NODE_ID,
                            server.getIdentityNode().getId());

        Assert.assertEquals("Voldemort config is not updated",
                            UPDATED_NODE_ID,
                            server.getVoldemortConfig().getNodeId());

        Assert.assertEquals("Metadata store is not updated",
                            UPDATED_NODE_ID,
                            server.getMetadataStore().getNodeId());
    }
}

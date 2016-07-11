package voldemort.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import com.google.common.collect.Lists;

public class HostMatcherTest {

    private static final int PORT = 6666; // No socket is created, so hard coded

    public static Cluster getCluster(List<String> hostNames, List<Integer> nodeIds) {
        List<Node> nodes = Lists.newArrayList();
        for(int i = 0; i < hostNames.size(); i++) {
            int nodeId = (nodeIds == null) ? i : nodeIds.get(i);
            nodes.add(new Node(nodeId, hostNames.get(i), PORT, PORT + 1, PORT + 2, Arrays.asList(i)));
        }
        return new Cluster("HostMatcherTest", nodes);
    }

    public static Cluster getCluster(List<String> hostNames) {
        return getCluster(hostNames, null);
    }

    private Cluster singleNodeCluster(int nodeId) {
        return getCluster(Arrays.asList("localhost"), Arrays.asList(nodeId));
    }

    private void validateNodeId(Cluster cluster,
                                HostMatcher matcher,
                                List<Integer> nodeIdsToValidate,
                                int actualNodeId) {
        NodeIdUtils.validateNodeId(cluster, matcher, actualNodeId);

        for(Integer nodeId: nodeIdsToValidate) {
            if(nodeId == actualNodeId) {
                // validate one more time
                NodeIdUtils.validateNodeId(cluster, matcher, actualNodeId);
                continue;
            }

            try {
                // Increment node Id by 1 and make sure it fails
                NodeIdUtils.validateNodeId(cluster, matcher, nodeId);
                Assert.fail("Validation should have failed");
            } catch(VoldemortException ex) {
                // Expected Ignore
            }
        }
    }

    @Test
    public void testLocalHost() {
        for(int nodeId = 0; nodeId < 5; nodeId++) {
            Cluster localCluster = singleNodeCluster(nodeId);
            HostMatcher matcher = new HostMatcher();
            int actual = NodeIdUtils.findNodeId(localCluster, matcher);
            Assert.assertEquals("Node Id is different", nodeId, actual);
            validateNodeId(localCluster, matcher, Arrays.asList(nodeId + 1), nodeId);
        }
    }

    private void validateCluster(Cluster cluster,
                                        List<String> hostNames,
                                        List<Integer> nodeIds,
                                        final int NUM_NODES) {

        HostMatcher invalidMatcher = new MockHostMatcher("invalidHost");
        try {
            NodeIdUtils.findNodeId(cluster, invalidMatcher);
            Assert.fail("None of the hosts should have matched");
        } catch(VoldemortException ex) {
            // Ignore
        }
        

        for(int i = 0; i < NUM_NODES; i++) {
            String hostName = hostNames.get(i);
            int expectedNodeId = nodeIds.get(i);
            HostMatcher matcher = new MockHostMatcher(hostName);
            int actulaNodeId = NodeIdUtils.findNodeId(cluster, matcher);
            Assert.assertEquals("Node Id is different", expectedNodeId, actulaNodeId);
            validateNodeId(cluster, matcher, nodeIds, actulaNodeId);
        }
    }

    @Test
    public void testHostMatch() {
        final int NUM_NODES = 60;
        List<String> hostNames = Lists.newArrayList();
        List<Integer> nodeIds = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            hostNames.add("host" + i);
            nodeIds.add(i);
        }

        Collections.shuffle(hostNames);
        Collections.shuffle(nodeIds);

        Cluster cluster = getCluster(hostNames, nodeIds);
        validateCluster(cluster, hostNames, nodeIds, NUM_NODES);
    }

    private void validateMatchFails(Cluster cluster, HostMatcher matcher) {
        try {
            NodeIdUtils.findNodeId(cluster, matcher);
            Assert.fail("multiple match should have failed");
        } catch(VoldemortException ex) {
            // Ignore.
        }

        for(int nodeId: cluster.getNodeIds()) {
            try {
                NodeIdUtils.validateNodeId(cluster, matcher, nodeId);
                Assert.fail("multiple match should have failed");
            } catch(VoldemortException ex) {
                // Ignore.
            }
        }
    }

    @Test
    public void testMultipleMatchFails() {
        final String SOMEHOST = "samehost";
        Cluster cluster = getCluster(Arrays.asList(SOMEHOST, SOMEHOST));

        HostMatcher matcher = new MockHostMatcher(SOMEHOST);
        validateMatchFails(cluster, matcher);
    }

    @Test
    public void testConflictingMatchFails() {
        final String SOMEHOST = "somehost";

        Cluster cluster = getCluster(Arrays.asList(SOMEHOST.toLowerCase(), SOMEHOST.toUpperCase()));
        HostMatcher matcher = new MockHostMatcher(SOMEHOST);
        validateMatchFails(cluster, matcher);
    }

    @Test
    public void testMultipleLocalHostFails() {
        Cluster cluster = ServerTestUtils.getLocalCluster(2);
        HostMatcher matcher = new HostMatcher();
        validateMatchFails(cluster, matcher);
    }
}

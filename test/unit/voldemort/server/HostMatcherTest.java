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

    private List<String> allTypes = Arrays.asList(HostMatcher.FQDN, HostMatcher.HOSTNAME);

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
            HostMatcher matcher = new PosixHostMatcher(allTypes);
            int actual = NodeIdUtils.findNodeId(localCluster, matcher);
            Assert.assertEquals("Node Id is different", nodeId, actual);
            validateNodeId(localCluster, matcher, Arrays.asList(nodeId + 1), nodeId);
        }
    }

    private void validateCluster(Cluster cluster,
                                        List<String> hostNames,
                                        List<String> fqdns,
                                        List<Integer> nodeIds,
                                        List<String> types,
                                        final int NUM_NODES) {

        HostMatcher invalidMatcher = new MockHostMatcher(types, "invalidHost", "invalidDomain");
        try {
            NodeIdUtils.findNodeId(cluster, invalidMatcher);
            Assert.fail("None of the hosts should have matched");
        } catch(VoldemortException ex) {
            // Ignore
        }
        

        for(int i = 0; i < NUM_NODES; i++) {
            String hostName = hostNames.get(i);
            int expectedNodeId = nodeIds.get(i);
            String fqdn = fqdns.get(i);
            HostMatcher matcher = new MockHostMatcher(types, hostName, fqdn);
            int actulaNodeId = NodeIdUtils.findNodeId(cluster, matcher);
            Assert.assertEquals("Node Id is different", expectedNodeId, actulaNodeId);
            validateNodeId(cluster, matcher, nodeIds, actulaNodeId);
        }
    }

    private void validateCluster(Cluster cluster,
                                 List<String> hostNames,
                                 List<String> fqdns,
                                 List<Integer> nodeIds,
                                 String type,
                                 final int NUM_NODES) {


        validateCluster(cluster, hostNames, fqdns, nodeIds, allTypes, NUM_NODES);
        validateCluster(cluster,
                        hostNames,
                        fqdns,
                        nodeIds,
                        Arrays.asList(type),
                        NUM_NODES);
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

        List<String> fqdns = Lists.newArrayList();
        for(int i = 0; i < NUM_NODES; i++) {
            fqdns.add(hostNames.get(i) + ".domain");
        }

        Cluster cluster = getCluster(hostNames, nodeIds);
        validateCluster(cluster, hostNames, fqdns, nodeIds, HostMatcher.HOSTNAME, NUM_NODES);

        cluster = getCluster(fqdns, nodeIds);
        validateCluster(cluster, hostNames, fqdns, nodeIds, HostMatcher.FQDN, NUM_NODES);
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

        HostMatcher matcher = new MockHostMatcher(allTypes, SOMEHOST, SOMEHOST + ".domain");
        validateMatchFails(cluster, matcher);
    }

    @Test
    public void testConflictingMatchFails() {
        final String SOMEHOST = "somehost";
        final String SOMEFQDN = "somehost.domain";

        Cluster cluster = getCluster(Arrays.asList(SOMEFQDN, SOMEHOST));
        HostMatcher matcher = new MockHostMatcher(allTypes, SOMEHOST, SOMEFQDN);
        validateMatchFails(cluster, matcher);
    }

    @Test
    public void testMultipleLocalHostFails() {
        Cluster cluster = ServerTestUtils.getLocalCluster(2);
        HostMatcher matcher = new PosixHostMatcher(allTypes);
        validateMatchFails(cluster, matcher);
    }

    @Test
    public void testMixPasses() {
        final String SOMEHOST = "somehost";
        final String SOMEFQDN = "somehost.domain";

        final int HOST_NODE_ID = 5;
        final int FQDN_NODE_ID = 10;

        Cluster cluster = getCluster(Arrays.asList(SOMEHOST, SOMEFQDN),
                                     Arrays.asList(HOST_NODE_ID, FQDN_NODE_ID));
        
        HostMatcher matcher1 = new MockHostMatcher(Arrays.asList(HostMatcher.HOSTNAME),
                                                  SOMEHOST,
                                                  SOMEFQDN);

        int hostNodeId = NodeIdUtils.findNodeId(cluster, matcher1);
        Assert.assertEquals("matcher found a different node id", HOST_NODE_ID, hostNodeId);
        
        HostMatcher matcher2 = new MockHostMatcher(Arrays.asList(HostMatcher.FQDN),
                                                   SOMEHOST,
                                                   SOMEFQDN);
        int fqdnNodeId = NodeIdUtils.findNodeId(cluster, matcher2);
        Assert.assertEquals("matcher found a different node id", FQDN_NODE_ID, fqdnNodeId);
    }
}

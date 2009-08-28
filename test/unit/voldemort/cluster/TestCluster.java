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

package voldemort.cluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import voldemort.NodeAvailabilityDetectorTestCase;
import voldemort.TestUtils;
import voldemort.cluster.nodeavailabilitydetector.NodeAvailabilityDetector;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

import com.google.common.collect.ImmutableList;

public class TestCluster extends TestCase implements NodeAvailabilityDetectorTestCase {

    private String clusterName = "test";
    private List<Node> nodes;
    private Cluster cluster;
    private Class<NodeAvailabilityDetector> nodeAvailabilityDetectorClass;
    private NodeAvailabilityDetector nodeAvailabilityDetector;

    public static Test suite() {
        return TestUtils.createNodeAvailabilityDetectorTestSuite(TestCluster.class);
    }

    @Override
    public void setUp() throws Exception {
        this.nodes = ImmutableList.of(new Node(1, "test1", 1, 1, ImmutableList.of(1, 2, 3)),
                                      new Node(2, "test1", 2, 2, ImmutableList.of(3, 5, 6)),
                                      new Node(3, "test1", 3, 3, ImmutableList.of(7, 8, 9)),
                                      new Node(4, "test1", 4, 4, ImmutableList.of(10, 11, 12)));
        this.cluster = new Cluster(clusterName, nodes);

        nodeAvailabilityDetector = nodeAvailabilityDetectorClass.newInstance();
        nodeAvailabilityDetector.setNodeBannageMs(10000);
        nodeAvailabilityDetector.setStores(new HashMap<Integer, Store<ByteArray, byte[]>>());
    }

    public void setNodeAvailabilityDetectorClass(Class<NodeAvailabilityDetector> nodeAvailabilityDetectorClass) {
        this.nodeAvailabilityDetectorClass = nodeAvailabilityDetectorClass;
    }

    public void testBasics() {
        assertEquals(nodes.size(), cluster.getNumberOfNodes());
        assertEquals(new HashSet<Node>(nodes), new HashSet<Node>(cluster.getNodes()));
        assertEquals(clusterName, cluster.getName());
        assertEquals(nodes.get(0), cluster.getNodeById(1));
    }

    public void testStatusBeginsAsAvailable() {
        for(Node n: cluster.getNodes())
            assertTrue("Node " + n.getId() + " is not available.",
                       nodeAvailabilityDetector.isAvailable(n));
    }

    public void testUnavailability() {
        Node n = cluster.getNodeById(1);

        // begins available
        assertTrue(nodeAvailabilityDetector.isAvailable(n));

        // if set unavailable, is unavailable
        nodeAvailabilityDetector.recordException(n, null);
        assertFalse(nodeAvailabilityDetector.isAvailable(n));

        // if we set it back to available then it is available again
        nodeAvailabilityDetector.recordSuccess(n);

        assertTrue(nodeAvailabilityDetector.isAvailable(n));
    }

}

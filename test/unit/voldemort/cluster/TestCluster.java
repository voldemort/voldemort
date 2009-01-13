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

import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;
import voldemort.MockTime;

import com.google.common.collect.ImmutableList;

public class TestCluster extends TestCase {

    private String clusterName = "test";
    private MockTime time = new MockTime();
    private List<Node> nodes;
    private Cluster cluster;

    public void setUp() {
        this.nodes = ImmutableList.of(new Node(1,
                                               "test1",
                                               1,
                                               1,
                                               ImmutableList.of(1, 2, 3),
                                               new NodeStatus(time)),
                                      new Node(2,
                                               "test1",
                                               2,
                                               2,
                                               ImmutableList.of(3, 5, 6),
                                               new NodeStatus(time)),
                                      new Node(3,
                                               "test1",
                                               3,
                                               3,
                                               ImmutableList.of(7, 8, 9),
                                               new NodeStatus(time)),
                                      new Node(4,
                                               "test1",
                                               4,
                                               4,
                                               ImmutableList.of(10, 11, 12),
                                               new NodeStatus(time)));
        this.cluster = new Cluster(clusterName, nodes);
    }

    public void testBasics() {
        assertEquals(nodes.size(), cluster.getNumberOfNodes());
        assertEquals(new HashSet<Node>(nodes), new HashSet<Node>(cluster.getNodes()));
        assertEquals(clusterName, cluster.getName());
        assertEquals(nodes.get(0), cluster.getNodeById(1));
    }

    public void testStatusBeginsAsAvailable() {
        for(Node n: cluster.getNodes()) {
            assertTrue("Node " + n.getId() + " is not available.", n.getStatus().isAvailable());
            assertFalse("Node " + n.getId() + " is not available.", n.getStatus().isUnavailable());
        }
    }

    public void testUnavailability() {
        Node n = cluster.getNodeById(1);
        NodeStatus status = n.getStatus();

        // begins available
        assertTrue(status.isAvailable());
        assertFalse(status.isUnavailable());

        // if set unavailable, is unavailable
        status.setUnavailable();
        assertFalse(status.isAvailable());
        assertTrue(status.isUnavailable());
        assertTrue(status.isUnavailable(10));

        // after 11 ms of no ops, still unavailable, but not
        // if checked with expiration
        time.addMilliseconds(11);
        assertTrue(status.isUnavailable());
        assertFalse(status.isAvailable());
        assertFalse(status.isUnavailable(10));

        // if we set it back to available then it is available again
        status.setAvailable();
        assertTrue(status.isAvailable());
        assertFalse(status.isUnavailable());
        assertFalse(status.isUnavailable(10));
    }

}

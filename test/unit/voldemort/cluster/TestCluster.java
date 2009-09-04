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

import junit.framework.Test;
import junit.framework.TestCase;
import voldemort.FailureDetectorTestCase;
import voldemort.TestUtils;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorUtils;

import com.google.common.collect.ImmutableList;

public class TestCluster extends TestCase implements FailureDetectorTestCase {

    private String clusterName = "test";
    private List<Node> nodes;
    private Cluster cluster;
    private Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;

    public static Test suite() {
        return TestUtils.createFailureDetectorTestSuite(TestCluster.class);
    }

    @Override
    public void setUp() throws Exception {
        this.nodes = ImmutableList.of(new Node(1, "test1", 1, 1, ImmutableList.of(1, 2, 3)),
                                      new Node(2, "test1", 2, 2, ImmutableList.of(3, 5, 6)),
                                      new Node(3, "test1", 3, 3, ImmutableList.of(7, 8, 9)),
                                      new Node(4, "test1", 4, 4, ImmutableList.of(10, 11, 12)));
        this.cluster = new Cluster(clusterName, nodes);

        failureDetector = FailureDetectorUtils.create(failureDetectorClass.getName(),
                                                                        10000);
    }

    public void setFailureDetectorClass(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
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
                       failureDetector.isAvailable(n));
    }

    public void testUnavailability() {
        Node n = cluster.getNodeById(1);

        // begins available
        assertTrue(failureDetector.isAvailable(n));

        // if set unavailable, is unavailable
        failureDetector.recordException(n, null);
        assertFalse(failureDetector.isAvailable(n));

        // if we set it back to available then it is available again
        failureDetector.recordSuccess(n);

        assertTrue(failureDetector.isAvailable(n));
    }

}

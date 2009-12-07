/*
 * Copyright 2009 LinkedIn, Inc
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

package voldemort.cluster.failuredetector;

import static org.junit.Assert.fail;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.MockTime;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.Time;

public abstract class AbstractFailureDetectorTest {

    protected Time time = new MockTime();
    protected Cluster cluster;
    protected FailureDetector failureDetector;

    @Before
    public void setUp() throws Exception {
        cluster = getNineNodeCluster();
        failureDetector = setUpFailureDetector();
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    protected Cluster setUpCluster() throws Exception {
        return getNineNodeCluster();
    }

    @Test
    public void testInvalidNode() throws Exception {
        Node invalidNode = new Node(10000,
                                    "localhost",
                                    8081,
                                    6666,
                                    6667,
                                    Collections.<Integer> emptyList());

        try {
            failureDetector.getLastChecked(invalidNode);
            fail("Should not be able to call getLastChecked on invalid node");
        } catch(IllegalArgumentException e) {
            // Expected...
        }

        try {
            failureDetector.isAvailable(invalidNode);
            fail("Should not be able to call isAvailable on invalid node");
        } catch(IllegalArgumentException e) {
            // Expected...
        }

        try {
            failureDetector.recordException(invalidNode, null);
            fail("Should not be able to call recordException on invalid node");
        } catch(IllegalArgumentException e) {
            // Expected...
        }

        try {
            failureDetector.recordSuccess(invalidNode);
            fail("Should not be able to call recordSuccess on invalid node");
        } catch(IllegalArgumentException e) {
            // Expected...
        }

        try {
            failureDetector.waitFor(invalidNode);
            fail("Should not be able to call waitFor on invalid node");
        } catch(IllegalArgumentException e) {
            // Expected...
        }
    }

    protected abstract FailureDetector setUpFailureDetector() throws Exception;

}

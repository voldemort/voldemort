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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.lang.management.ManagementFactory;
import java.util.Collections;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;

public abstract class AbstractFailureDetectorTest {

    protected static final int BANNAGE_MILLIS = 10000;

    protected Time time;

    protected Cluster cluster;

    protected FailureDetector failureDetector;

    @Before
    public void setUp() throws Exception {
        time = createTime();
        cluster = getNineNodeCluster();
        failureDetector = createFailureDetector();
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    protected Cluster setUpCluster() throws Exception {
        return getNineNodeCluster();
    }

    protected void assertAvailable(Node node) {
        assertEquals(9, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(true, failureDetector.isAvailable(node));
    }

    protected void assertUnavailable(Node node) {
        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));
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
    }

    @Test
    public void testGeneralJmx() throws Exception {
        assertJmxEquals("availableNodes", "Node0,Node1,Node2,Node3,Node4,Node5,Node6,Node7,Node8");
        assertJmxEquals("unavailableNodes", "");
        assertJmxEquals("availableNodeCount", 9);
        assertJmxEquals("nodeCount", 9);
    }

    protected void assertJmxEquals(String attributeName, Object attributeValue) throws Exception {
        JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(new JMXServiceURL("service:jmx:rmi://"),
                                                                                null,
                                                                                ManagementFactory.getPlatformMBeanServer());
        cs.start();

        JMXConnector cc = null;

        try {
            cc = JMXConnectorFactory.connect(cs.getAddress());
            MBeanServerConnection mbsc = cc.getMBeanServerConnection();
            ObjectName objectName = JmxUtils.createObjectName(JmxUtils.getPackageName(failureDetector.getClass()),
                                                              failureDetector.getClass()
                                                                             .getSimpleName());

            Object availableNodes = mbsc.getAttribute(objectName, attributeName);
            assertEquals(attributeValue, availableNodes);
        } finally {
            if(cc != null)
                cc.close();

            cs.stop();
        }
    }

    protected abstract FailureDetector createFailureDetector() throws Exception;

    protected abstract Time createTime() throws Exception;

}

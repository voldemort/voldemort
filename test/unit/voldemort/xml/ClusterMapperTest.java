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

package voldemort.xml;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;

import com.google.common.collect.Lists;

public class ClusterMapperTest extends TestCase {

    public void testClusterMapperValues() {
        ClusterMapper mapper = new ClusterMapper();
        Cluster cluster = mapper.readCluster(new StringReader(VoldemortTestConstants.getOneNodeClusterXml()));
        assertEquals(cluster.getNumberOfNodes(), 1);
        assertEquals(cluster.getName(), "mycluster");
        Node node = cluster.getNodes().iterator().next();
        assertNotNull(node);
        assertEquals(node.getId(), 0);
        List<Integer> tags = node.getPartitionIds();
        assertTrue("Tag not found.", tags.contains(0));
        assertTrue("Tag not found.", tags.contains(1));

    }

    public void testOtherClusters() {
        ClusterMapper mapper = new ClusterMapper();
        Cluster cluster = mapper.readCluster(new StringReader(VoldemortTestConstants.getNineNodeClusterXml()));
        assertEquals(cluster.getNumberOfNodes(), 9);
        assertEquals(cluster.getZones().size(), 1);

        cluster = mapper.readCluster(new StringReader(VoldemortTestConstants.getTwoNodeClusterXml()));
        assertEquals(cluster.getNumberOfNodes(), 2);
        assertEquals(cluster.getZones().size(), 1);

        cluster = mapper.readCluster(new StringReader(VoldemortTestConstants.getFourNodeClusterWithZonesXml()));
        assertEquals(cluster.getNumberOfNodes(), 4);
        Collection<Zone> zones = cluster.getZones();
        assertEquals(zones.size(), 3);
        for(Zone zone: zones) {
            assertEquals(zone.getProximityList().size(), 2);
        }
    }

    public void testClusterEquals() {
        List<Zone> zones2 = ServerTestUtils.getZones(2);
        List<Zone> zones3 = ServerTestUtils.getZones(3);

        Cluster cluster1 = ServerTestUtils.getLocalCluster(4, 10, 2);
        Cluster cluster2 = new Cluster("cluster2", Lists.newArrayList(cluster1.getNodes()), zones3);

        // Test different number of zones
        assertFalse(cluster1.equals(cluster2));

        // Test proximity size not same
        List<Zone> modifiedZones2 = Lists.newArrayList();
        modifiedZones2.add(zones2.get(0));
        LinkedList<Integer> newProximityList = Lists.newLinkedList(zones2.get(1).getProximityList());
        newProximityList.add(100);
        modifiedZones2.add(new Zone(zones2.get(1).getId(), newProximityList));

        cluster1 = new Cluster("cluster1", new ArrayList<Node>(), zones2);
        cluster2 = new Cluster("cluster2", new ArrayList<Node>(), modifiedZones2);
        assertFalse(cluster1.equals(cluster2));

        // Test proximity list different order
        List<Zone> modifiedZones3 = Lists.newArrayList();
        for(int zoneId = 0; zoneId < 3; zoneId++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList(zones3.get(zoneId)
                                                                          .getProximityList());
            Collections.reverse(proximityList);
            modifiedZones3.add(new Zone(zones3.get(zoneId).getId(), proximityList));
        }

        cluster1 = new Cluster("cluster1", new ArrayList<Node>(), zones3);
        cluster2 = new Cluster("cluster2", new ArrayList<Node>(), modifiedZones3);
        assertFalse(cluster1.equals(cluster2));

        // Test nodes in different zones

        cluster1 = ServerTestUtils.getLocalCluster(4, 10, 2);

        List<Node> newNodes = Lists.newArrayList();
        for(Node node: cluster1.getNodes()) {
            newNodes.add(new Node(node.getId(),
                                  node.getHost(),
                                  node.getHttpPort(),
                                  node.getSocketPort(),
                                  node.getAdminPort(),
                                  0,
                                  node.getPartitionIds()));
        }
        cluster2 = new Cluster("cluster2", newNodes, Lists.newArrayList(cluster1.getZones()));
        assertFalse(cluster1.equals(cluster2));

        // Test equality

        cluster2 = new Cluster("cluster2",
                               Lists.newArrayList(cluster1.getNodes()),
                               Lists.newArrayList(cluster1.getZones()));
        assertTrue(cluster1.equals(cluster2));
    }
}

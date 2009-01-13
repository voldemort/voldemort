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
import java.util.List;

import junit.framework.TestCase;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

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
        mapper.readCluster(new StringReader(VoldemortTestConstants.getNineNodeClusterXml()));
        mapper.readCluster(new StringReader(VoldemortTestConstants.getTwoNodeClusterXml()));
    }
}

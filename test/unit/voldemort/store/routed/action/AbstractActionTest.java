/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.io.StringReader;

import org.junit.After;
import org.junit.Before;

import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.xml.StoreDefinitionsMapper;

public class AbstractActionTest {

    protected Cluster cluster, clusterWithZones;
    protected StoreDefinition storeDef;
    protected final ByteArray aKey = TestUtils.toByteArray("jay");
    protected FailureDetector failureDetector, failureDetectorWithZones;

    @Before
    public void setUp() throws Exception {
        cluster = VoldemortTestConstants.getThreeNodeCluster();
        failureDetector = new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(cluster.getNodes()));
        clusterWithZones = VoldemortTestConstants.getFourNodeClusterWithZones();
        failureDetectorWithZones = new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(clusterWithZones.getNodes()));
        storeDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getSingleStoreWithZonesXml()))
                                               .get(0);
    }

    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
        if(failureDetectorWithZones != null)
            failureDetectorWithZones.destroy();
    }

}

/*
 * Copyright 2008-2013 LinkedIn, Inc
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
package voldemort.server.storage;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.routing.StoreRoutingPlan;
import voldemort.store.StoreDefinition;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class VersionedPutPruningTest {

    Cluster cluster;
    StoreDefinition storeDef;
    StoreRoutingPlan routingPlan;

    @Before
    public void setup() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeCluster();
        StringReader reader = new StringReader(VoldemortTestConstants.getSingleStore322Xml());
        storeDef = new StoreDefinitionsMapper().readStoreList(reader).get(0);
        routingPlan = new StoreRoutingPlan(cluster, storeDef);
    }

    @Test
    public void testPruningLogic() {

        byte[] key = "key1".getBytes();

        // key1 replicas :[0, 2, 1]
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
        VectorClock clock1 = TestUtils.getClock(0, 2, 1, 3);
        VectorClock clock2 = TestUtils.getClock(9, 4);
        VectorClock clock3 = TestUtils.getClock(0, 1);
        VectorClock clock4 = TestUtils.getClock(8, 0);

        vals.add(new Versioned<byte[]>(key, clock1));
        vals.add(new Versioned<byte[]>(key, clock2));
        vals.add(new Versioned<byte[]>(key, clock3));
        vals.add(new Versioned<byte[]>(key, clock4));

        MutableBoolean didPrune = new MutableBoolean(false);
        List<Versioned<byte[]>> prunedVals = VersionedPutPruneJob.pruneNonReplicaEntries(vals,
                                                                                         Lists.newArrayList(0,
                                                                                                            2,
                                                                                                            1),
                                                                                         didPrune);
        assertEquals("Must have pruned some versions", true, didPrune.booleanValue());
        assertEquals("Not pruned properly", TestUtils.getClock(0, 1, 2), prunedVals.get(0)
                                                                                   .getVersion());
        assertEquals("Not pruned properly", TestUtils.getClock(), prunedVals.get(1).getVersion());
        assertEquals("Not pruned properly", TestUtils.getClock(0, 1), prunedVals.get(2)
                                                                                .getVersion());
        assertEquals("Not pruned properly", TestUtils.getClock(0), prunedVals.get(3).getVersion());

        List<Versioned<byte[]>> resolvedVals = VectorClockUtils.resolveVersions(prunedVals);

        assertEquals("Must be exactly one winning version", 1, resolvedVals.size());
        assertEquals("Incorrect winning version",
                     TestUtils.getClock(0, 1, 2),
                     resolvedVals.get(0).getVersion());
        assertEquals("Incorrect winning version",
                     clock1.getTimestamp(),
                     ((VectorClock) resolvedVals.get(0).getVersion()).getTimestamp());
    }

    @After
    public void teardown() {

    }
}

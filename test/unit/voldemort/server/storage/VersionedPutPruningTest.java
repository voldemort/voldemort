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
import static org.junit.Assert.assertFalse;

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
import voldemort.server.storage.prunejob.VersionedPutPruneJob;
import voldemort.store.StoreDefinition;
import voldemort.utils.Time;
import voldemort.versioning.ChainedResolver;
import voldemort.versioning.Occurred;
import voldemort.versioning.TimeBasedInconsistencyResolver;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockInconsistencyResolver;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public class VersionedPutPruningTest {

    Cluster cluster;
    StoreDefinition storeDef;
    StoreRoutingPlan routingPlan;
    byte[] key;
    List<Integer> keyReplicas;

    @Before
    public void setup() throws Exception {
        cluster = VoldemortTestConstants.getNineNodeCluster();
        StringReader reader = new StringReader(VoldemortTestConstants.getSingleStore322Xml());
        storeDef = new StoreDefinitionsMapper().readStoreList(reader).get(0);
        routingPlan = new StoreRoutingPlan(cluster, storeDef);
        key = "key1".getBytes();
        keyReplicas = Lists.newArrayList(0, 2, 1);
    }

    @Test
    public void testPruningLogic() {
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
                                                                                         keyReplicas,
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

    private List<Versioned<byte[]>> pruneAndResolve(List<Versioned<byte[]>> vals,
                                                    MutableBoolean didPrune) {
        return VectorClockUtils.resolveVersions(VersionedPutPruneJob.pruneNonReplicaEntries(vals,
                                                                                            keyReplicas,
                                                                                            didPrune));
    }

    @Test
    public void testOnlineBehavior() {

        long now = System.currentTimeMillis();

        // let's assume previous replicas are [4, 5, 0]
        VectorClock fetchedClock = TestUtils.getVersionedPutClock(now, 4, 4, 5, 0);
        VectorClock onlineClock = TestUtils.getVersionedPutClock(now, 0, 0, 2, 1);
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock, onlineClock));

        // case where key has received writes before the prune job
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
        vals.add(new Versioned<byte[]>(key, fetchedClock));
        vals.add(new Versioned<byte[]>(key, onlineClock));
        MutableBoolean didPrune = new MutableBoolean();

        vals = pruneAndResolve(vals, didPrune);
        assertEquals("Must have pruned something", true, didPrune.booleanValue());
        assertEquals("Must have one winning version", 1, vals.size());
        assertEquals("Must resolve to onlineClock", onlineClock, vals.get(0).getVersion());

        // case where key has not received any writes before the prune job
        vals = new ArrayList<Versioned<byte[]>>();
        vals.add(new Versioned<byte[]>(key, fetchedClock));
        didPrune = new MutableBoolean();

        vals = pruneAndResolve(vals, didPrune);
        assertEquals("Must have pruned something", true, didPrune.booleanValue());
        assertEquals("Must have one winning version", 1, vals.size());
        assertEquals("Must resolve to [0:ts] clock",
                     TestUtils.getVersionedPutClock(now, -1, 0),
                     vals.get(0).getVersion());
        VectorClock nextOnlineClock = TestUtils.getVersionedPutClock(now + Time.MS_PER_SECOND,
                                                                     0,
                                                                     0,
                                                                     2,
                                                                     1);
        assertFalse("Next online write would not result in conflict",
                    Occurred.CONCURRENTLY == VectorClockUtils.compare((VectorClock) vals.get(0)
                                                                                        .getVersion(),
                                                                      nextOnlineClock));
    }

    @Test
    public void testOnlineBehaviorWithConflicts() {
        long now = System.currentTimeMillis();

        // let's assume previous replicas are [4, 5, 0]. Note the intersection
        // between old and new replicas
        VectorClock fetchedClock1 = TestUtils.getVersionedPutClock(now, 4, 4, 5, 0);
        VectorClock fetchedClock2 = TestUtils.getVersionedPutClock(now, 5, 4, 5, 0);

        // to tighen the screws, assume the fetch delay was 0ms and two
        // conflicting writes followed on the same ms
        VectorClock onlineClock1 = TestUtils.getVersionedPutClock(now, 0, 0, 2, 1);
        VectorClock onlineClock2 = TestUtils.getVersionedPutClock(now, 2, 0, 2, 1);

        assertEquals("fetched versions themselves should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock1, fetchedClock2));
        assertEquals("online versions themselves should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(onlineClock1, onlineClock2));
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock1, onlineClock1));
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock1, onlineClock1));
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock1, onlineClock2));
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock2, onlineClock1));
        assertEquals("fetched and online versions should conflict",
                     Occurred.CONCURRENTLY,
                     VectorClockUtils.compare(fetchedClock2, onlineClock2));

        // case where key has received writes before the prune job
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
        vals.add(new Versioned<byte[]>(key, fetchedClock1));
        vals.add(new Versioned<byte[]>(key, onlineClock1));
        vals.add(new Versioned<byte[]>(key, fetchedClock2));
        vals.add(new Versioned<byte[]>(key, onlineClock2));
        MutableBoolean didPrune = new MutableBoolean();

        vals = pruneAndResolve(vals, didPrune);
        assertEquals("Must have pruned something", true, didPrune.booleanValue());
        assertEquals("Must have two winning versions", 2, vals.size());
        assertEquals("Must have onlineClock1", onlineClock1, vals.get(0).getVersion());
        assertEquals("Must have onlineClock2", onlineClock2, vals.get(1).getVersion());

        // case where key has not received any writes before the prune job
        vals = new ArrayList<Versioned<byte[]>>();
        vals.add(new Versioned<byte[]>(key, fetchedClock1));
        vals.add(new Versioned<byte[]>(key, fetchedClock2));
        didPrune = new MutableBoolean();

        vals = pruneAndResolve(vals, didPrune);
        assertEquals("Must have pruned something", true, didPrune.booleanValue());
        // Note that since 0 is not a master in both fetched clocks, there will
        // be one version. If 0 were to be a master, there will be one version,
        // since master clock will trump non-master clock
        assertEquals("Must have one winning version", 1, vals.size());
        assertEquals("Must resolve to [0:ts] clock",
                     TestUtils.getVersionedPutClock(now, -1, 0),
                     vals.get(0).getVersion());

        VectorClock nextOnlineClock1 = TestUtils.getVersionedPutClock(now, 0, 0, 2, 1);
        VectorClock nextOnlineClock2 = TestUtils.getVersionedPutClock(now, 2, 0, 2, 1);

        assertFalse("Next online write would not result in conflict",
                    Occurred.CONCURRENTLY == VectorClockUtils.compare((VectorClock) vals.get(0)
                                                                                        .getVersion(),
                                                                      nextOnlineClock1));
        assertFalse("Next online write would not result in conflict",
                    Occurred.CONCURRENTLY == VectorClockUtils.compare((VectorClock) vals.get(0)
                                                                                        .getVersion(),
                                                                      nextOnlineClock2));
    }

    @Test
    public void testOutOfSyncPruning() {
        long now = System.currentTimeMillis();

        byte[] onlineVal = "online".getBytes();

        ChainedResolver<Versioned<byte[]>> resolver = new ChainedResolver<Versioned<byte[]>>(new VectorClockInconsistencyResolver<byte[]>(),
                                                                                             new TimeBasedInconsistencyResolver<byte[]>());

        // let's assume previous replicas are [4, 5, 0]
        VectorClock fetchedClock = TestUtils.getVersionedPutClock(now, 4, 4, 5, 0);
        VectorClock onlineClock1 = TestUtils.getVersionedPutClock(now + Time.MS_PER_SECOND,
                                                                  0,
                                                                  0,
                                                                  2,
                                                                  1);

        // Both the servers have the two versions.
        List<Versioned<byte[]>> vals1 = new ArrayList<Versioned<byte[]>>();
        vals1.add(new Versioned<byte[]>(key, fetchedClock));
        vals1.add(new Versioned<byte[]>(onlineVal, onlineClock1));
        List<Versioned<byte[]>> vals2 = new ArrayList<Versioned<byte[]>>();
        vals2.add(new Versioned<byte[]>(key, fetchedClock));
        vals2.add(new Versioned<byte[]>(onlineVal, onlineClock1));

        // job fixes server 1
        vals1 = pruneAndResolve(vals1, new MutableBoolean());
        assertEquals("Must have one winning version", 1, vals1.size());
        assertEquals("Must resolve to onlineClock", onlineClock1, vals1.get(0).getVersion());

        // reads only from pruned server
        List<Versioned<byte[]>> resolvedVersions = resolver.resolveConflicts(vals1);
        assertEquals("Must read out latest timestamp version",
                     onlineClock1.getTimestamp(),
                     ((VectorClock) resolvedVersions.get(0).getVersion()).getTimestamp());
        assertEquals("Online value to be read out",
                     new String(onlineVal),
                     new String(resolvedVersions.get(0).getValue()));

        // reads only from non pruned server
        resolvedVersions = resolver.resolveConflicts(vals2);
        // Note : check on timestamp is meaningless when actual merging is
        // involved , since merging clocks will result in
        // System.currentTimeMillis() being used for the merged vector clock
        assertEquals("Online value to be read out",
                     new String(onlineVal),
                     new String(resolvedVersions.get(0).getValue()));

        // reads combining both
        List<Versioned<byte[]>> vals = new ArrayList<Versioned<byte[]>>();
        vals.addAll(vals1);
        vals.addAll(vals2);
        resolvedVersions = resolver.resolveConflicts(vals);
        assertEquals("Online value to be read out",
                     new String(onlineVal),
                     new String(resolvedVersions.get(0).getValue()));

    }

    @After
    public void teardown() {

    }
}

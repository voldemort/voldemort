package voldemort.store.routed;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class ZoneAffinityGetVersionsTest extends AbstractZoneAffinityTest {
    public ZoneAffinityGetVersionsTest(Integer clientZoneId, Cluster cluster, List<StoreDefinition> storeDefs) {
        super(clientZoneId, cluster, storeDefs);
    }

    @Override
    public void setupZoneAffinitySettings() {
        clientConfig.getZoneAffinity().setEnableGetVersionsOpZoneAffinity(true);
    }

    @Test
    public void testAllUp() {
        try {
            client.getVersions("K1");
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testLocalZoneDown() {
        for(Integer nodeId: cluster.getNodeIdsInZone(clientZoneId)) {
            this.vservers.get(nodeId).stop();
        }
        try {
            client.getVersions("K1");
            fail("Did not fail fast");
        } catch(InsufficientOperationalNodesException e) {

        }
    }

    @Test
    public void testLocalZonePartialDownSufficientReads() {
        // turn off one node in same zone as client so that reads can still
        // complete
        this.vservers.get(cluster.getNodeIdsInZone(clientZoneId).iterator().next()).stop();
        try {
            client.getVersions("K1");
        } catch(InsufficientOperationalNodesException e) {
            fail("Failed with exception: " + e);
        }
    }

    @Test
    public void testLocalZonePartialDownInSufficientReads() {
        // Stop all but one node in same zone as client. This is not sufficient
        // for zone reads.
        Set<Integer> nodeIds = cluster.getNodeIdsInZone(clientZoneId);
        nodeIds.remove(nodeIds.iterator().next());
        for(Integer nodeId: nodeIds) {
            this.vservers.get(nodeId).stop();
        }
        try {
            client.getVersions("K1");
            fail("Did not fail fast");
        } catch(InsufficientOperationalNodesException e) {

        }
    }
}

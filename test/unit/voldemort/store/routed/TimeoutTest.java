package voldemort.store.routed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import voldemort.VoldemortException;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * 
 * Test the configured timeout operate as expected.
 * 
 * @author jtuberville
 * 
 */
public class TimeoutTest extends TestCase {

    class SlowStore implements Store<ByteArray, byte[]> {

        private final long delay;

        SlowStore(long delay) {
            this.delay = delay;
        }

        public void close() throws VoldemortException {
            throw new UnsupportedOperationException();
        }

        public boolean delete(ByteArray key, Version version) throws VoldemortException {
            throw new UnsupportedOperationException();
        }

        public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
            throw new UnsupportedOperationException();
        }

        public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
                throws VoldemortException {
            throw new UnsupportedOperationException();
        }

        public Object getCapability(StoreCapabilityType capability) {
            throw new UnsupportedOperationException();
        }

        public String getName() {
            throw new UnsupportedOperationException();
        }

        public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
            try {
                Thread.sleep(delay);
            } catch(InterruptedException e) {
                throw new VoldemortException(e);
            }
        }

    }

    public void testPutTimeout() {
        int timeout = 50;
        StoreDefinition definition = new StoreDefinition("test",
                                                         "foo",
                                                         new SerializerDefinition("test"),
                                                         new SerializerDefinition("test"),
                                                         RoutingTier.CLIENT,
                                                         RoutingStrategyType.CONSISTENT_STRATEGY,
                                                         3,
                                                         3,
                                                         3,
                                                         3,
                                                         3,
                                                         0);
        Map<Integer, Store<ByteArray, byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[]>>();
        List<Node> nodes = new ArrayList<Node>();
        int totalDelay = 0;
        for(int i = 0; i < 3; i++) {
            int delay = 4 + i * timeout;
            totalDelay += delay;
            stores.put(i, new SlowStore(delay));
            List<Integer> partitions = Arrays.asList(i);
            nodes.add(new Node(i, "none", 0, 0, partitions));
        }

        RoutedStore routedStore = new RoutedStore("test",
                                                  stores,
                                                  new Cluster("test", nodes),
                                                  definition,
                                                  3,
                                                  false,
                                                  timeout);

        long start = System.currentTimeMillis();
        try {
            routedStore.put(new ByteArray("test".getBytes()),
                            new Versioned<byte[]>(new byte[] { 1 }));
            fail("Should have thrown");
        } catch(InsufficientOperationalNodesException e) {
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed + " < " + totalDelay, elapsed < totalDelay);
        }

    }
}

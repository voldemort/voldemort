package voldemort.scheduled;

import static voldemort.TestUtils.bytesEqual;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.server.scheduler.SlopPusherJob;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.versioning.Versioned;

public class SlopPusherTest extends TestCase {
	
	private StorageEngine<byte[],Slop> slopStore;
	private Map<Integer,Store<byte[],byte[]>> stores;
	private SlopPusherJob pusher;
	private Random random;
	
	public SlopPusherTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		slopStore = new InMemoryStorageEngine<byte[],Slop>("slop");
		stores = new HashMap<Integer,Store<byte[],byte[]>>();
		stores.put(0, new InMemoryStorageEngine<byte[],byte[]>("0"));
		stores.put(1, new InMemoryStorageEngine<byte[],byte[]>("1"));
		stores.put(2, new InMemoryStorageEngine<byte[],byte[]>("2"));
		pusher = new SlopPusherJob(slopStore, stores);
		random = new Random();
	}
	
	private Slop randomSlop(String name, int nodeId) {
	    return new Slop(name, Operation.PUT, TestUtils.randomBytes(10), TestUtils.randomBytes(10), nodeId, new Date());
	}
	
	private void testPush(Versioned<Slop>...slops) {
	    // put all the slop in the slop store
	    for(Versioned<Slop> s: slops)
	        slopStore.put(s.getValue().makeKey(), s);
	    
	    // run the pusher
	    pusher.run();
	    
	    // now all the slop should be gone and the various stores should have those items
	    for(Versioned<Slop> vs: slops) {
	        // check that all the slops are in the stores
	        // and no new slops have appeared
	        // and the SloppyStore is now empty
	        Slop slop = vs.getValue();
	        assertEquals("Slop remains.", 0, slopStore.get(slop.makeKey()).size());
	        assertTrue(bytesEqual(slop.getValue(), stores.get(slop.getNodeId()).get(slop.makeKey()).get(0).getValue()));
	    }
	}

	@SuppressWarnings("unchecked")
	public void testPushSingleSlop() {
	    testPush(new Versioned<Slop>(randomSlop("0", 0)));
	}
}

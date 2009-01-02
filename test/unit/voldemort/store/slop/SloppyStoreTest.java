package voldemort.store.slop;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.ByteArrayStoreTest;
import voldemort.store.FailingStore;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.Versioned;


public class SloppyStoreTest extends ByteArrayStoreTest {
    
    private static final byte[] testVal = "test".getBytes();
    private static final int NODE_ID = 0;

	@Override
    @SuppressWarnings("unchecked")
	public Store<byte[],byte[]> getStore() {
	    Collection<InMemoryStorageEngine<byte[],Slop>> backups = Arrays.asList(new InMemoryStorageEngine<byte[],Slop>("test"));
	    return new SloppyStore(NODE_ID, new InMemoryStorageEngine<byte[],byte[]>("test"), backups);
	}
	
    @SuppressWarnings("unchecked")
    public SloppyStore getSloppyStore(Store<byte[],byte[]> store) {
        Collection<InMemoryStorageEngine<byte[],Slop>> backups = Arrays.asList(new InMemoryStorageEngine<byte[],Slop>("test"));
        return new SloppyStore(NODE_ID, store, backups);
    }
    
    private void assertBackupHasOperation(Slop slop, List<Store<byte[],Slop>> backups) {
        for(Store<byte[],Slop> backup: backups) {
            List<Versioned<Slop>> slops = backup.get(slop.makeKey());
            for(Versioned<Slop> found: slops) {
                Slop foundSlop = found.getValue();
                if(foundSlop.getKey().equals(slop.getKey()) &&
                   TestUtils.bytesEqual(foundSlop.getValue(), slop.getValue()) &&
                   foundSlop.getOperation().equals(slop.getOperation()))
                    return;
            }
        }
        fail("Could not find slop " + slop + " in backup stores.");
    }
    
    public void testFailingStore() {
        SloppyStore store = getSloppyStore(new FailingStore<byte[],byte[]>("test", new UnreachableStoreException("Unreachable store.")));
        try {
            store.put(testVal, new Versioned<byte[]>(testVal));
            fail("Failing store doesn't fail.");
        } catch(UnreachableStoreException e) {
            Slop slop = new Slop("test", Slop.Operation.PUT, testVal, testVal, NODE_ID, new Date());
            assertBackupHasOperation(slop, store.getBackupStores());
        }
    }

}

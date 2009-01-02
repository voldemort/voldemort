package voldemort.scheduled;

import java.util.List;
import java.util.concurrent.Semaphore;

import junit.framework.TestCase;
import voldemort.MockTime;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.store.StorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class DataCleanupJobTest extends TestCase {

    private MockTime time;
    private StorageEngine<String, String> engine;

    public void setUp() {
        time = new MockTime();
        engine = new InMemoryStorageEngine<String, String>("test");
    }

    public void testCleanupCleansUp() {
        time.setTime(123);
        put("a", "b", "c");
        time.setTime(123 + Time.MS_PER_DAY + 1);
        put("d", "e", "f");
        assertContains("a", "b", "c", "d", "e", "f");

        // update a single item to bump its vector clock time
        put("a");

        // now run cleanup
        new DataCleanupJob<String, String>(engine, new Semaphore(1), Time.MS_PER_DAY, time).run();

        // Check that all the later keys are there AND the key updated later
        assertContains("a", "d", "e", "f");
    }

    private void put(String... items) {
        for(String item : items) {
            VectorClock clock = null;
            List<Versioned<String>> found = engine.get(item);
            if(found.size() == 0) {
                clock = new VectorClock(time.getMilliseconds());
            } else if(found.size() == 1) {
                VectorClock oldClock = (VectorClock) found.get(0).getVersion();
                clock = oldClock.incremented(0, time.getMilliseconds());
            } else {
                fail("Found multiple versions.");
            }
            engine.put(item, new Versioned<String>(item, clock));
        }
    }

    private void assertContains(String... keys) {
        for(String key : keys) {
            List<Versioned<String>> found = engine.get(key);
            assertTrue("Did not find key '" + key + "' in store!", found.size() > 0);
        }
    }

}

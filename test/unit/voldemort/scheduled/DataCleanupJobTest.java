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

package voldemort.scheduled;

import java.util.List;

import junit.framework.TestCase;
import voldemort.MockTime;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.EventThrottler;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class DataCleanupJobTest extends TestCase {

    private MockTime time;
    private StorageEngine<String, String, String> engine;

    @Override
    public void setUp() {
        time = new MockTime();
        engine = new InMemoryStorageEngine<String, String, String>("test");
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
        new DataCleanupJob<String, String, String>(engine,
                                                   new ScanPermitWrapper(1),
                                                   Time.MS_PER_DAY,
                                                   time,
                                                   new EventThrottler(1)).run();

        // Check that all the later keys are there AND the key updated later
        assertContains("a", "d", "e", "f");
    }

    private void put(String... items) {
        for(String item: items) {
            VectorClock clock = null;
            List<Versioned<String>> found = engine.get(item, null);
            if(found.size() == 0) {
                clock = new VectorClock(time.getMilliseconds());
            } else if(found.size() == 1) {
                VectorClock oldClock = (VectorClock) found.get(0).getVersion();
                clock = oldClock.incremented(0, time.getMilliseconds());
            } else {
                fail("Found multiple versions.");
            }
            engine.put(item, new Versioned<String>(item, clock), null);
        }
    }

    private void assertContains(String... keys) {
        for(String key: keys) {
            List<Versioned<String>> found = engine.get(key, null);
            assertTrue("Did not find key '" + key + "' in store!", found.size() > 0);
        }
    }

}

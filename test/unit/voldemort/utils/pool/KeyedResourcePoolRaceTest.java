/*
 * Copyright 2012 LinkedIn, Inc
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
package voldemort.utils.pool;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class KeyedResourcePoolRaceTest extends KeyedResourcePoolBaseTest {

    protected static final int POOL_SIZE = 100;
    protected static final long TIMEOUT_MS = 500;
    protected static final long NUM_TESTS = 250;

    @Before
    public void setUp() {
        factory = new TestResourceFactory();
        config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                         .setTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.pool = new KeyedResourcePool<String, TestResource>(factory, config);
    }

    // See http://code.google.com/p/project-voldemort/issues/detail?id=276
    @Test
    public void testAttemptGrow() {
        ExecutorService service = Executors.newFixedThreadPool(POOL_SIZE);
        for(int i = 0; i < NUM_TESTS; i++) {
            final CountDownLatch checkouts = new CountDownLatch(POOL_SIZE);
            List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(POOL_SIZE);
            for(int t = 0; t < POOL_SIZE; t++) {
                tasks.add(new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        try {
                            TestResource resource = pool.checkout("a");
                            checkouts.countDown();
                            checkouts.await();
                            resource.invalidate();
                            pool.checkin("a", resource);
                            return true;
                        } catch(Exception e) {
                            checkouts.countDown();
                            throw e;
                        }
                    }
                });
            }
            try {
                List<Future<Boolean>> futures = service.invokeAll(tasks);
                for(Future<Boolean> future: futures) {
                    assertTrue(future.get());
                }
            } catch(Exception e) {
                fail("Unexpected exception - " + e.getMessage());
            }
        }
    }
}

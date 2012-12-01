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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

public class KeyedResourcePoolStressTest {

    protected static final int POOL_SIZE = 100;
    protected static final long TIMEOUT_MS = 500;
    protected static final long NUM_TESTS = 5000;

    protected TestResourceFactory factory;
    protected KeyedResourcePool<String, TestResource> pool;
    protected ResourcePoolConfig config;

    @Before
    public void setUp() {
        factory = new TestResourceFactory();
        config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                         .setTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.pool = new KeyedResourcePool<String, TestResource>(factory, config);
    }

    @Test
    public void testAttemptGrow() {
        ExecutorService service = Executors.newFixedThreadPool(POOL_SIZE);
        for(int i = 0; i < NUM_TESTS; i++) {
            if(i % 100 == 0) {
                System.out.println("Test run: " + i);
            }
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

    protected static class TestResource {

        private String value;
        private AtomicBoolean isValid;
        private AtomicBoolean isDestroyed;

        public TestResource(String value) {
            this.value = value;
            this.isValid = new AtomicBoolean(true);
            this.isDestroyed = new AtomicBoolean(false);
        }

        public boolean isValid() {
            return isValid.get();
        }

        public void invalidate() {
            this.isValid.set(false);
        }

        public boolean isDestroyed() {
            return isDestroyed.get();
        }

        public void destroy() {
            this.isDestroyed.set(true);
        }

        @Override
        public String toString() {
            return "TestResource(" + value + ")";
        }

    }

    protected static class TestResourceFactory implements ResourceFactory<String, TestResource> {

        private final AtomicInteger created = new AtomicInteger(0);
        private final AtomicInteger destroyed = new AtomicInteger(0);
        private Exception createException;
        private Exception destroyException;
        private boolean isCreatedValid = true;

        @Override
        public TestResource create(String key) throws Exception {
            if(createException != null)
                throw createException;
            TestResource r = new TestResource(Integer.toString(created.getAndIncrement()));
            if(!isCreatedValid)
                r.invalidate();
            return r;
        }

        @Override
        public void destroy(String key, TestResource obj) throws Exception {
            if(destroyException != null)
                throw destroyException;
            destroyed.incrementAndGet();
            obj.destroy();
        }

        @Override
        public boolean validate(String key, TestResource value) {
            return value.isValid();
        }

        public int getCreated() {
            return this.created.get();
        }

        public int getDestroyed() {
            return this.destroyed.get();
        }

        public void setDestroyException(Exception e) {
            this.destroyException = e;
        }

        public void setCreateException(Exception e) {
            this.createException = e;
        }

        public void setCreatedValid(boolean isValid) {
            this.isCreatedValid = isValid;
        }

        @Override
        public void close() {}

    }

}

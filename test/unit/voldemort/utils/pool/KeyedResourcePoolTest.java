package voldemort.utils.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

public class KeyedResourcePoolTest extends TestCase {

    private static int POOL_SIZE = 5;
    private static long TIMEOUT_MS = 100;

    private TestResourceFactory factory;
    private KeyedResourcePool<String, TestResource> pool;

    @Override
    public void setUp() {
        factory = new TestResourceFactory();
        ResourcePoolConfig config = new ResourcePoolConfig();
        config.setPoolSize(POOL_SIZE);
        config.setTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.pool = new KeyedResourcePool<String, TestResource>(factory, config);
    }

    public void testPoolingOccurs() throws Exception {
        TestResource r1 = this.pool.checkout("a");
        this.pool.checkin("a", r1);
        TestResource r2 = this.pool.checkout("a");
        assertTrue("Checked out value should equal checked in value (found " + r1 + ", " + r2 + ")",
                   r1 == r2);
    }

    public void testFullPoolBlocks() throws Exception {
        for(int i = 0; i < POOL_SIZE; i++)
            this.pool.checkout("a");

        try {
            this.pool.checkout("a");
            fail("Checking out more items than in the pool should timeout.");
        } catch(TimeoutException e) {
            // this is good
        }
    }

    public void testInvalidIsDestroyed() throws Exception {
        TestResource r1 = this.pool.checkout("a");
        r1.invalidate();
        this.pool.checkin("a", r1);
        TestResource r2 = this.pool.checkout("a");
        assertTrue("Invalid objects should be destroyed.", r1 != r2);
        assertTrue("Invalid objects should be destroyed.", r1.isDestroyed());
    }

    public void testMultithreaded() throws Exception {
        int numThreads = POOL_SIZE * 2;
        final String[] keys = new String[numThreads * 2];
        for(int i = 0; i < keys.length; i++)
            keys[i] = Integer.toString(i);

        final AtomicInteger totalExecutions = new AtomicInteger(0);
        final AtomicInteger destroyed = new AtomicInteger(0);
        final AtomicBoolean isStopped = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for(int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {

                public void run() {
                    while(!isStopped.get()) {
                        int curr = totalExecutions.getAndIncrement();
                        String key = keys[curr % keys.length];
                        try {
                            TestResource r = pool.checkout(key);
                            assertTrue(r.isValid());
                            if(curr % 10021 == 0) {
                                r.invalidate();
                                destroyed.getAndIncrement();
                            }
                            pool.checkin(key, r);
                        } catch(Exception e) {
                            fail("Unexpected exception: " + e);
                        }
                    }
                }
            });
        }
        Thread.sleep(1000);
        isStopped.set(true);
        Thread.sleep(200);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(100, TimeUnit.MILLISECONDS));
        pool.close();
        assertEquals(factory.getCreated(), factory.getDestroyed());
    }

    private static class TestResource {

        private String value;
        private AtomicBoolean isValid;
        private AtomicBoolean isDestroyed;

        public TestResource(String value) {
            this.value = value;
            this.isValid = new AtomicBoolean(true);
            this.isDestroyed = new AtomicBoolean(true);
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
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

    private static class TestResourceFactory implements ResourceFactory<String, TestResource> {

        private final AtomicInteger created = new AtomicInteger(0);
        private final AtomicInteger destroyed = new AtomicInteger(0);

        public TestResource create(String key) throws Exception {
            TestResource r = new TestResource(Integer.toString(created.getAndIncrement()));
            return r;
        }

        public void destroy(String key, TestResource obj) throws Exception {
            destroyed.incrementAndGet();
            obj.destroy();
        }

        public boolean validate(String key, TestResource value) {
            return value.isValid();
        }

        public int getCreated() {
            return this.created.get();
        }

        public int getDestroyed() {
            return this.destroyed.get();
        }

    }

}

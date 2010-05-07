package voldemort.utils.pool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

public class KeyedResourcePoolTest extends TestCase {

    private static int POOL_SIZE = 5;
    private static long TIMEOUT_MS = 100;
    private static int MAX_ATTEMPTS = 10;

    private TestResourceFactory factory;
    private KeyedResourcePool<String, TestResource> pool;

    @Override
    public void setUp() {
        factory = new TestResourceFactory();
        ResourcePoolConfig config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                                            .setTimeout(TIMEOUT_MS,
                                                                        TimeUnit.MILLISECONDS)
                                                            .setMaxInvalidAttempts(MAX_ATTEMPTS);
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

    public void testExceptions() throws Exception {
        // we should start with an empty pool
        assertEquals(0, this.pool.getTotalResourceCount());

        Exception toThrow = new Exception("An exception!");

        // test exception on destroy
        TestResource checkedOut = this.pool.checkout("a");
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());
        this.factory.setDestroyException(toThrow);
        try {
            this.pool.checkin("a", checkedOut);
            // checking out again should force destroy
            this.pool.checkout("a");
            assertTrue(checkedOut.isDestroyed());
        } catch(Exception caught) {
            fail("No exception expected.");
        }
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());

        this.factory.setCreateException(toThrow);
        try {
            this.pool.checkout("b");
            fail("Excpected exception!");
        } catch(Exception caught) {
            assertEquals("The exception thrown by the factory should propage to the caller.",
                         toThrow,
                         caught);
        }
        // failed checkout shouldn't effect count
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());
    }

    public void testInvalidIsDestroyed() throws Exception {
        TestResource r1 = this.pool.checkout("a");
        r1.invalidate();
        this.pool.checkin("a", r1);
        TestResource r2 = this.pool.checkout("a");
        assertTrue("Invalid objects should be destroyed.", r1 != r2);
        assertTrue("Invalid objects should be destroyed.", r1.isDestroyed());
    }

    public void testMaxInvalidCreations() throws Exception {
        this.factory.setCreatedValid(false);
        try {
            this.pool.checkout("a");
            fail("Exceeded max failed attempts without exception.");
        } catch(ExcessiveInvalidResourcesException e) {
            // this is expected
        }
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
        private Exception createException;
        private Exception destroyException;
        private boolean isCreatedValid = true;

        public TestResource create(String key) throws Exception {
            if(createException != null)
                throw createException;
            TestResource r = new TestResource(Integer.toString(created.getAndIncrement()));
            if(!isCreatedValid)
                r.invalidate();
            return r;
        }

        public void destroy(String key, TestResource obj) throws Exception {
            if(destroyException != null)
                throw destroyException;
            destroyed.incrementAndGet();
            obj.destroy();
        }

        public boolean validate(String key, TestResource value) {
            return value.isValid();
        }

        @SuppressWarnings("unused")
        public int getCreated() {
            return this.created.get();
        }

        @SuppressWarnings("unused")
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

        public void close() {}

    }

}

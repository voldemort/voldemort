package voldemort.socketpool;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import voldemort.socketpool.AbstractSocketPoolTest.TestStats;
import voldemort.utils.socketpool.PoolableObjectFactory;
import voldemort.utils.socketpool.ResourcePoolConfig;

public class SimpleSocketPoolTest extends TestCase {

    public void testPoolLimitNoTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig();
        config.setBorrowTimeout(1000);
        config.setBorrowTimeoutUnit(TimeUnit.MILLISECONDS);
        config.setDefaultPoolSize(20);

        PoolableObjectFactory<String, String> factory = PoolableObjectFactoryFactory.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(100);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // borrow timeout >> doSomething() no timeout expected
        TestStats testStats = test.startTest(factory, config, 50, 200);
        assertEquals("We should see Zero timeoutRequests", 0, testStats.timeoutRequests);
    }

    public void testPoolLimitSomeTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig();
        config.setBorrowTimeout(50);
        config.setBorrowTimeoutUnit(TimeUnit.MILLISECONDS);
        config.setDefaultPoolSize(20);

        PoolableObjectFactory<String, String> factory = PoolableObjectFactoryFactory.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(100);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // borrow timeout >> doSomething() no timeout expected
        TestStats testStats = test.startTest(factory, config, 50, 200);
        assertEquals("We should see some timeoutRequests", true, testStats.timeoutRequests > 0);
    }

    public void testNoTimeout() throws Exception {
        final ResourcePoolConfig config = new ResourcePoolConfig();
        config.setBorrowTimeout(100);
        config.setBorrowTimeoutUnit(TimeUnit.MILLISECONDS);
        config.setDefaultPoolSize(20);

        PoolableObjectFactory<String, String> factory = PoolableObjectFactoryFactory.getBasicPoolFactory();
        final AbstractSocketPoolTest<String, String> test = new AbstractSocketPoolTest<String, String>() {

            @Override
            protected void doSomethingWithResource(String key, String resource) throws Exception {
                Thread.sleep(200);
            }

            @Override
            protected String getRequestKey() throws Exception {
                return "test-key";
            }
        };

        // threads same as pool size no timeout expected
        TestStats testStats = test.startTest(factory, config, 20, 200);
        assertEquals("We should see Zero timeoutRequests", 0, testStats.timeoutRequests);
    }
}

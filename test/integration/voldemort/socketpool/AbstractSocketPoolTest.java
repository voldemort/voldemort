package voldemort.socketpool;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import voldemort.utils.socketpool.BlockingKeyedResourcePool;
import voldemort.utils.socketpool.PoolableObjectFactory;
import voldemort.utils.socketpool.ResourcePoolConfig;

public abstract class AbstractSocketPoolTest<K, V> extends TestCase {

    protected class TestStats {

        public int timeoutRequests;

        public TestStats() {
            timeoutRequests = 0;
        }
    }

    public TestStats startTest(final PoolableObjectFactory<K, V> factory,
                               final ResourcePoolConfig config,
                               int nThreads,
                               int nRequests) throws Exception {

        final BlockingKeyedResourcePool<K, V> pool = new BlockingKeyedResourcePool<K, V>(factory,
                                                                                         config);
        final ConcurrentHashMap<K, AtomicInteger> resourceInHand = new ConcurrentHashMap<K, AtomicInteger>();
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        final TestStats testStats = new TestStats();

        for(int i = 0; i < nRequests; i++) {
            final K key = getRequestKey();
            resourceInHand.putIfAbsent(key, new AtomicInteger(0));
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        // borrow resource
                        V resource = pool.borrowResource(key);
                        resourceInHand.get(key).incrementAndGet();
                        System.out.println("Borrowing of " + key + " completed at " + new Date());

                        // assert that resourceInHand is less than equal to pool
                        // Size
                        assertEquals("resources In Hand(" + resourceInHand.get(key).get()
                                             + ") should be less than equal to pool size("
                                             + config.getDefaultPoolSize() + ")",
                                     true,
                                     resourceInHand.get(key).get() <= config.getDefaultPoolSize());

                        // do something
                        doSomethingWithResource(key, resource);

                        // return
                        pool.returnResource(key, resource);
                        resourceInHand.get(key).decrementAndGet();
                        System.out.println("return completed" + key + " resource:" + resource
                                           + " at " + new Date());
                    } catch(TimeoutException e) {
                        // only if alloted resources are same as pool size
                        assertEquals("resources In Hand(" + resourceInHand.get(key).get()
                                             + ") should be same as  pool size("
                                             + config.getDefaultPoolSize() + ")",
                                     config.getDefaultPoolSize(),
                                     resourceInHand.get(key).get());
                        ++testStats.timeoutRequests;
                        System.out.println("saw timeout !!");
                        return;
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        return testStats;
    }

    protected abstract K getRequestKey() throws Exception;

    protected abstract void doSomethingWithResource(K key, V resource) throws Exception;
}

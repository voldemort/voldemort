package voldemort.socketpool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourceFactory;
import voldemort.utils.pool.ResourcePoolConfig;

public abstract class AbstractSocketPoolTest<K, V> extends TestCase {

    protected static class TestStats {

        public int timeoutRequests;

        public TestStats() {
            timeoutRequests = 0;
        }
    }

    public TestStats startTest(final ResourceFactory<K, V> factory,
                               final ResourcePoolConfig config,
                               int nThreads,
                               int nRequests) throws Exception {

        final KeyedResourcePool<K, V> pool = new KeyedResourcePool<K, V>(factory, config);
        final ConcurrentHashMap<K, AtomicInteger> resourceInHand = new ConcurrentHashMap<K, AtomicInteger>();
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        final TestStats testStats = new TestStats();
        final AtomicBoolean passed = new AtomicBoolean(true);
        final AtomicInteger errVal = new AtomicInteger();

        for(int i = 0; i < nRequests; i++) {
            final K key = getRequestKey();
            resourceInHand.putIfAbsent(key, new AtomicInteger(0));
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        // borrow resource
                        V resource = pool.checkout(key);
                        resourceInHand.get(key).incrementAndGet();

                        // check that resourceInHand is less than equal to pool
                        // size
                        if(resourceInHand.get(key).get() > config.getMaxPoolSize()) {
                            passed.set(false);
                            errVal.set(resourceInHand.get(key).get());
                        }

                        // do something
                        doSomethingWithResource(key, resource);

                        // return
                        resourceInHand.get(key).decrementAndGet();
                        pool.checkin(key, resource);
                    } catch(TimeoutException e) {
                        ++testStats.timeoutRequests;
                        return;
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5 * 60, TimeUnit.SECONDS);

        if(!passed.get()) {
            String errMsg = String.format("resources In Hand(%d) should be less than equal to pool size(%d)",
                                          errVal.get(),
                                          config.getMaxPoolSize());
            fail(errMsg);
        }
        return testStats;
    }

    protected abstract K getRequestKey() throws Exception;

    protected abstract void doSomethingWithResource(K key, V resource) throws Exception;
}

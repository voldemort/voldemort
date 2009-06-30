package voldemort;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

/**
 * Commons-pool-1.5.1 is causing some socket exceptions at Linkedin duiring test
 * trial.<br>
 * The test scenario is
 * 
 * <br>
 * Client --> socket call to server A and B (preferred reads = 2) <br>
 * B is slow , A is fast to response <br>
 * Common-Pools-1.4 : Client starts seeing Socket exception from B but continue
 * to serve from A (As I expect) <br>
 * Common-Pools-1.5 : both A and B start throwing socket exception and client
 * see request failures.
 * 
 * @author bbansal
 * 
 */
public class CommonsPoolDeadlockTest {

    public static void main(String[] args) throws InterruptedException {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        config.maxActive = 5;
        config.maxTotal = 10;
        config.maxIdle = 5;
        config.maxWait = 500;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
        KeyedPoolableObjectFactory objFactory = new DummyPoolableObjectFactory();
        final KeyedObjectPool pool = new GenericKeyedObjectPool(objFactory, config);
        final AtomicInteger completed = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        System.out.println("You should now see 20 quick checkouts in rapid succession:");
        for(int i = 0; i < 20; i++) {
            final String key = Integer.toString(i);
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        System.out.println(new Date() + " borrow " + key);
                        Object obj = pool.borrowObject(key);
                        if(Integer.parseInt(key) % 2 == 0) {
                            Thread.sleep(5000);
                        }
                        pool.returnObject(key, obj);
                        System.out.println(new Date() + " Returned " + key);
                    } catch(Exception e) {
                        synchronized(this) {
                            System.out.println("Exception for key:" + key);
                            e.printStackTrace();
                        }
                    } finally {
                        System.out.println(completed.incrementAndGet());
                    }
                }
            });
        }

        executor.awaitTermination(10, TimeUnit.MINUTES);
        executor.shutdown();
        if(completed.get() == 20) {
            System.out.println("Test completed.");
        }
    }

    private static class DummyPoolableObjectFactory implements KeyedPoolableObjectFactory {

        public void activateObject(Object k, Object v) throws Exception {}

        public void destroyObject(Object k, Object v) throws Exception {}

        // build fast in each case
        public Object makeObject(Object k) throws Exception {
            return new Object();
        }

        public void passivateObject(Object k, Object v) throws Exception {}

        public boolean validateObject(Object k, Object v) {
            return true;
        }
    }
}

package voldemort;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

public class CommonsPoolTest {

    public static void main(String[] args) {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        config.maxActive = 100;
        config.maxTotal = 100;
        config.maxIdle = 100;
        config.maxWait = 1000000;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
        KeyedPoolableObjectFactory objFactory = new TestPoolableObjectFactory();
        final KeyedObjectPool pool = new GenericKeyedObjectPool(objFactory, config);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        System.out.println("You should now see 20 quick checkouts in rapid succession:");
        for(int i = 0; i < 20; i++) {
            final String key = Integer.toString(i);
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        System.out.println("Trying to borrow " + key + " at " + new Date());
                        pool.borrowObject(key);
                        System.out.println("Borrowing of " + key + " completed at " + new Date());
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private static class TestPoolableObjectFactory implements KeyedPoolableObjectFactory {

        private AtomicBoolean flag = new AtomicBoolean(false);

        public void activateObject(Object k, Object v) throws Exception {}

        public void destroyObject(Object k, Object v) throws Exception {}

        public Object makeObject(Object k) throws Exception {
            boolean isFirst = flag.compareAndSet(false, true);
            if(isFirst)
                Thread.sleep(20 * 1000);
            return new Object();
        }

        public void passivateObject(Object k, Object v) throws Exception {}

        public boolean validateObject(Object k, Object v) {
            return true;
        }

    }

}

package voldemort.performance;

import java.text.NumberFormat;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourceFactory;
import voldemort.utils.pool.ResourcePoolConfig;

public class ResourcePoolPerfTest {

    public static void main(String[] args) throws Exception {

        final int numKeys = 10;
        final int numThreads = 10;
        final int numRequests = 10000000;
        NumberFormat format = NumberFormat.getInstance();
        format.setMaximumFractionDigits(2);

        for(int poolSize: new int[] { 1, 5, 10 }) {
            System.out.println("Perf test for voldemort pool with numThreads = " + numThreads
                               + ", poolSize = " + poolSize + ":");
            final KeyedResourcePool<Integer, String> pool = KeyedResourcePool.create(new StringResourceFactory(),
                                                                                     new ResourcePoolConfig().setMaxPoolSize(poolSize)
                                                                                                             .setIsFair(true));
            PerformanceTest test = new PerformanceTest() {

                @Override
                public void doOperation(int id) throws Exception {
                    Integer key = id % numKeys;
                    String s = pool.checkout(key);
                    pool.checkin(key, s);
                }
            };
            test.run(numRequests, numThreads);
            test.printStats();
            System.out.println();
        }

        System.out.println("--------------------------------------");
        System.out.println();

        for(int poolSize: new int[] { 1, 5, 10 }) {
            System.out.println("Perf test for commons pool with numThreads = " + numThreads
                               + ", poolSize = " + poolSize + ":");
            GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
            config.maxActive = poolSize;
            config.testOnBorrow = true;
            config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
            config.maxWait = 10000;
            StringPoolableObjectFactory objFactory = new StringPoolableObjectFactory();
            final KeyedObjectPool pool = new GenericKeyedObjectPool(objFactory, config);
            PerformanceTest test = new PerformanceTest() {

                @Override
                public void doOperation(int id) throws Exception {
                    Integer key = id % numKeys;
                    String s = (String) pool.borrowObject(key);
                    pool.returnObject(key, s);
                }
            };
            test.run(numRequests, numThreads);
            test.printStats();
            System.out.println();
        }

    }

    private static class StringResourceFactory implements ResourceFactory<Integer, String> {

        public String create(Integer key) {
            return new String(key + "-val");
        }

        public void destroy(Integer key, String obj) {}

        public boolean validate(Integer key, String value) {
            return true;
        }
    }

    private static class StringPoolableObjectFactory implements KeyedPoolableObjectFactory {

        public void activateObject(Object k, Object v) {}

        public void passivateObject(Object k, Object v) {}

        public void destroyObject(Object k, Object v) {}

        public Object makeObject(Object k) {
            return new String(k + "-val");
        }

        public boolean validateObject(Object k, Object v) {
            return true;
        }

    }

}

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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.common.VoldemortOpCode;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class SlowStorageEngineTest extends AbstractStorageEngineTest {

    private static final Logger logger = Logger.getLogger(KeyedResourcePool.class.getName());

    private StorageEngine<ByteArray, byte[], byte[]> store;
    private final List<Byte> opList;

    public SlowStorageEngineTest() {
        opList = new ArrayList<Byte>();
        opList.add(VoldemortOpCode.GET_OP_CODE);
        opList.add(VoldemortOpCode.GET_VERSION_OP_CODE);
        opList.add(VoldemortOpCode.GET_ALL_OP_CODE);
        opList.add(VoldemortOpCode.PUT_OP_CODE);
        opList.add(VoldemortOpCode.DELETE_OP_CODE);
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Do not change the magic constants in the next two constructors! The
        // unit tests assert on specific delays occurring.
        SlowStorageEngine.OperationDelays queued = new SlowStorageEngine.OperationDelays(10,
                                                                                         20,
                                                                                         30,
                                                                                         40,
                                                                                         50);
        SlowStorageEngine.OperationDelays concurrent = new SlowStorageEngine.OperationDelays(50,
                                                                                             40,
                                                                                             30,
                                                                                             20,
                                                                                             10);
        this.store = new SlowStorageEngine<ByteArray, byte[], byte[]>("test", queued, concurrent);
    }

    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(new ByteArray(TestUtils.randomBytes(10)));
        return keys;
    }

    // Two modes for the runnable: give it a time to check (expectedTimeMs) or
    // give it a concurrent queue upon which to add its run time.
    public class OpInvoker implements Runnable {

        private final CountDownLatch signal;
        private final byte opCode;

        private long expectedTimeMs;
        private ConcurrentLinkedQueue<Long> runTimes;

        private final ByteArray key;
        private final byte[] value;

        protected OpInvoker(CountDownLatch signal, byte opCode) {
            this.signal = signal;
            this.opCode = opCode;

            this.expectedTimeMs = -1;
            this.runTimes = null;

            this.key = new ByteArray(ByteUtils.getBytes("key", "UTF-8"));
            this.value = ByteUtils.getBytes("value", "UTF-8");
            logger.debug("OpInvoker created for operation " + getOpName() + "(Thread: "
                         + Thread.currentThread().getName() + ")");
        }

        // expectedTimeMs <= 0 means not checking the runtime
        OpInvoker(CountDownLatch signal, byte opCode, long expectedTimeMs) {
            this(signal, opCode);
            this.expectedTimeMs = expectedTimeMs;
        }

        OpInvoker(CountDownLatch signal, byte opCode, ConcurrentLinkedQueue<Long> runTimes) {
            this(signal, opCode);
            this.runTimes = runTimes;
        }

        private String getOpName() {
            switch(this.opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    return "Get";
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    return "GetVersion";
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    return "GetAll";
                case VoldemortOpCode.DELETE_OP_CODE:
                    return "Delete";
                case VoldemortOpCode.PUT_OP_CODE:
                    return "Put";
                default:
                    logger.error("getOpName invoked with bad operation code: " + opCode);
            }
            return null;
        }

        private void doGet() {
            store.get(key, null);
        }

        private void doGetAll() {
            List<ByteArray> keys = new ArrayList<ByteArray>();
            keys.add(key);
            store.getAll(keys, null);
        }

        private void doGetVersion() {
            store.getVersions(key);
        }

        private void doPut() {
            try {
                store.put(key, new Versioned<byte[]>(value), null);
            } catch(ObsoleteVersionException e) {
                // This exception is expected in some tests.
            }
        }

        private void doDelete() {
            store.delete(key, new VectorClock());
        }

        public void run() {
            long startTimeNs = System.nanoTime();

            switch(this.opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    doGet();
                    break;
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    doGetVersion();
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    doGetAll();
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    doPut();
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    doDelete();
                    break;
                default:
                    logger.error("OpInvoker issued with bad operation code: " + this.opCode);
            }
            long runTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);

            if(this.expectedTimeMs > 0) {
                String details = "(runTimeMs: " + runTimeMs + ", Thread: "
                                 + Thread.currentThread().getName() + ")";
                assertFalse("OpInvoker operation time is bad " + details,
                            !isRunTimeBad(runTimeMs, this.expectedTimeMs));
            }

            if(this.runTimes != null)
                runTimes.add(runTimeMs);
            logger.debug("OpInvoker finished operation " + getOpName() + "(Thread: "
                         + Thread.currentThread().getName() + ")");
            signal.countDown();
        }
    }

    // true if runtime is not within a "reasonable" range. Reasonable
    // defined by a 10% fudge factor.
    private boolean isRunTimeBad(long runTimeMs, long expectedTimeMs) {
        if((runTimeMs < expectedTimeMs || runTimeMs > (expectedTimeMs * 1.1))) {
            return false;
        }
        return true;
    }

    /**
     * Test the time of each op type individually.
     */
    @Test
    public void testEachOpTypeIndividually() {
        for(int i = 0; i < 5; ++i) {
            long timeoutMs = 60;
            if(i == 0)
                timeoutMs = 0;

            for(byte op: opList) {
                CountDownLatch waitForOp = new CountDownLatch(1);
                new Thread(new OpInvoker(waitForOp, op, timeoutMs)).start();
                try {
                    waitForOp.await();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * Test repeated operations.
     */
    @Test
    public void testEachOpTypeRepeated() {
        // Magic number '2': Run once to warm up, run again and test asserts on
        // second pass
        for(int j = 0; j < 2; j++) {
            for(byte op: opList) {
                ConcurrentLinkedQueue<Long> runTimes = new ConcurrentLinkedQueue<Long>();
                CountDownLatch waitForOps = new CountDownLatch(5 + 1);
                for(int i = 0; i < 5; ++i) {
                    new Thread(new OpInvoker(waitForOps, op, runTimes)).start();
                }

                waitForOps.countDown();
                try {
                    waitForOps.await();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }

                // Test runs after the single warm up run.
                if(j > 0) {
                    // Determine what the longest delay should be and test the
                    // maximum delay against that value. The magic constants
                    // used to construct the SlowStorageEngine determine the
                    // longest delay.
                    Long[] allTimes = runTimes.toArray(new Long[0]);
                    Arrays.sort(allTimes);
                    long maxTimeMs = allTimes[4];
                    long expectedTimeMs = 0;
                    switch(op) {
                        case VoldemortOpCode.GET_OP_CODE:
                            expectedTimeMs = (5 * 10) + 50;
                            break;
                        case VoldemortOpCode.GET_VERSION_OP_CODE:
                            expectedTimeMs = (5 * 20) + 40;
                            break;
                        case VoldemortOpCode.GET_ALL_OP_CODE:
                            expectedTimeMs = (5 * 30) + 30;
                            break;
                        case VoldemortOpCode.PUT_OP_CODE:
                            expectedTimeMs = (5 * 40) + 20;
                            break;
                        case VoldemortOpCode.DELETE_OP_CODE:
                            expectedTimeMs = (5 * 50) + 10;
                            break;
                    }
                    String details = "(maxTimeMs: " + maxTimeMs + ", " + expectedTimeMs + ")";
                    assertFalse("OpInvoker operation time is bad " + details,
                                !isRunTimeBad(maxTimeMs, expectedTimeMs));
                }
            }
        }
    }

}

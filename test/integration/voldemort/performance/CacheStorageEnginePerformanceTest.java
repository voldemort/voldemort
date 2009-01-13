/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.performance;

import java.util.concurrent.atomic.AtomicInteger;

import voldemort.store.ObsoleteVersionException;
import voldemort.store.Store;
import voldemort.store.memory.CacheStorageConfiguration;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class CacheStorageEnginePerformanceTest {

    public static void main(String[] args) {
        if(args.length != 3)
            Utils.croak("USAGE: java " + CacheStorageEnginePerformanceTest.class.getName()
                        + " num-threads num-requests read-fraction");
        int numThreads = Integer.parseInt(args[0]);
        int numRequests = Integer.parseInt(args[1]);
        double readPercent = Double.parseDouble(args[2]);
        final int valueRange = numRequests / 10;
        final int mod = 100;
        final int readMax = (int) readPercent * mod;

        final Store<byte[], byte[]> store = new CacheStorageConfiguration().getStore("test");
        final AtomicInteger obsoletes = new AtomicInteger(0);

        PerformanceTest readWriteTest = new PerformanceTest() {

            public void doOperation(int index) throws Exception {
                try {
                    byte[] bytes = Integer.toString(index % valueRange).getBytes();
                    if(index % mod < readMax)
                        store.get(bytes);
                    else
                        store.put(bytes, new Versioned<byte[]>(bytes));
                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        readWriteTest.run(numRequests, numThreads);
        System.out.println("Cache storage engine performance test results:");
        readWriteTest.printStats();
        System.out.println("Number of obsolete puts: " + obsoletes.get());
    }

}

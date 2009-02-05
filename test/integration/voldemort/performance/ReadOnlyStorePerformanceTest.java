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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.readonly.RandomAccessFileStorageConfiguration;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;

public class ReadOnlyStorePerformanceTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length != 4)
            Utils.croak("USAGE: java " + ReadOnlyStorePerformanceTest.class.getName()
                        + " num-threads num-requests server-properties-file storeName");
        int numThreads = Integer.parseInt(args[0]);
        int numRequests = Integer.parseInt(args[1]);
        String serverPropsFile = args[2];
        String storeName = args[3];

        final Store<byte[], byte[]> store = new RandomAccessFileStorageConfiguration(new VoldemortConfig(new Props(new File(serverPropsFile)))).getStore(storeName);

        final AtomicInteger obsoletes = new AtomicInteger(0);
        final AtomicInteger nullResults = new AtomicInteger(0);
        final AtomicInteger totalResults = new AtomicInteger(0);

        PerformanceTest readWriteTest = new PerformanceTest() {

            private final int MaxMemberID = (int) (30 * 1000 * 1000);

            public void doOperation(int index) throws Exception {
                try {
                    byte[] bytes = (new Integer((int) (Math.random() * MaxMemberID))).toString()
                                                                                     .getBytes();
                    totalResults.incrementAndGet();
                    if(null == store.get(bytes)) {
                        nullResults.incrementAndGet();
                    }
                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        readWriteTest.run(numRequests, numThreads);
        System.out.println("Random Access Read Only store Results:");
        System.out.println("null Reads ratio:" + (nullResults.doubleValue())
                           / totalResults.doubleValue());
        readWriteTest.printStats();
    }
}

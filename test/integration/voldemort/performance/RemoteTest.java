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

import static voldemort.utils.Utils.croak;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.TestUtils;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.versioning.Versioned;

public class RemoteTest {

    public static void main(String[] args) throws Exception {
        if(args.length != 4)
            croak("USAGE: java " + RemoteTest.class.getName()
                  + " url num_requests value_size start_num");

        System.err.println("Bootstraping cluster data.");
        String url = args[0];
        int numRequests = Integer.parseInt(args[1]);
        int valueSize = Integer.parseInt(args[2]);
        int startNum = Integer.parseInt(args[3]);

        StoreClientFactory factory = new SocketStoreClientFactory(Executors.newFixedThreadPool(20),
                                                                  6,
                                                                  200,
                                                                  20000,
                                                                  20000,
                                                                  20000,
                                                                  64000,
                                                                  new DefaultSerializerFactory(),
                                                                  url);
        final StoreClient<String, String> store = factory.getStoreClient("test");

        final String value = new String(TestUtils.randomBytes(valueSize));

        System.err.println("Beginning delete test.");
        final AtomicInteger count0 = new AtomicInteger(startNum);
        final AtomicInteger successes = new AtomicInteger(0);
        ExecutorService service = Executors.newFixedThreadPool(8);
        long start = System.currentTimeMillis();
        final CountDownLatch latch0 = new CountDownLatch(numRequests);
        for(int i = 0; i < numRequests; i++) {
            service.execute(new Runnable() {

                public void run() {
                    try {
                        String str = Integer.toString(count0.getAndIncrement());
                        store.delete(str);
                        successes.getAndIncrement();
                    } catch(Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch0.countDown();
                    }
                }
            });
        }
        latch0.await();
        long deleteTime = System.currentTimeMillis() - start;
        System.out.println("Throughput: " + (numRequests / (float) deleteTime * 1000)
                           + " deletes/sec.");
        System.out.println(successes.get() + " things deleted.");

        System.err.println("Beginning write test.");
        final AtomicInteger count1 = new AtomicInteger(startNum);
        start = System.currentTimeMillis();
        final CountDownLatch latch1 = new CountDownLatch(numRequests);
        for(int i = 0; i < numRequests; i++) {
            service.execute(new Runnable() {

                public void run() {
                    try {
                        String str = Integer.toString(count1.getAndIncrement());
                        store.put(str, new Versioned<String>(value));
                    } catch(Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch1.countDown();
                    }
                }
            });
        }
        latch1.await();
        long writeTime = System.currentTimeMillis() - start;
        System.out.println("Throughput: " + (numRequests / (float) writeTime * 1000)
                           + " writes/sec.");

        System.err.println("Beginning read test.");
        final CountDownLatch latch2 = new CountDownLatch(numRequests);
        start = System.currentTimeMillis();
        final AtomicInteger count2 = new AtomicInteger(startNum);
        for(int i = 0; i < numRequests; i++) {
            service.execute(new Runnable() {

                public void run() {
                    try {
                        String str = Integer.toString(count2.getAndIncrement());
                        store.get(str);
                    } catch(Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch2.countDown();
                    }
                }
            });
        }
        latch2.await();
        long readTime = System.currentTimeMillis() - start;
        System.out.println("Throughput: " + (numRequests / (float) readTime * 1000.0)
                           + " reads/sec.");

        System.exit(0);
    }

}

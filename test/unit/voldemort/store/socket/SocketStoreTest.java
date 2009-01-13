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

package voldemort.store.socket;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.server.socket.SocketServer;
import voldemort.store.ByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.versioning.Versioned;

public class SocketStoreTest extends ByteArrayStoreTest {

    private SocketServer socketServer;
    private SocketStore socketStore;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        socketServer = ServerTestUtils.getSocketServer(VoldemortTestConstants.getOneNodeClusterXml(),
                                                       VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                       "test",
                                                       6667);
        socketStore = ServerTestUtils.getSocketStore("test", 6667);
    }

    @Override
    public Store<byte[], byte[]> getStore() {
        return socketStore;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        socketServer.shutdown();
        socketStore.close();
    }

    public void testThreadOverload() throws Exception {
        final Store<byte[], byte[]> store = getStore();
        final AtomicInteger val = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(100);
        Executor exec = Executors.newCachedThreadPool();
        for(int i = 0; i < 100; i++) {
            exec.execute(new Runnable() {

                public void run() {
                    store.put(TestUtils.randomString("abcdefghijklmnopqrs", 10).getBytes(),
                              new Versioned<byte[]>(TestUtils.randomBytes(8)));
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

}

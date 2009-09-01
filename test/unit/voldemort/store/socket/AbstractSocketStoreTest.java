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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * A base-socket store test that works with any store RequestFormat
 * 
 * @author jay
 * 
 */
public abstract class AbstractSocketStoreTest extends AbstractByteArrayStoreTest {

    private static final Logger logger = Logger.getLogger(AbstractSocketStoreTest.class);

    public AbstractSocketStoreTest(RequestFormatType type) {
        this.requestFormatType = type;
    }

    private int socketPort;
    private AbstractSocketService socketService;
    private SocketStore socketStore;
    private RequestFormatType requestFormatType;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.socketPort = ServerTestUtils.findFreePort();
        socketService = ServerTestUtils.getSocketService(VoldemortTestConstants.getOneNodeClusterXml(),
                                                         VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                         "test",
                                                         socketPort);
        socketService.start();
        socketStore = ServerTestUtils.getSocketStore("test", socketPort, requestFormatType);
    }

    @Override
    public Store<ByteArray, byte[]> getStore() {
        return socketStore;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        socketService.stop();
        socketStore.close();
    }

    public void testVeryLargeValues() {
        final Store<ByteArray, byte[]> store = getStore();
        byte[] biggie = new byte[1 * 1024 * 1024];
        ByteArray key = new ByteArray(biggie);
        Random rand = new Random();
        for(int i = 0; i < 10; i++) {
            rand.nextBytes(biggie);
            Versioned<byte[]> versioned = new Versioned<byte[]>(biggie);
            store.put(key, versioned);
            assertNotNull(store.get(key));
            assertTrue(store.delete(key, versioned.getVersion()));
        }
    }

    public void testThreadOverload() throws Exception {
        final Store<ByteArray, byte[]> store = getStore();
        int numOps = 100;
        final CountDownLatch latch = new CountDownLatch(numOps);
        Executor exec = Executors.newCachedThreadPool();
        for(int i = 0; i < numOps; i++) {
            exec.execute(new Runnable() {

                public void run() {
                    store.put(TestUtils.toByteArray(TestUtils.randomString("abcdefghijklmnopqrs",
                                                                           10)),
                              new Versioned<byte[]>(TestUtils.randomBytes(8)));
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    public void testRepeatedClosedConnections() throws Exception {
        for(int i = 0; i < 100; i++) {
            Socket s = new Socket();
            s.setTcpNoDelay(true);
            s.setSoTimeout(1000);
            s.connect(new InetSocketAddress("localhost", socketPort));
            logger.info("Client opened" + i);
            // Thread.sleep(1);
            assertTrue(s.isConnected());
            assertTrue(s.isBound());
            assertTrue(!s.isClosed());
            s.close();
            logger.info("Client closed" + i);
        }
    }

}

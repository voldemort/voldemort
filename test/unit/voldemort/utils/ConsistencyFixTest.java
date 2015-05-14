/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ConsistencyFix.BadKey;
import voldemort.utils.ConsistencyFix.BadKeyOrphanReader;
import voldemort.utils.ConsistencyFix.BadKeyReader;
import voldemort.utils.ConsistencyFix.BadKeyStatus;
import voldemort.utils.ConsistencyFix.BadKeyWriter;
import voldemort.utils.ConsistencyFix.Stats;
import voldemort.versioning.VectorClock;

public class ConsistencyFixTest {

    final static String STORE_NAME = "consistency-fix";
    final static String STORES_XML = "test/common/voldemort/config/consistency-stores.xml";

    /**
     * 
     * @return bootstrap url
     */
    public static String setUpCluster() {
        // setup four nodes with one store and one partition
        final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                    10000,
                                                                                    100000,
                                                                                    32 * 1024);
        VoldemortServer[] servers = new VoldemortServer[4];
        int partitionMap[][] = { { 0 }, { 1 }, { 2 }, { 3 } };
        try {
            Cluster cluster = ServerTestUtils.startVoldemortCluster(4,
                                                                    servers,
                                                                    partitionMap,
                                                                    socketStoreFactory,
                                                                    true,
                                                                    null,
                                                                    STORES_XML,
                                                                    new Properties());
            Node node = cluster.getNodeById(0);
            return "tcp://" + node.getHost() + ":" + node.getSocketPort();
        } catch(IOException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }

        return null;
    }

    public void badKeyReaderWriteBadKeys(String fileName, boolean addWhiteSpace) {
        // Write file of "bad keys" with messy white space
        try {
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName));
            if(addWhiteSpace)
                fileWriter.write("\n\n\t\t\n\n\t\n");
            for(int i = 0; i < 1000; ++i) {
                byte[] keyb = TestUtils.randomBytes(10);
                fileWriter.write(ByteUtils.toHexString(keyb) + "\n");
                if(addWhiteSpace) {

                    if(i % 5 == 0) {
                        fileWriter.write("\n\n\t\t\n\n\t\n");
                    }
                    if(i % 7 == 0) {
                        fileWriter.write("\t");
                    }
                }
            }
            if(addWhiteSpace)
                fileWriter.write("\n\n\t\t\n\n\t\n");
            fileWriter.close();
        } catch(IOException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
    }

    public void badKeyReaderWriteOrphanKeys(String fileName, boolean addWhiteSpace) {
        // Write file of "orphans" with some white space between entries.
        /*- 
         * Example entry:
        6473333333646464,2
        00,version(1:1, 2:1) ts:1357257858674
        00,version(1:1, 3:1) ts:1357257858684
         */
        try {
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName));
            if(addWhiteSpace)
                fileWriter.write("\n\n\t\t\n\n\t\n");
            for(int i = 0; i < 1000; ++i) {
                int numValues = (i % 3) + 1;
                byte[] keyb = TestUtils.randomBytes(10);
                String keyLine = ByteUtils.toHexString(keyb) + "," + numValues;
                System.out.println("keyLine: " + keyLine);
                fileWriter.write(keyLine + "\n");
                for(int j = 0; j < numValues; j++) {
                    int valLength = (j + 10) * (j + 1);
                    String value = ByteUtils.toHexString(TestUtils.randomBytes(valLength));
                    VectorClock vectorClock = TestUtils.getClock(j);
                    String valueLine = value + "," + vectorClock.toString();
                    System.out.println("valueLine: " + valueLine);
                    fileWriter.write(valueLine + "\n");
                }
                if(addWhiteSpace) {
                    if(i % 5 == 0) {
                        fileWriter.write("\n\n\t\t\n\n\t\n");
                    }
                }
            }
            if(addWhiteSpace)
                fileWriter.write("\n\n\t\t\n\n\t\n");
            fileWriter.close();
        } catch(IOException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
    }

    /**
     * 
     * @param orphan true for testing orphan, false for testing normal...
     */
    public void badKeyReaderHelper(boolean orphan) {
        String tmpDir = TestUtils.createTempDir().getAbsolutePath();
        String fileName = tmpDir + "BadKeyFile";
        if(orphan) {
            badKeyReaderWriteOrphanKeys(fileName, true);
        } else {
            badKeyReaderWriteBadKeys(fileName, true);
        }

        // Get cluster bootstrap url
        String url = setUpCluster();

        // Construct ConsistencyFix with parseOnly true
        ConsistencyFix consistencyFix = new ConsistencyFix(url, STORE_NAME, 100, 100, false, true);

        // Do set up for BadKeyReader akin to consistencyFix.execute...
        int parallelism = 1;
        BlockingQueue<Runnable> blockingQ = new ArrayBlockingQueue<Runnable>(parallelism);
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        ExecutorService consistencyFixWorkers = new ThreadPoolExecutor(parallelism,
                                                                       parallelism,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       blockingQ,
                                                                       rejectedExecutionHandler);
        BlockingQueue<BadKeyStatus> badKeyQOut = new ArrayBlockingQueue<BadKeyStatus>(10000);

        ExecutorService badKeyReaderService = Executors.newSingleThreadExecutor();
        CountDownLatch allBadKeysReadLatch = new CountDownLatch(1);

        // Submit file of bad keys to (appropriate) BadKeyReader
        BadKeyReader bkr = null;
        if(orphan) {
            bkr = new BadKeyOrphanReader(allBadKeysReadLatch,
                                         fileName,
                                         consistencyFix,
                                         consistencyFixWorkers,
                                         badKeyQOut);
        } else {
            bkr = new BadKeyReader(allBadKeysReadLatch,
                                   fileName,
                                   consistencyFix,
                                   consistencyFixWorkers,
                                   badKeyQOut);
        }
        badKeyReaderService.submit(bkr);

        // Wait for file to be processed.
        try {
            allBadKeysReadLatch.await();

            badKeyReaderService.shutdown();
            consistencyFixWorkers.shutdown();
        } catch(InterruptedException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }
        consistencyFix.close();

        // Make sure everything worked as expected.
        assertFalse(bkr.hasException());
        assertEquals(0, badKeyQOut.size());
    }

    @Test
    public void testBadKeyReader() {
        badKeyReaderHelper(false);
    }

    @Test
    public void testBadKeyOrphanReader() {
        badKeyReaderHelper(true);
    }

    @Test
    public void testBadKeyResult() {
        BadKey badKey = new BadKey("0101", "0101\n");
        ConsistencyFix.BadKeyStatus bkr1 = new BadKeyStatus(badKey, ConsistencyFix.Status.SUCCESS);
        assertFalse(bkr1.isPoison());
        assertEquals(bkr1.getBadKey().getKeyInHexFormat(), "0101");
        assertEquals(bkr1.getBadKey().getReaderInput(), "0101\n");
        assertEquals(bkr1.getStatus(), ConsistencyFix.Status.SUCCESS);

        ConsistencyFix.BadKeyStatus bkr2 = new BadKeyStatus();
        assertTrue(bkr2.isPoison());
        assertEquals(bkr2.getBadKey(), null);
        assertEquals(bkr2.getStatus(), null);
    }

    @Test
    public void testBadKeyWriter() {
        String tmpDir = TestUtils.createTempDir().getAbsolutePath();
        String fileName = tmpDir + "BadKeyFile";

        // Set up bad key writer
        BlockingQueue<BadKeyStatus> bq = new ArrayBlockingQueue<BadKeyStatus>(5);
        ExecutorService badKeyWriterService = Executors.newSingleThreadExecutor();

        BadKeyWriter badKeyWriter = new BadKeyWriter(fileName, bq);
        badKeyWriterService.submit(badKeyWriter);

        // Enqueue stuff for bad key writer to write
        try {
            for(int i = 0; i < 100; ++i) {
                BadKey badKey = new BadKey(Integer.toHexString(i), Integer.toHexString(i) + "\n");

                bq.put(new BadKeyStatus(badKey, ConsistencyFix.Status.REPAIR_EXCEPTION));
            }
            // Poison bad key writer
            bq.put(new BadKeyStatus());
        } catch(InterruptedException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }

        // wait for bad key writer to shutdown
        badKeyWriterService.shutdown();
        try {
            badKeyWriterService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch(InterruptedException e) {
            e.printStackTrace();
            fail("Unexpected exception");
        }

        assertFalse(badKeyWriter.hasException());

        // Read output file & verify.
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(fileName));

            int i = 0;
            for(String keyLine = fileReader.readLine(); keyLine != null; keyLine = fileReader.readLine()) {
                assertEquals(keyLine, Integer.toHexString(i));
                i++;
            }
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            fail("Unexpected exception");
        } catch(IOException ioe) {
            ioe.printStackTrace();
            fail("Unexpected exception");
        }
    }

    @Test
    public void testConsistencyFixSetupTeardown() {
        String url = setUpCluster();

        ConsistencyFix consistencyFix = new ConsistencyFix(url, STORE_NAME, 100, 100, false, false);

        consistencyFix.close();
    }

    @Test
    public void testStats() throws InterruptedException {
        ConsistencyFix.Stats stats = new Stats(1000);

        long lastTimeMs = stats.lastTimeMs;
        TimeUnit.MILLISECONDS.sleep(2);
        for(int i = 0; i < 1001; ++i) {
            stats.incrementFixCount();
        }
        assertTrue(stats.fixCount == 1001);
        assertTrue(stats.startTimeMs < stats.lastTimeMs);
        assertTrue(lastTimeMs < System.currentTimeMillis());

    }

    @Test
    public void testStatus() {
        ConsistencyFix.Status status = ConsistencyFix.Status.SUCCESS;
        assertEquals(status.toString(), "success");
    }
}

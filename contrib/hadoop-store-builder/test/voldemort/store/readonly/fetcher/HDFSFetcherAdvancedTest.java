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

package voldemort.store.readonly.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mockito;
import org.mortbay.jetty.EofException;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.fetcher.HdfsFetcher.CopyStats;
import voldemort.utils.Utils;

/*
 * This test suite tests the HDFSFetcher We test the fetch from hadoop by
 * simulating exceptions during fetches
 */
public class HDFSFetcherAdvancedTest {

    public static final Random UNSEEDED_RANDOM = new Random();

    /*
     * Tests that HdfsFetcher can correctly fetch a file in happy path
     */
    @Test
    public void testCheckSumMetadata() throws Exception {

        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = createTempDir();
        File testDestinationDirectory = testSourceDirectory;

        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        HdfsFetcher fetcher = new HdfsFetcher();

        File metadataFile = new File(testSourceDirectory, ".metadata");

        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());

        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        // Correct metadata checksum - MD5
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                              CheckSumType.MD5))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        File tempDest = new File(testDestinationDirectory.getAbsolutePath() + "1");
        if(tempDest.exists()) {

            deleteDir(tempDest);
        }

        File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                         testDestinationDirectory.getAbsolutePath() + "1");

        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "1");

        tempDest = new File(testDestinationDirectory.getAbsolutePath() + "1");
        if(tempDest.exists()) {

            deleteDir(tempDest);
        }

    }

    public static File createTempDir() {
        return createTempDir(new File(System.getProperty("java.io.tmpdir")));
    }

    /**
     * Create a temporary directory that is a child of the given directory
     * 
     * @param parent The parent directory
     * @return The temporary directory
     */
    public static File createTempDir(File parent) {
        File temp = new File(parent, "hdfsfetchertestadvanced");
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        return temp;
    }

    /**
     * Convenient method to execute private methods from other classes.
     * 
     * @param test Instance of the class we want to test
     * @param methodName Name of the method we want to test
     * @param params Arguments we want to pass to the method
     * @return Object with the result of the executed method
     * @throws Exception
     */
    private Object invokePrivateMethod(Object test, String methodName, Object params[])
            throws Exception {
        Object ret = null;

        final Method[] methods = test.getClass().getDeclaredMethods();
        for(int i = 0; i < methods.length; ++i) {
            if(methods[i].getName().equals(methodName)) {
                methods[i].setAccessible(true);
                ret = methods[i].invoke(test, params);
                break;
            }
        }

        return ret;
    }

    /*
     * Tests that HdfsFetcher can correctly fetch a file when there is an
     * IOException, specifically an EofException during the fetch
     */
    @Test
    public void testEofExceptionIntermittent() throws Exception {

        File testSourceDirectory = createTempDir();
        File testDestinationDirectory = testSourceDirectory;

        File indexFile = new File(testSourceDirectory, "0_0.index");
        byte[] indexBytes = TestUtils.randomBytes(100);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);

        final Path source = new Path(indexFile.getAbsolutePath());
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(CheckSumType.MD5);

        fileCheckSumGenerator.update(indexBytes);
        byte[] checksumCalculated = calculateCheckSumForFile(source);

        HdfsFetcher fetcher = new HdfsFetcher();

        Configuration config = new Configuration();

        FileSystem fs = source.getFileSystem(config);

        FileSystem spyfs = Mockito.spy(fs);
        CopyStats stats = new CopyStats(testSourceDirectory.getAbsolutePath(), sizeOfPath(fs,
                                                                                          source));

        File destination = new File(testDestinationDirectory.getAbsolutePath() + "1");
        Utils.mkdirs(destination);
        File copyLocation = new File(destination, "0_0.index");

        Mockito.doThrow(new IOException())
               .doAnswer(Mockito.CALLS_REAL_METHODS)
               .when(spyfs)
               .open(source);

        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5 };

        CheckSum ckSum = (CheckSum) this.invokePrivateMethod(fetcher,
                                                             "copyFileWithCheckSum",
                                                             params);

        assertEquals(Arrays.equals(ckSum.getCheckSum(), checksumCalculated), true);

    }

    /*
     * Tests that HdfsFetcher can correctly fetch a file when there is an
     * IOException, specifically an EofException during the fetch this test case
     * is different from the earlier one since it simulates an excpetion midway
     * a fetch
     */

    @Test
    public void testEofExceptionIntermittentDuringFetch() throws Exception {

        File testSourceDirectory = createTempDir();
        File testDestinationDirectory = testSourceDirectory;

        File indexFile = new File(testSourceDirectory, "0_0.index");
        byte[] indexBytes = TestUtils.randomBytes(VoldemortConfig.DEFAULT_BUFFER_SIZE * 3);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);

        final Path source = new Path(indexFile.getAbsolutePath());
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(CheckSumType.MD5);

        fileCheckSumGenerator.update(indexBytes);
        byte[] checksumCalculated = calculateCheckSumForFile(source);

        HdfsFetcher fetcher = new HdfsFetcher();

        Configuration config = new Configuration();

        FileSystem fs = source.getFileSystem(config);

        FileSystem spyfs = Mockito.spy(fs);
        CopyStats stats = new CopyStats(testSourceDirectory.getAbsolutePath(), sizeOfPath(fs,
                                                                                          source));

        File destination = new File(testDestinationDirectory.getAbsolutePath() + "1");
        Utils.mkdirs(destination);
        File copyLocation = new File(destination, "0_0.index");

        FSDataInputStream input = null;

        input = fs.open(source);
        FSDataInputStream spyinput = Mockito.spy(input);

        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS)
               .doThrow(new EofException())
               .when(spyinput)
               .read();

        Mockito.doReturn(spyinput).doReturn(input).when(spyfs).open(source);

        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5 };

        CheckSum ckSum = (CheckSum) this.invokePrivateMethod(fetcher,
                                                             "copyFileWithCheckSum",
                                                             params);

        assertEquals(Arrays.equals(ckSum.getCheckSum(), checksumCalculated), true);

    }

    /*
     * Tests that HdfsFetcher can correctly handle when there is an
     * RuntimeException
     * 
     * Expected- the exception should be consumed without spilling it over
     */

    @Test
    public void testIntermittentRuntimeExceptions() throws Exception {

        File testSourceDirectory = createTempDir();
        File testDestinationDirectory = createTempDir();

        File indexFile = new File(testSourceDirectory, "0_0.index");
        byte[] indexBytes = TestUtils.randomBytes(100);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);

        final Path source = new Path(indexFile.getAbsolutePath());
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(CheckSumType.MD5);

        fileCheckSumGenerator.update(indexBytes);

        HdfsFetcher fetcher = new HdfsFetcher();

        Configuration config = new Configuration();

        FileSystem fs = source.getFileSystem(config);

        FileSystem spyfs = Mockito.spy(fs);
        CopyStats stats = new CopyStats(testSourceDirectory.getAbsolutePath(), sizeOfPath(fs,
                                                                                          source));

        File destination = new File(testDestinationDirectory.getAbsolutePath() + "1");
        Utils.mkdirs(destination);
        File copyLocation = new File(destination, "0_0.index");

        Mockito.doThrow(new RuntimeException())
               .doAnswer(Mockito.CALLS_REAL_METHODS)
               .when(spyfs)
               .open(source);

        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5 };

        CheckSum ckSum = (CheckSum) this.invokePrivateMethod(fetcher,
                                                             "copyFileWithCheckSum",
                                                             params);

    }

    private long sizeOfPath(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(fs, status.getPath());
                else
                    size += status.getLen();
            }
        }
        return size;
    }

    /*
     * Helper method to delete a non empty directory
     */
    public static boolean deleteDir(File dir) {
        if(dir.isDirectory()) {
            String[] children = dir.list();
            for(int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if(!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    /*
     * Helper method to calculate checksum for a single file
     */
    private byte[] calculateCheckSumForFile(Path source) throws Exception {
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(CheckSumType.MD5);
        byte[] buffer = new byte[VoldemortConfig.DEFAULT_BUFFER_SIZE];

        FSDataInputStream input = null;

        Configuration config = new Configuration();

        FileSystem fs = source.getFileSystem(config);
        input = fs.open(source);

        while(true) {
            int read = input.read(buffer);
            if(read < 0) {
                break;
            }
            // Update the per file checksum
            if(fileCheckSumGenerator != null) {
                fileCheckSumGenerator.update(buffer, 0, read);
            }

        }

        return fileCheckSumGenerator.getCheckSum();
    }
}

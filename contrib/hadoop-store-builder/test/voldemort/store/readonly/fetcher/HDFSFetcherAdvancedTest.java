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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mortbay.jetty.EofException;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.fetcher.HdfsFetcher.CopyStats;
import voldemort.store.readonly.utils.ReadOnlyTestUtils;
import voldemort.utils.Utils;

@RunWith(Parameterized.class)
/*
 * This test suite tests the HDFSFetcher We test the fetch from hadoop by
 * simulating exceptions during fetches
 */
public class HDFSFetcherAdvancedTest {

    public static final Random UNSEEDED_RANDOM = new Random();
    private boolean isCompressed = false;
    private File testSourceDir, testCompressedSourceDir, testDestDir, copyLocation;
    private Path source;
    byte[] checksumCalculated;
    private HdfsFetcher fetcher;
    private FileSystem fs;
    private CopyStats stats;
    String indexFileName;
    FSDataInputStream input;
    byte[] buffer;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    public HDFSFetcherAdvancedTest(boolean isCompressed) {
        this.isCompressed = isCompressed;
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



    private void setUp() throws Exception {
        // First clean up any dirty state.
        cleanUp();

        testSourceDir = createTempDir();
        testDestDir = testSourceDir;
        indexFileName = "0_0.index";
        File indexFile = new File(testSourceDir, indexFileName);
        byte[] indexBytes = TestUtils.randomBytes(VoldemortConfig.DEFAULT_BUFFER_SIZE * 3);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);
        source = new Path(indexFile.getAbsolutePath());
        checksumCalculated = calculateCheckSumForFile(source);

        if(isCompressed) {
            testCompressedSourceDir = new File(testSourceDir.getAbsolutePath() + "_compressed");
            Utils.mkdirs(testCompressedSourceDir);
            File compressedIndexFile = new File(testCompressedSourceDir, indexFileName + ".gz");
            gzipFile(indexFile.getAbsolutePath(), compressedIndexFile.getAbsolutePath());

            // reset source to new Compressed directory

            source = new Path(compressedIndexFile.getAbsolutePath());

            // Checksum still needs to be calculated only for the uncompressed
            // data since we calculate the checksum for uncompressed data in
            // server.
        }

        fetcher = new HdfsFetcher();
        fs = source.getFileSystem(new Configuration());
        File destination = new File(testDestDir.getAbsolutePath() + "1");
        if(isCompressed) {
            stats = new CopyStats(testCompressedSourceDir.getAbsolutePath(), sizeOfPath(fs, source));
            copyLocation = new File(destination, indexFileName + ".gz");
        } else {
            stats = new CopyStats(testSourceDir.getAbsolutePath(), sizeOfPath(fs, source));
            copyLocation = new File(destination, indexFileName);
        }

        Utils.mkdirs(destination);

        buffer = new byte[VoldemortConfig.DEFAULT_BUFFER_SIZE];
    }

    private void cleanUp() {
        if(testSourceDir != null)
            deleteDir(testSourceDir);
        if(testDestDir != null)
            deleteDir(testDestDir);
        if(testCompressedSourceDir != null)
            deleteDir(testCompressedSourceDir);
        source = null;
        checksumCalculated = null;
        fetcher = null;
        fs = null;
        stats = null;
        input = null;
        buffer = null;
    }

    private void gzipFile(String srcPath, String destPath) throws Exception {
        byte[] buffer = new byte[1024];
        FileOutputStream fileOutputStream = new FileOutputStream(destPath);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(fileOutputStream)));

        FileInputStream fileInput = new FileInputStream(srcPath);
        int bytes_read;
        while((bytes_read = fileInput.read(buffer)) > 0) {
            out.write(buffer, 0, bytes_read);
        }
        fileInput.close();
        out.close();
    }

    private void unGunzipFile(String compressedFile, String decompressedFile) {

        byte[] buffer = new byte[1024];
        try {
            FileSystem fs = FileSystem.getLocal(new Configuration());
            FSDataInputStream fileIn = fs.open(new Path(compressedFile));
            FilterInputStream gZIPInputStream = new GZIPInputStream(fileIn);
            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
            int bytes_read;
            while((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
            gZIPInputStream.close();
            fileOutputStream.close();

        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    /*
     * Tests that HdfsFetcher can correctly fetch a file in happy path
     * 
     * Checks for both checksum and correctness in case of decompression.
     */
    @Test
    public void testCheckSumMetadata() throws Exception {
        cleanUp();
        testSourceDir = createTempDir();
        File testUncompressedSourceDir = null;

        // generate index , data and , metadata files
        File indexFile = new File(testSourceDir, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));
        File dataFile = new File(testSourceDir, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));
        HdfsFetcher fetcher = new HdfsFetcher();
        File metadataFile = new File(testSourceDir, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        // Correct metadata checksum - MD5
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDir.listFiles(),
                                                                              CheckSumType.MD5))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        /*
         * if isCompressed == true replace .index and .data files with their
         * compressed files before invoking fetch. Move the original
         * uncompressed .index and .data files to a temporary location so they
         * can be used later to check for data equality.
         */
        if(isCompressed) {
            String destIndexPath = indexFile.getAbsolutePath() + ".gz";
            gzipFile(indexFile.getAbsolutePath(), destIndexPath);
            String destDataPath = dataFile.getAbsolutePath() + ".gz";
            gzipFile(dataFile.getAbsolutePath(), destDataPath);

            testUncompressedSourceDir = new File(testSourceDir.getAbsolutePath() + "-uncompressed");
            testUncompressedSourceDir.delete();
            testUncompressedSourceDir.mkdir();

            if(!indexFile.renameTo(new File(testUncompressedSourceDir, indexFile.getName()))
               || !dataFile.renameTo(new File(testUncompressedSourceDir, dataFile.getName()))) {
                throw new Exception("cannot move irrelevant files");
            }

        }
        testDestDir = new File(testSourceDir.getAbsolutePath() + "1");
        if(testDestDir.exists()) {

            deleteDir(testDestDir);
        }

        File fetchedFile = fetcher.fetch(testSourceDir.getAbsolutePath(),
                                         testDestDir.getAbsolutePath());

        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestDir.getAbsolutePath());
        if(isCompressed) {
            for(File file: testUncompressedSourceDir.listFiles()) {
                if(file.isFile()) {

                    Assert.assertTrue(ReadOnlyTestUtils.areTwoBinaryFilesEqual(file,
                                                                               new File(testDestDir,
                                                                                        file.getName())));
                }
            }

        }
        if(testDestDir.exists()) {

            deleteDir(testDestDir);
        }

    }

    @Test
    public void testCompressionAndDecompression() throws Exception {
        File tempDir = createTempDir(new File("/tmp/bla-1"));
        indexFileName = "0_0.index";
        File indexFile = new File(tempDir, indexFileName);
        byte[] indexBytes = TestUtils.randomBytes(100 * 3);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);
        String compressedFile = indexFile.getAbsolutePath() + "_compressed.gz";
        String decompressedFile = indexFile.getAbsolutePath() + "_decompressed";
        gzipFile(indexFile.getAbsolutePath(), compressedFile);
        unGunzipFile(compressedFile, decompressedFile);

    }


    /*
     * Tests that HdfsFetcher can correctly fetch a file when there is an
     * IOException, specifically an EofException during the fetch
     * 
     * For a compressed input, exception is acceptable.
     */
    @Test
    public void testIOExceptionIntermittent() throws Exception {
        setUp();
        FileSystem spyfs = Mockito.spy(fs);
        Mockito.doThrow(new IOException())
               .doAnswer(Mockito.CALLS_REAL_METHODS)
               .when(spyfs)
               .open(source);
        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5, buffer };
        CheckSum ckSum = (CheckSum) this.invokePrivateMethod(fetcher,
                                                             "copyFileWithCheckSumTest",
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
        setUp();
        input = fs.open(source);
        FileSystem spyfs = Mockito.spy(fs);
        FSDataInputStream spyinput = Mockito.spy(input);

        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS)
               .doThrow(new EofException())
               .when(spyinput)
               .read();
        Mockito.doReturn(spyinput).doReturn(input).when(spyfs).open(source);
        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5, buffer };

        CheckSum ckSum = null;
        try {
            ckSum = (CheckSum) this.invokePrivateMethod(fetcher, "copyFileWithCheckSumTest",
                                                             params);
        } catch(Exception ex) {
            if(isCompressed) {
                // This is expected
                return;
            } else {
                Assert.fail("Unexpected exption thrown : " + ex.getMessage());
            }
        }
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
        setUp();
        FileSystem spyfs = Mockito.spy(fs);
        Mockito.doThrow(new RuntimeException())
               .doAnswer(Mockito.CALLS_REAL_METHODS)
               .when(spyfs)
               .open(source);
        Object[] params = { spyfs, source, copyLocation, stats, CheckSumType.MD5, buffer };
        CheckSum ckSum = (CheckSum) this.invokePrivateMethod(fetcher,
                                                             "copyFileWithCheckSumTest",
                                                             params);
        assertEquals(Arrays.equals(ckSum.getCheckSum(), checksumCalculated), true);
    }

    /*
     * Tests that corrupted compressed stream triggers exception when servers
     * starts to decompress.
     * 
     * 1. We produce random bytes in index and data files
     * 
     * 2. We rename them to end with ".gz" to simulate corrupted compressed
     * streams
     * 
     * 3. We run the fetcher. Fetcher would see the ".gz" extension and starts
     * decompressing and does not find right GZIP headers . Thus produces
     * exception.
     */
    @Test
    public void testCorruptedCompressedFile() throws Exception {
        if(!isCompressed) {
            return;
        }

        testSourceDir = createTempDir();
        File testUncompressedSourceDir = null;

        // generate index , data and , metadata files
        File indexFile = new File(testSourceDir, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));
        File dataFile = new File(testSourceDir, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));
        HdfsFetcher fetcher = new HdfsFetcher();
        File metadataFile = new File(testSourceDir, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        // Correct metadata checksum - MD5
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDir.listFiles(),
                                                                              CheckSumType.MD5))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        // Rename index and data files to end with .gz. we are generating fake
        // compressed files here.

        if(!indexFile.renameTo(new File(testSourceDir, indexFile.getName() + ".gz"))
           || !dataFile.renameTo(new File(testSourceDir, dataFile.getName() + ".gz"))) {
            Assert.fail("cannot rename files as desired");
        }
        testDestDir = new File(testSourceDir.getAbsolutePath() + "1");
        if(testDestDir.exists()) {

            deleteDir(testDestDir);
        }

        File fetchedFile;
        try {
            fetchedFile = fetcher.fetch(testSourceDir.getAbsolutePath(),
                                         testDestDir.getAbsolutePath());
        } catch(VoldemortException ex) {
            // this is expected, since we did not send valid compressed file
            cleanUp();
            return;
        } catch(Exception ex) {
            // Any other exceptionis not acceptable. Fail the test case
            cleanUp();
            Assert.fail("Unexpected Exception thrown!");
        }
        cleanUp();
        // If the code reaches here, this means something is seriously bad
        Assert.fail("Unexpected behavior!");


    }

}

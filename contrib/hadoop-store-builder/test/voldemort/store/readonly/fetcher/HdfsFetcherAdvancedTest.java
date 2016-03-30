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
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.collect.Lists;
import junit.framework.Assert;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.UnauthorizedStoreException;
import voldemort.store.readonly.utils.ReadOnlyTestUtils;
import voldemort.utils.Utils;

@RunWith(Parameterized.class)
/*
 * This test suite tests the HDFSFetcher We test the fetch from hadoop by
 * simulating exceptions during fetches
 */
public class HdfsFetcherAdvancedTest {

    private final boolean isCompressed;
    private final boolean enableStatsFile;
    private File testSourceDir, testCompressedSourceDir, testDestDir, copyLocation;
    private Path source;
    byte[] checksumCalculated;
    private HdfsFetcher fetcher;
    private FileSystem fs;
    private HdfsCopyStats stats;
    String indexFileName;
    FSDataInputStream input;
    byte[] buffer;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false, true }, { true, false } });
    }

    public HdfsFetcherAdvancedTest(boolean isCompressed, boolean enableStatsFile) {
        this.isCompressed = isCompressed;
        this.enableStatsFile = enableStatsFile;
    }

    protected byte[] copyFileWithCheckSumTest(HdfsFetcher fetcher,
                                                FileSystem fs,
                                                Path source,
                                                File dest,
                                                HdfsCopyStats stats,
                                                CheckSumType checkSumType,
                                                byte[] buffer) throws IOException {

        FetchStrategy fetchStrategy =
                new BasicFetchStrategy(fetcher, fs, stats, new AsyncOperationStatus(-1, "Bogus AsyncOp just for tests."), VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE);
        return fetchStrategy.fetch(new HdfsFile(fs.getFileStatus(source)),
                dest,
                checkSumType);
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
        File temp = new File(parent, "hdfsfetchertestadvanced_" + System.currentTimeMillis());
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

    /*
     * Helper method to calculate checksum for a single file
     */
    private byte[] calculateCheckSumForFile(Path source) throws Exception {
        CheckSum fileCheckSumGenerator = CheckSum.getInstance(CheckSumType.MD5);
        byte[] buffer = new byte[VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE];

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

    private void setUp() throws Exception {
        // First clean up any dirty state.
        cleanUp();

        testSourceDir = createTempDir();
        testDestDir = testSourceDir;
        indexFileName = "0_0.index";
        File indexFile = new File(testSourceDir, indexFileName);
        byte[] indexBytes = TestUtils.randomBytes(VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE * 3);
        FileUtils.writeByteArrayToFile(indexFile, indexBytes);
        source = new Path(indexFile.getAbsolutePath());
        checksumCalculated = calculateCheckSumForFile(source);

        String sourceString = testSourceDir.getAbsolutePath();
        String finalIndexFileName = indexFileName;

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

            sourceString = testCompressedSourceDir.getAbsolutePath();
            finalIndexFileName += ".gz";
        }

        // Fake config with a bogus node ID and server config path
        VoldemortConfig config = new VoldemortConfig(-1, "");
        config.setReadOnlyFetchRetryCount(3);
        config.setReadOnlyFetchRetryDelayMs(1000);
        config.setReadOnlyStatsFileEnabled(true);
        config.setReadOnlyMaxVersionsStatsFile(50);

        fetcher = new HdfsFetcher(config);
        fs = source.getFileSystem(new Configuration());
        HdfsPathInfo hdfsPathInfo = new HdfsPathInfo(Lists.newArrayList(new HdfsDirectory(fs, source, config)));
        File destination = new File(testDestDir.getAbsolutePath() + "1");

        stats = new HdfsCopyStats(sourceString,
                destination,
                enableStatsFile,
                5,
                false,
                hdfsPathInfo);
        copyLocation = new File(destination, finalIndexFileName);

        Utils.mkdirs(destination);

        buffer = new byte[VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE];
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
    public void testForDiskQuota() {

        try {
            // When disk quota is set to -1, no quota check should be performed
            testForDiskQuota((int) VoldemortConfig.DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB, 2048);
        } catch(Exception e) {
            Assert.fail("testDiskForQuota(" + VoldemortConfig.DEFAULT_DEFAULT_STORAGE_SPACE_QUOTA_IN_KB
                        + ", 2048) failed with Exception: " + e);
        }

         boolean testhasErrors = false;
         try {
         // When disk quota is 0 expect InvalidBootsrapURLException. What this
         // mean is no new stores will be onboarded on the fly during BnP
         testForDiskQuota(0, 2048);
         } catch(UnauthorizedStoreException e) {
         // This is expected
         testhasErrors = true;
         } catch(Exception e) {
            Assert.fail("testForDiskQuota(0, 2048) failed with Exception: " + e);
         }
         if(!testhasErrors) {
            Assert.fail("testForDiskQuota(0, 2048) should have failed with UnauthorizedStoreException.");
         }


        testhasErrors = false;
        try {
        // Expect QuotaExceeded Exception when disk quota is less thatn data
        // size
        testForDiskQuota(2, 3000);
        } catch(QuotaExceededException e) {
        // This is expected
        testhasErrors = true;
        } catch(Exception e) {
            Assert.fail("testForDiskQuota(2, 3000) failed with Exception: " + e);
        }
        if(!testhasErrors) {
            Assert.fail("testForDiskQuota(2, 3000) should have failed with QuotaExceededException.");
        }


        try{
        // When there is enough quota fetch succeeds.
        testForDiskQuota(2, 1000);
        }catch(Exception e){
            Assert.fail("testForDiskQuota(2, 1000) failed with Exception: " + e);
        }

    }

    private void testForDiskQuota(int diskQuotaInKB, int actualDataSizeInBytes) throws Exception {
        cleanUp();
        if(testSourceDir != null && testSourceDir.exists()){
            deleteDir(testSourceDir);
        }
        testSourceDir = createTempDir();
        File testUncompressedSourceDir = null;

        int indexFileSize = actualDataSizeInBytes / 4;
        int dataFileSize = actualDataSizeInBytes - indexFileSize;

        // generate index , data and , metadata files
        File indexFile = new File(testSourceDir, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(indexFileSize));
        File dataFile = new File(testSourceDir, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(dataFileSize));
        HdfsFetcher fetcher = new HdfsFetcher();
        File metadataFile = new File(testSourceDir, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        // Correct metadata checksum - MD5
        byte[] computedCheksum = CheckSumTests.calculateCheckSum(testSourceDir.listFiles(),
                                                                 CheckSumType.MD5);
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM, new String(Hex.encodeHex(computedCheksum)));
        metadata.add(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES,
                     Integer.toString(actualDataSizeInBytes));
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
                                         testDestDir.getAbsolutePath(),
                                         diskQuotaInKB);

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
        byte[] actualCheckSum = copyFileWithCheckSumTest(fetcher,
                                                         spyfs,
                                                         source,
                                                         copyLocation,
                                                         stats,
                                                         CheckSumType.MD5,
                                                         buffer);
        assertEquals(Arrays.equals(actualCheckSum, checksumCalculated), true);
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

        byte[] actualCheckSum = null;
        try {
            actualCheckSum = copyFileWithCheckSumTest(fetcher,
                                                      spyfs,
                                                      source,
                                                      copyLocation,
                                                      stats,
                                                      CheckSumType.MD5,
                                                      buffer);
        } catch(Exception ex) {
            if(isCompressed) {
                // This is expected
                return;
            } else {
                Assert.fail("Unexpected exption thrown : " + ex.getMessage());
            }
        }
        assertEquals(Arrays.equals(actualCheckSum, checksumCalculated), true);
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
        byte[] actualCheckSum = copyFileWithCheckSumTest(fetcher,
                                                         spyfs,
                                                         source,
                                                         copyLocation,
                                                         stats,
                                                         CheckSumType.MD5,
                                                         buffer);
        assertEquals(Arrays.equals(actualCheckSum, checksumCalculated), true);
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
            Assert.fail("Unexpected! Fetch should have failed, but instead, successfully got: " + fetchedFile);
        } catch(VoldemortException ex) {
            // this is expected, since we did not send valid compressed file
            cleanUp();
            return;
        } catch(Exception ex) {
            // Any other exception is not acceptable. Fail the test case
            cleanUp();
            Assert.fail("Unexpected Exception thrown!");
        }
    }

}

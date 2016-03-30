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

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

/**
 * Tests for the HDFS-based fetcher
 *
 *
 */
public class HdfsFetcherTest extends TestCase {

    public void testCheckSumMetadata() throws Exception {
        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                         testDestinationDirectory.getAbsolutePath() + "1");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "1");

        // Write bad metadata file
        File metadataFile = new File(testSourceDirectory, ".metadata");
        FileUtils.writeByteArrayToFile(metadataFile, TestUtils.randomBytes(100));
        try {
            fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                        testDestinationDirectory.getAbsolutePath() + "2");
            fail("Should have thrown an exception since metadata file is corrupt");
        } catch(VoldemortException e) {}
        metadataFile.delete();

        // Missing metadata checksum type
        metadataFile = new File(testSourceDirectory, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "3");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "3");
        metadataFile.delete();

        // Incorrect checksum type + missing checksum
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, "blah");
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "4");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "4");
        metadataFile.delete();

        // Incorrect metadata checksum
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM, "1234");
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "5");
        assertNull(fetchedFile);
        metadataFile.delete();

        // Correct metadata checksum - MD5
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                              CheckSumType.MD5))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "6");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "6");

        // Correct metadata checksum - ADLER32
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.ADLER32));
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                              CheckSumType.ADLER32))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "7");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "7");

        // Correct metadata checksum - CRC32
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.CRC32));
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                     new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                              CheckSumType.CRC32))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "8");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                                                    + "8");
    }

    public void testFetch() throws Exception {
        // Tests kept for backwards compatibility

        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        File testFile = File.createTempFile("test", ".dat", testSourceDirectory);
        testFile.createNewFile();

        // Test 1: No checksum file - return correctly
        // Required for backward compatibility with existing hadoop stores
        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                         testDestinationDirectory.getAbsolutePath() + "1");
        assertNotNull(fetchedFile);

        // Test 2: Add checksum file with incorrect fileName, should not fail
        File checkSumFile = new File(testSourceDirectory, "blahcheckSum.txt");
        checkSumFile.createNewFile();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "2");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

        // Test 3: Add checksum file with correct fileName, should be ignored
        checkSumFile = new File(testSourceDirectory, "adler32checkSum.txt");
        checkSumFile.createNewFile();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "3");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

    }

    public void testAggStatsWithValidFile() throws Exception {
        HdfsFetcherAggStats stats = HdfsFetcherAggStats.getStats();
        long totalBytesFetchedBefore = stats.getTotalBytesFetched();
        long totalFetchesBefore = stats.getTotalFetches();
        double totalDataFetchRateBefore = stats.getTotalDataFetchRate();

        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        File metadataFile = new File(testSourceDirectory, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                        CheckSumType.MD5))));
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                testDestinationDirectory.getAbsolutePath() + "1");
        assertNotNull(fetchedFile);
        assertEquals(fetchedFile.getAbsolutePath(), testDestinationDirectory.getAbsolutePath()
                + "1");

        // The total bytes fetched includes meta data file as well.
        assertEquals(totalBytesFetchedBefore + 500 + metadata.toJsonString().length(), stats.getTotalBytesFetched());
        assertEquals(totalFetchesBefore + 1, stats.getTotalFetches());
        assertTrue(stats.getTotalDataFetchRate() > totalDataFetchRateBefore);
    }

    public void testAggStatsWithInvalidMetaFile() throws Exception {
        HdfsFetcherAggStats stats = HdfsFetcherAggStats.getStats();
        long totalBytesFetchedBefore = stats.getTotalBytesFetched();
        long totalFileReadFailuresBefore = stats.getTotalFileReadFailures();
        long totalFetchesBefore = stats.getTotalFetches();
        long totalIncompleteFetchesBefore = stats.getTotalIncompleteFetches();

        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        HdfsFetcher fetcher = new HdfsFetcher();

        // Write bad metadata file
        File metadataFile = new File(testSourceDirectory, ".metadata");
        FileUtils.writeByteArrayToFile(metadataFile, TestUtils.randomBytes(100));
        try {
            File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                    testDestinationDirectory.getAbsolutePath() + "1");
            fail("Should have thrown an exception since metadata file is corrupt");
        } catch(VoldemortException e) {}
        metadataFile.delete();

        // The total bytes fetched includes meta data file as well.
        assertEquals(totalBytesFetchedBefore + 100, stats.getTotalBytesFetched());
        assertEquals(totalFileReadFailuresBefore + 1, stats.getTotalFileReadFailures());
        assertEquals(totalFetchesBefore + 1, stats.getTotalFetches());
        assertEquals(totalIncompleteFetchesBefore + 1, stats.getTotalIncompleteFetches());
    }

    public void testAggStatsWithQuotaExceedException() throws Exception {
        HdfsFetcherAggStats stats = HdfsFetcherAggStats.getStats();
        long totalBytesFetchedBefore = stats.getTotalBytesFetched();
        long totalQuotaExceedFailuresBefore = stats.getTotalQuotaExceedFailures();
        long totalFetchesBefore = stats.getTotalFetches();
        long totalIncompleteFetchesBefore = stats.getTotalIncompleteFetches();

        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(1000));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(4000));

        File metadataFile = new File(testSourceDirectory, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                        CheckSumType.MD5))));
        metadata.add(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES, "5000");
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = null;
        try {
            fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                    testDestinationDirectory.getAbsolutePath() + "1", 1);
        } catch (Exception e)
        {}
        assertNull(fetchedFile);

        // The total bytes fetched includes meta data file as well.
        assertEquals(totalBytesFetchedBefore + metadata.toJsonString().length(), stats.getTotalBytesFetched());
        assertEquals(totalQuotaExceedFailuresBefore + 1, stats.getTotalQuotaExceedFailures());
        assertEquals(totalFetchesBefore + 1, stats.getTotalFetches());
        assertEquals(totalIncompleteFetchesBefore + 1, stats.getTotalIncompleteFetches());
    }

    public void testAggStatsWithUnauthorizedStoreException() throws Exception {
        HdfsFetcherAggStats stats = HdfsFetcherAggStats.getStats();
        long totalBytesFetchedBefore = stats.getTotalBytesFetched();
        long totalUnauthorizedStoreFailuresBefore = stats.getTotalUnauthorizedStoreFailures();
        long totalFetchesBefore = stats.getTotalFetches();
        long totalIncompleteFetchesBefore = stats.getTotalIncompleteFetches();

        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        File metadataFile = new File(testSourceDirectory, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM,
                new String(Hex.encodeHex(CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                        CheckSumType.MD5))));
        metadata.add(ReadOnlyStorageMetadata.DISK_SIZE_IN_BYTES, "5000");
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = null;
        try {
            fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                    testDestinationDirectory.getAbsolutePath() + "1", 0);
        } catch (Exception e)
        {}
        assertNull(fetchedFile);

        // The total bytes fetched includes meta data file as well.
        assertEquals(totalBytesFetchedBefore + metadata.toJsonString().length(), stats.getTotalBytesFetched());
        assertEquals(totalUnauthorizedStoreFailuresBefore + 1, stats.getTotalUnauthorizedStoreFailures());
        assertEquals(totalFetchesBefore + 1, stats.getTotalFetches());
        assertEquals(totalIncompleteFetchesBefore + 1, stats.getTotalIncompleteFetches());
    }

    public void testAggStatsWithCheckSumFailure() throws Exception {
        HdfsFetcherAggStats stats = HdfsFetcherAggStats.getStats();
        long totalBytesFetchedBefore = stats.getTotalBytesFetched();
        long totalCheckSumFailuresBefore = stats.getTotalCheckSumFailures();
        long totalFetchesBefore = stats.getTotalFetches();
        long totalIncompleteFetchesBefore = stats.getTotalIncompleteFetches();

        // Generate 0_0.[index | data] and their corresponding metadata
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        // Missing metadata file
        File indexFile = new File(testSourceDirectory, "0_0.index");
        FileUtils.writeByteArrayToFile(indexFile, TestUtils.randomBytes(100));

        File dataFile = new File(testSourceDirectory, "0_0.data");
        FileUtils.writeByteArrayToFile(dataFile, TestUtils.randomBytes(400));

        File metadataFile = new File(testSourceDirectory, ".metadata");
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
        metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.READONLY_V2.getCode());
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE, CheckSum.toString(CheckSumType.MD5));
        metadata.add(ReadOnlyStorageMetadata.CHECKSUM, "1234");
        FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());

        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = null;
        try {
            fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                    testDestinationDirectory.getAbsolutePath() + "1");
        } catch (Exception e)
        {}
        assertNull(fetchedFile);

        // The total bytes fetched includes meta data file as well.
        assertEquals(totalBytesFetchedBefore + 500 + metadata.toJsonString().length(), stats.getTotalBytesFetched());
        assertEquals(totalCheckSumFailuresBefore + 1, stats.getTotalCheckSumFailures());
        assertEquals(totalFetchesBefore + 1, stats.getTotalFetches());
        assertEquals(totalIncompleteFetchesBefore + 1, stats.getTotalIncompleteFetches());
    }
}

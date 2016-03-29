package voldemort.store.readonly.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.UnauthorizedStoreException;

import java.lang.reflect.Type;
import java.lang.Exception;

@RunWith(Parameterized.class)
public class HdfsCopyStatsTest {

    private final File testSourceDir;
    private File destination;
    private final boolean enableStatsFile;
    private final int maxStatsFile;
    private final boolean isFileCopy;
    private File statsDir;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    public HdfsCopyStatsTest(boolean enableStatsFile) {
        testSourceDir = HdfsFetcherAdvancedTest.createTempDir();
        destination = new File(testSourceDir.getAbsolutePath() + "_dest");
        statsDir = HdfsCopyStats.getStatDir(destination);
        this.enableStatsFile = enableStatsFile;
        this.maxStatsFile = 4;
        isFileCopy = false;
    }

    @Test
    public void testCopyStats() throws Exception {
        destination = new File(testSourceDir.getAbsolutePath() + "_dest");
        statsDir = HdfsCopyStats.getStatDir(destination);
        if(statsDir != null)
            HdfsFetcherAdvancedTest.deleteDir(statsDir);
        List<String> expectedStatsFile = new ArrayList<String>();
        for(int i = 0; i < maxStatsFile + 2; i++) {
            destination = new File(testSourceDir.getAbsolutePath() + "_dest" + i);
            String destName = destination.getName();
            // Sleep to get last modified time stamp different for all files
            // linux timestamp has second granularity, so sleep for a second
            Thread.sleep(1000);
            HdfsCopyStats stats = new HdfsCopyStats(testSourceDir.getAbsolutePath(),
                    destination,
                    enableStatsFile,
                    maxStatsFile,
                    isFileCopy,
                    HdfsPathInfo.getTestObject(1000));

            if(stats.getStatsFile() != null) {
                expectedStatsFile.add(stats.getStatsFile().getName());
            }

            Random r = new Random();
            for(int j = 0; j < 10; j++) {
                File file = new File(destination, "file" + i);
                stats.reportFileDownloaded(file,
                                           System.currentTimeMillis(),
                                           r.nextInt(),
                                           r.nextLong(),
                                           r.nextInt(),
                                           r.nextLong(),
                                           null);

            }
            Exception e = r.nextBoolean() ? null : new Exception();
            stats.reportFileError(new File(destination, "error"), r.nextInt(), r.nextLong(), e);

            stats.reportError("MyMessage", e);
            stats.complete();

            if(destination != null)
                HdfsFetcherAdvancedTest.deleteDir(destination);
        }

        statsDir = HdfsCopyStats.getStatDir(destination);
        if(enableStatsFile && isFileCopy == false) {
            assertTrue("stats dir exists", statsDir.exists());

            File[] statsFiles = statsDir.listFiles();
            assertEquals("Number of files should be equal to the maxStatsFiles",
                         maxStatsFile,
                         statsFiles.length);

            Set<String> actualStatsFile = new HashSet<String>();
            for(File statFile : statsFiles) {
                assertTrue("Size of the stat file should be greater than zero",
                           statFile.length() > 0);
                actualStatsFile.add(statFile.getName());
            }

            while(expectedStatsFile.size() > maxStatsFile) {
                expectedStatsFile.remove(0);
            }

            assertEquals("Expected and actual files are different",
                         new HashSet<String>(expectedStatsFile),
                         actualStatsFile);
        } else {
            assertFalse("statsDir " + statsDir + " should not exist", statsDir.exists());
        }
        cleanUp();

    }

    public void cleanUp() {
        if(testSourceDir != null)
            HdfsFetcherAdvancedTest.deleteDir(testSourceDir);
        if(statsDir != null)
            HdfsFetcherAdvancedTest.deleteDir(statsDir);
    }

    @Test
    public void testRecordBytesTransferred() {
        HdfsCopyStats stats = new HdfsCopyStats(null,
                null,
                false,
                3,
                false,
                null);
        long totalAggBytesFetchedBefore = HdfsFetcherAggStats.getStats().getTotalBytesFetched();
        stats.recordBytesTransferred(100);
        stats.complete();

        assertEquals(100, stats.getBytesTransferredSinceLastReport(), 0);
        assertEquals(100, stats.getBytesTransferredSinceLastReport(), 0);
        assertEquals(100, stats.getTotalBytesTransferred(), 0);
        assertEquals(totalAggBytesFetchedBefore + 100, HdfsFetcherAggStats.getStats().getTotalBytesFetched(), 0);
    }

    private Map<String, Long> buildMethodResultMap(long authenticateFailedRes,
                                      long fileNotFoundRes,
                                      long fileReadFailedRes,
                                      long quotaCheckFailedRes,
                                      long unauthorizedStorePushRes) {
        Map<String, Long> resMap = new HashMap<String, Long>();
        resMap.put("getTotalAuthenticationFailures", new Long(authenticateFailedRes));
        resMap.put("getTotalFileNotFoundFailures", new Long(fileNotFoundRes));
        resMap.put("getTotalFileReadFailures", new Long(fileReadFailedRes));
        resMap.put("getTotalQuotaExceedFailures", new Long(quotaCheckFailedRes));
        resMap.put("getTotalUnauthorizedStoreFailures", new Long(unauthorizedStorePushRes));

        return resMap;
    }

    private Map<String, Long> invokeInternalMethod(HdfsFetcherAggStats stats, Set<String> methodNames)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Map<String, Long> res = new HashMap<String, Long>();
        for (String methodName : methodNames) {
            Long methodRes = (Long)stats.getClass().getMethod(methodName).invoke(stats);
            res.put(methodName, methodRes);
        }

        return res;
    }

    @Test
    public void testReportExceptionForStats()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Map<Exception, Map> config = new HashMap<Exception, Map>();
        config.put(new Exception(new AuthenticationException("test")), buildMethodResultMap(1, 0, 0, 0, 0));
        config.put(new FileNotFoundException(), buildMethodResultMap(0, 1, 0, 0, 0));
        config.put(new IOException(), buildMethodResultMap(0, 0, 1, 0, 0));
        config.put(new QuotaExceededException("test"), buildMethodResultMap(0, 0, 0, 1, 0));
        config.put(new UnauthorizedStoreException("test"), buildMethodResultMap(0, 0, 0, 0, 1));

        HdfsFetcherAggStats aggStats = HdfsFetcherAggStats.getStats();

        for (Map.Entry<Exception, Map> entry : config.entrySet()) {
            Exception e = entry.getKey();
            Map<String, Long> methodResMap = entry.getValue();
            Set<String> methodSet = methodResMap.keySet();
            // Get result before invocation
            Map<String, Long> beforeRes = invokeInternalMethod(aggStats, methodSet);
            HdfsCopyStats.reportExceptionForStats(e);
            // Get result after invocation
            Map<String, Long> afterRes = invokeInternalMethod(aggStats, methodSet);
            // Compare the difference
            for (String methodName : methodSet) {
                String msg = "Method expects " + methodResMap.get(methodName) + " with exception: " + e.getClass().getName();
                assertEquals(msg, methodResMap.get(methodName).longValue(),
                        afterRes.get(methodName).longValue() - beforeRes.get(methodName).longValue());
            }
        }
    }
}

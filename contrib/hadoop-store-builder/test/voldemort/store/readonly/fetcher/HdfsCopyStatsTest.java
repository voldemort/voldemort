package voldemort.store.readonly.fetcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
        testSourceDir = HDFSFetcherAdvancedTest.createTempDir();
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
            HDFSFetcherAdvancedTest.deleteDir(statsDir);
        List<String> expectedStatsFile = new ArrayList<String>();
        for(int i = 0; i < maxStatsFile + 2; i++) {
            destination = new File(testSourceDir.getAbsolutePath() + "_dest" + i);
            String destName = destination.getName();
            expectedStatsFile.add(destName);
            // Sleep to get last modified time stamp different for all files
            // linux timestamp has second granularity, so sleep for a second
            Thread.sleep(1000);
            HdfsCopyStats stats = new HdfsCopyStats(testSourceDir.getAbsolutePath(),
                                                    destination,
                                                    enableStatsFile,
                                                    maxStatsFile,
                                                    isFileCopy,
                                                    1000);

            Random r = new Random();
            for(int j = 0; j < 10; j++) {
                File file = new File(destination, "file" + i);
                stats.reportFileDownloaded(file,
                                           System.currentTimeMillis(),
                                           r.nextInt(),
                                           r.nextLong(),
                                           r.nextInt(),
                                           r.nextLong());
                
               
            }
            Exception e = r.nextBoolean() ? null : new Exception();
            stats.reportFileError(new File(destination, "error"), r.nextInt(), r.nextLong(), e);

            stats.reportError("MyMessage", e);
            stats.complete();

            if(destination != null)
                HDFSFetcherAdvancedTest.deleteDir(destination);
        }

        statsDir = HdfsCopyStats.getStatDir(destination);
        if(enableStatsFile && isFileCopy == false) {
            assertTrue("stats dir exists", statsDir.exists());

            File[] statsFiles = statsDir.listFiles();
            assertEquals("Number of files should be equal to the maxStatsFiles",
                         maxStatsFile,
                         statsFiles.length);
            
            List<String> actualStatsFile = new ArrayList<String>();
            for(File statFile : statsFiles) {
                assertTrue("Size of the stat file should be greater than zero",
                           statFile.length() > 0);
                actualStatsFile.add(statFile.getName());
            }

            while(expectedStatsFile.size() > maxStatsFile) {
                expectedStatsFile.remove(0);
            }

            assertEquals("Expected and actual files are different",
                         expectedStatsFile,
                         actualStatsFile);
        } else {
            assertFalse("statsDir " + statsDir + " should not exist", statsDir.exists());
        }
        cleanUp();

    }

    public void cleanUp() {
        if(testSourceDir != null)
            HDFSFetcherAdvancedTest.deleteDir(testSourceDir);
        if(statsDir != null)
            HDFSFetcherAdvancedTest.deleteDir(statsDir);
    }
}

package voldemort.store.readonly.fetcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import org.apache.log4j.spi.ThrowableInformation;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.quota.QuotaExceededException;
import voldemort.store.readonly.UnauthorizedStoreException;
import voldemort.utils.ByteUtils;
import voldemort.utils.ExceptionUtils;
import voldemort.utils.Time;

/**
 * N.B.: bytes written and transferred should be equal for fetches with compression disabled.
 */
public class HdfsCopyStats {

    private final String sourceFile;
    private final long startTimeMS;
    private volatile long bytesTransferredSinceLastReport;
    private volatile long totalBytesTransferred;
    private volatile long bytesWrittenSinceLastReport;
    private volatile long totalBytesWritten;
    private volatile long lastReportNs;
    private final HdfsPathInfo pathInfo;

    private File statsFile;
    private BufferedWriter statsFileWriter = null;
    public static final String STATS_DIRECTORY = ".stats";
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Logger logger = Logger.getLogger(HdfsCopyStats.class);

    private static final HdfsFetcherAggStats aggStats = HdfsFetcherAggStats.getStats();

    private static void deleteExtraStatsFiles(File statsDirectory, int maxStatsFile) {
        if(maxStatsFile <= 0) {
            return;
        }

        try {
            File[] files = statsDirectory.listFiles();
            // One more stats file will be created for the current run.
            int allowedStatsFile = maxStatsFile - 1;

            if(files.length <= allowedStatsFile) {
                return;
            }

            Arrays.sort(files, new Comparator<File>() {

                @Override
                public int compare(File f1, File f2) {
                    return Long.valueOf(f2.lastModified()).compareTo(f1.lastModified());
                }
            });

            for(int i = allowedStatsFile; i < files.length; i++) {
                files[i].delete();
            }
        } catch(Exception e) {
            logger.error("Error during cleanup of stats file directory", e);
        }

    }

    public static File getStatDir(File destination) {
        // Downloading is happening on the top level directory, no place to
        // create summary
        File destParent = destination.getParentFile();
        if(destParent == null) {
            return null;
        }

        return new File(destParent, STATS_DIRECTORY);
    }

    public File getStatsFile() {
        return this.statsFile;
    }

    private static final int STATS_VERSION = 4;
    private void initializeStatsFile(File destination,
                                     boolean enableStatsFile,
                                     int maxVersionsStatsFile,
                                     boolean isFileCopy) {
        // Stats file is disabled in the config
        if(enableStatsFile == false) {
            return;
        }

        // Downloading just a file(test), stats are not required.
        if(isFileCopy) {
            return;
        }

        File statsDirectory = getStatDir(destination);
        if(statsDirectory == null) {
            return;
        }
        try {
            if(statsDirectory.exists() == false) {
                statsDirectory.mkdirs();
            }

            if(statsDirectory.exists() == false) {
                logger.info("Could not create stats directory for destination " + destination);
                return;
            }

            deleteExtraStatsFiles(statsDirectory, maxVersionsStatsFile);

            String destName = destination.getName() + ".stats-v" + STATS_VERSION;
            this.statsFile = new File(statsDirectory, destName);
            statsFile.createNewFile();

            statsFileWriter = new BufferedWriter(new FileWriter(statsFile, true));
            statsFileWriter.write("Starting fetch at " + startTimeMS + "MS from " + sourceFile
                                  + " . Info: " + pathInfo);
            statsFileWriter.newLine();
            /*
             * When you add or remove columns, increase the version number
             * STATS_VERSION so that script might be able to make automatic
             * parsing of the columns
             */
            statsFileWriter.write("Time, FileName, StartTime(MS), Size, TimeTaken(MS), Attempts #, TotalBytesTransferred, TotalBytesWritten, CheckSum");
            statsFileWriter.newLine();
            statsFileWriter.flush();

        } catch(Exception e) {
            statsFileWriter = null;
            logger.error("Error during stats directory for destination " + destination, e);
            return;
        }
    }

    public HdfsCopyStats(String source,
                         File destination,
                         boolean enableStatsFile,
                         int maxVersionsStatsFile,
                         boolean isFileCopy,
                         HdfsPathInfo pathInfo) {
        this.sourceFile = source;
        this.totalBytesTransferred = 0L;
        this.bytesTransferredSinceLastReport = 0L;
        this.pathInfo = pathInfo;
        this.lastReportNs = System.nanoTime();
        this.startTimeMS = System.currentTimeMillis();
        initializeStatsFile(destination, enableStatsFile, maxVersionsStatsFile, isFileCopy);
    }

    public void recordBytesWritten(long bytesWritten) {
        this.totalBytesWritten += bytesWritten;
        this.bytesWrittenSinceLastReport += bytesWritten;
    }

    public void recordBytesTransferred(long bytesTransferred) {
        this.totalBytesTransferred += bytesTransferred;
        this.bytesTransferredSinceLastReport += bytesTransferred;
        // Update the aggregated stats for total bytes transferred
        aggStats.recordBytesTransferred(bytesTransferred);
    }

    public static void storeFetch() {
        aggStats.storeFetch();
    }
    public void singleFileFetchStart(boolean isRetry) {
        aggStats.singleFileFetchStart(isRetry);
    }

    public void singleFileFetchEnd() {
        aggStats.singleFileFetchEnd();
    }

    public void checkSumFailed() {
        aggStats.checkSumFailed();
    }

    public static void reportExceptionForStats(Exception e) {
        if (ExceptionUtils.recursiveClassEquals(e, AuthenticationException.class)) {
            aggStats.authenticateFailed();
        } else if (e instanceof QuotaExceededException) {
            aggStats.quotaCheckFailed();
        } else if (e instanceof UnauthorizedStoreException) {
            aggStats.unauthorizedStorePush();
        } else if (e instanceof FileNotFoundException) {
            aggStats.fileNotFound();
        } else {
            aggStats.fileReadFailed();
        }
    }

    public static void incompleteFetch() {
        aggStats.incompleteFetch();
    }

    public void reset() {
        this.bytesTransferredSinceLastReport = 0;
        this.bytesWrittenSinceLastReport = 0;
        this.lastReportNs = System.nanoTime();
    }

    public long getBytesTransferredSinceLastReport() {
        return bytesTransferredSinceLastReport;
    }

    public long getBytesWrittenSinceLastReport() {
        return bytesWrittenSinceLastReport;
    }

    private void reportStats(String message) {
        try {
            if(statsFileWriter != null) {
                statsFileWriter.write(dateFormat.format(new Date()));
                statsFileWriter.write(",");
                statsFileWriter.write(message);
                statsFileWriter.newLine();
                statsFileWriter.flush();
            }
        } catch(IOException e) {
            // ignore errors
        }

    }

    public void reportError(String message, Throwable t) {
        if(statsFileWriter != null && t != null) {
            reportStats(message + " Error Message : " + t.getMessage());
            PrintWriter pw = new PrintWriter(statsFileWriter);
            t.printStackTrace(pw);
        } else {
            reportStats(message);
        }
    }

    public void reportFileError(File file,int attempts, long startTimeMS, Throwable t) {
        long nowMS = System.currentTimeMillis();
        String message = " Error occurred during file download " + file.getName()
                         + " after attempts " + attempts + " time elapsed " + (nowMS - startTimeMS)
                         + " MS.";
        reportError(message, t);
    }

    public void reportFileDownloaded(File file,
                                     long startTimeMS,
                                     long fileSize,
                                     long timeTakenMS,
                                     int attempts,
                                     long totalBytesWritten,
                                     byte[] checkSum) {
        String fileCheckSum;
        if(checkSum != null) {
            fileCheckSum = ByteUtils.toHexString(checkSum);
        } else {
            fileCheckSum = "NO_CHECKSUM";
        }
        reportStats(file.getName() + "," + startTimeMS + "," + fileSize + "," + timeTakenMS + ","
                    + attempts + "," + totalBytesTransferred + "," + totalBytesWritten + ","
                    + fileCheckSum);
    }

    public void complete() {
        long nowMS = System.currentTimeMillis() ;
        String expectedTotalBytesTransferred = "unavailable";
        if (pathInfo != null) {
            expectedTotalBytesTransferred = String.valueOf(pathInfo.getTotalSize());
        }
        reportStats(" Completed at " + nowMS + " MS. Total bytes transferred: " + totalBytesTransferred
                    + " . Expected total bytes transferred: " + expectedTotalBytesTransferred
                    + " . Total bytes written: " + totalBytesWritten
                    + " . Time taken(MS): " + (nowMS - startTimeMS));
        if(statsFileWriter != null) {
            IOUtils.closeQuietly(statsFileWriter);
        }
    }

    public double getPercentCopied() {
        if (pathInfo == null) {
            return -1.0;
        }
        if(pathInfo.getTotalSize() == 0) {
            return 0.0;
        } else {
            return (double) (totalBytesTransferred * 100) / (double) pathInfo.getTotalSize();
        }
    }

    @JmxGetter(name = "totalBytesTransferred", description = "The total number of bytes transferred over the network so far in this transfer.")
    public long getTotalBytesTransferred() {
        return totalBytesTransferred;
    }

    @JmxGetter(name = "totalBytesWritten", description = "The total number of bytes written to disk so far in this transfer.")
    public long getTotalBytesWritten() {
        return totalBytesWritten;
    }

    @JmxGetter(name = "bytesTransferredPerSecond", description = "The rate of the transfer in bytes/second.")
    public double getBytesTransferredPerSecond() {
        double elapsedSecs = (System.nanoTime() - lastReportNs) / (double) Time.NS_PER_SECOND;
        return bytesTransferredSinceLastReport / elapsedSecs;
    }

    @JmxGetter(name = "bytesWrittenPerSecond", description = "The rate of persisting data to disk in bytes/second.")
    public double getBytesWrittenPerSecond() {
        double elapsedSecs = (System.nanoTime() - lastReportNs) / (double) Time.NS_PER_SECOND;
        return bytesWrittenSinceLastReport / elapsedSecs;
    }

    @JmxGetter(name = "filename", description = "The file path being copied.")
    public String getFilename() {
        return this.sourceFile;
    }
}

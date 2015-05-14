package voldemort.store.readonly.fetcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.Time;

public class HdfsCopyStats {

    private final String sourceFile;
    private final long startTimeMS;
    private volatile long bytesSinceLastReport;
    private volatile long totalBytesCopied;
    private volatile long lastReportNs;
    private volatile long totalBytes;

    private BufferedWriter statsFileWriter = null;
    public static final String STATS_DIRECTORY = ".stats";
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Logger logger = Logger.getLogger(HdfsCopyStats.class);

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

            String destName = destination.getName();
            File statsFile = new File(statsDirectory, destName);
            statsFile.createNewFile();

            statsFileWriter = new BufferedWriter(new FileWriter(statsFile));
            statsFileWriter.write("Starting fetch at " + startTimeMS + "MS from "
                                  + sourceFile);
            statsFileWriter.newLine();
            statsFileWriter.write("Time, FileName, StartTime(MS), Size, TimeTaken(MS), Attempts #, TotalBytes");
            statsFileWriter.newLine();

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
                         long totalBytes) {
        this.sourceFile = source;
        this.totalBytesCopied = 0L;
        this.bytesSinceLastReport = 0L;
        this.totalBytes = totalBytes;
        this.lastReportNs = System.nanoTime();
        this.startTimeMS = System.currentTimeMillis();
        initializeStatsFile(destination, enableStatsFile, maxVersionsStatsFile, isFileCopy);
    }

    public void recordBytes(long bytes) {
        this.totalBytesCopied += bytes;
        this.bytesSinceLastReport += bytes;
    }

    public void reset() {
        this.bytesSinceLastReport = 0;
        this.lastReportNs = System.nanoTime();
    }

    public long getBytesSinceLastReport() {
        return bytesSinceLastReport;
    }
    
    private void reportStats(String message) {
        try {
            if(statsFileWriter != null) {
                statsFileWriter.write(dateFormat.format(new Date()));
                statsFileWriter.write(",");
                statsFileWriter.write(message);
                statsFileWriter.newLine();
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
        }
    }

    public void reportFileError(File file,int attempts, long startTimeMS, Throwable t) {
        long nowMS = System.currentTimeMillis();
        String message = " Error occured during file download " + file.getName()
                         + " after attempts " + attempts + " time elapsed " + (nowMS - startTimeMS)
                         + " MS.";
        reportError(message, t);
    }

    public void reportFileDownloaded(File file,
                                     long startTimeMS,
                                     long fileSize,
                                     long timeTakenMS,
                                     int attempts,
                                     long totalBytesDownloaded) {
        reportStats(file.getName() + "," + startTimeMS + "," + fileSize + "," + timeTakenMS + ","
                    + attempts + "," + totalBytesDownloaded);
    }

    public void complete() {
        long nowMS = System.currentTimeMillis() ;
        reportStats(" Completed at " + nowMS + "MS. Total bytes Copied " + totalBytesCopied
                    + " . Expected Total bytes " + totalBytes + " . Time taken(MS) "
                    + (nowMS - startTimeMS));
        if(statsFileWriter != null) {
            IOUtils.closeQuietly(statsFileWriter);
        }
    }

    public double getPercentCopied() {
        if(totalBytes == 0) {
            return 0.0;
        } else {
            return (double) (totalBytesCopied * 100) / (double) totalBytes;
        }
    }

    @JmxGetter(name = "totalBytesCopied", description = "The total number of bytes copied so far in this transfer.")
    public long getTotalBytesCopied() {
        return totalBytesCopied;
    }

    @JmxGetter(name = "bytesPerSecond", description = "The rate of the transfer in bytes/second.")
    public double getBytesPerSecond() {
        double ellapsedSecs = (System.nanoTime() - lastReportNs) / (double) Time.NS_PER_SECOND;
        return bytesSinceLastReport / ellapsedSecs;
    }

    @JmxGetter(name = "filename", description = "The file path being copied.")
    public String getFilename() {
        return this.sourceFile;
    }
}

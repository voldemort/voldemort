package voldemort.store.readonly.fetcher;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class BasicFetchStrategy implements FetchStrategy {

    private final FileSystem fs;

    private final HdfsCopyStats stats;
    private final byte[] buffer;
    private final HdfsFetcher fetcher;
    private final int bufferSize;
    private final AsyncOperationStatus status;

    private static final Logger logger = Logger.getLogger(BasicFetchStrategy.class);

    public BasicFetchStrategy(HdfsFetcher fetcher,
                              FileSystem fs,
                              HdfsCopyStats stats,
                              AsyncOperationStatus status,
                              int bufferSize) {
        this.fs = fs;
        this.stats = stats;
        this.status = status;
        this.buffer = new byte[bufferSize];
        this.bufferSize = bufferSize;
        this.fetcher = fetcher;
    }

    @Override
    public Map<HdfsFile, byte[]> fetch(HdfsDirectory directory, File dest) throws IOException {

        Map<HdfsFile, byte[]> fileCheckSumMap = new HashMap<HdfsFile, byte[]>(directory.getFiles().size());

        CheckSumType checkSumType = directory.getCheckSumType();
        for (HdfsFile file : directory.getFiles()) {
            String fileName = file.getDiskFileName();
            File copyLocation = new File(dest, fileName);
            CheckSum fileCheckSumGenerator = copyFileWithCheckSum(file, copyLocation, checkSumType);
            if (fileCheckSumGenerator != null) {
                fileCheckSumMap.put(file, fileCheckSumGenerator.getCheckSum());
            }
        }
        return fileCheckSumMap;
    }

    /**
     * Function to copy a file from the given filesystem with a checksum of type
     * 'checkSumType' computed and returned. In case an error occurs during such
     * a copy, we do a retry for a maximum of NUM_RETRIES
     *
     * @param source
     *            Source path of the file to copy
     * @param dest
     *            Destination path of the file on the local machine
     * @param checkSumType
     *            Type of the Checksum to be computed for this file
     * @return A Checksum (generator) of type checkSumType which contains the
     *         computed checksum of the copied file
     * @throws IOException
     */
    private CheckSum copyFileWithCheckSum(HdfsFile source, File dest, CheckSumType checkSumType) throws IOException {
        CheckSum fileCheckSumGenerator = null;
        logger.debug("Starting copy of " + source + " to " + dest);

        // Check if its Gzip compressed
        boolean isCompressed = source.isCompressed();
        FilterInputStream input = null;

        OutputStream output = null;
        long startTimeMS = System.currentTimeMillis();
        int previousAttempt = 0;

        for (int attempt = 1; attempt <= fetcher.getMaxAttempts(); attempt++) {
            boolean success = false;
            long totalBytesRead = 0;
            boolean fsOpened = false;
            try {

                // Create a per file checksum generator
                if (checkSumType != null) {
                    fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
                }

                logger.info("Starting attempt # " + attempt + " / " + fetcher.getMaxAttempts() +
                        " to fetch remote file: " + source +
                        "\nLocal destination: " + dest);

                input = new ThrottledInputStream(fs.open(source.getPath()), fetcher.getThrottler(), stats);

                if (isCompressed) {
                    // We are already bounded by the "hdfs.fetcher.buffer.size"
                    // specified in the Voldemort config, the default value of
                    // which is 64K. Using the same as the buffer size for
                    // GZIPInputStream as well.
                    input = new GZIPInputStream(input, this.bufferSize);
                }
                fsOpened = true;

                output = new BufferedOutputStream(new FileOutputStream(dest));

                int read;

                while (true) {
                    read = input.read(buffer);
                    if (read < 0) {
                        break;
                    } else {
                        output.write(buffer, 0, read);
                    }

                    // Update the per file checksum
                    if(fileCheckSumGenerator != null) {
                        fileCheckSumGenerator.update(buffer, 0, read);
                    }

                    stats.recordBytesWritten(read);
                    totalBytesRead += read;
                    boolean reportIntervalPassed = stats.getBytesTransferredSinceLastReport() > fetcher.getReportingIntervalBytes();
                    if (attempt != previousAttempt || reportIntervalPassed) {
                        previousAttempt = attempt;
                        NumberFormat format = NumberFormat.getNumberInstance();
                        format.setMaximumFractionDigits(2);
                        String message = stats.getTotalBytesTransferred() / (1024 * 1024) + " MB copied at "
                                + format.format(stats.getBytesTransferredPerSecond() / (1024 * 1024)) + " MB/sec"
                                + ", " + format.format(stats.getPercentCopied()) + " % complete"
                                + ", attempt: " + attempt + " / " + fetcher.getMaxAttempts()
                                + ", current file: " + dest.getName();
                        if(this.status != null) {
                            this.status.setStatus(message);
                        }
                        if (reportIntervalPassed) {
                            logger.info(message);
                            stats.reset();
                        }
                    }
                }
                stats.reportFileDownloaded(dest,
                        startTimeMS,
                        source.getSize(),
                        System.currentTimeMillis() - startTimeMS,
                        attempt,
                        totalBytesRead);
                logger.info("Completed copy of " + source + " to " + dest);
                success = true;
            } catch (IOException e) {
                if(!fsOpened) {
                    logger.error("Error while opening the file stream to " + source, e);
                } else {
                    logger.error("Error while copying file " + source + " after " + totalBytesRead + " bytes.", e);
                }
                if(e.getCause() != null) {
                    logger.error("Cause of error ", e.getCause());
                }

                if(attempt < fetcher.getMaxAttempts()) {
                    logger.info("Will retry copying after " + fetcher.getRetryDelayMs() + " ms");
                    sleepForRetryDelayMs();
                } else {
                    stats.reportFileError(dest, fetcher.getMaxAttempts(), startTimeMS, e);
                    logger.info("Fetcher giving up copy after " + fetcher.getMaxAttempts() + " attempts");
                    throw e;
                }
            } finally {
                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(input);
                if(success) {
                    break;
                }
            }
            logger.debug("Completed copy of " + source + " to " + dest);
        }
        return fileCheckSumGenerator;
    }

    private void sleepForRetryDelayMs() {
        if (fetcher.getRetryDelayMs() > 0) {
            try {
                Thread.sleep(fetcher.getRetryDelayMs());
            } catch (InterruptedException ie) {
                logger.error("Fetcher interrupted while waiting to retry", ie);
            }
        }
    }

    @Override
    public CheckSum fetch(HdfsFile file, File dest, CheckSumType checkSumType) throws IOException {
        return copyFileWithCheckSum(file, dest, checkSumType);
    }

}

package voldemort.store.readonly.fetcher;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.server.protocol.admin.AsyncOperationStoppedException;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;


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
            byte[] fileCheckSum = copyFileWithCheckSum(file, copyLocation, checkSumType);
            if(fileCheckSum != null) {
                fileCheckSumMap.put(file, fileCheckSum);
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
    private byte[] copyFileWithCheckSum(HdfsFile source, File dest, CheckSumType checkSumType)
            throws IOException {
        byte[] checkSum = null;
        CheckSum bufferCheckSumGenerator = null;
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
            bufferCheckSumGenerator = null;

            stats.singleFileFetchStart(attempt != 1);
            try {
                // Create a per file checksum generator
                if (checkSumType != null) {
                    bufferCheckSumGenerator = CheckSum.getInstance(checkSumType);
                }

                logger.info("Starting attempt #" + attempt + "/" + fetcher.getMaxAttempts() +
                        " to fetch remote file: " + source + " to local destination: " + dest);

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
                    if (status != null && status.hasException()) {
                        Exception ex = status.getException();
                        if (ex instanceof AsyncOperationStoppedException) {
                            // Then stop() has been called, so let's bubble up the exception
                            throw (AsyncOperationStoppedException) ex;
                        }
                    }

                    read = input.read(buffer);
                    if (read < 0) {
                        break;
                    } else {
                        output.write(buffer, 0, read);
                    }

                    // Update the per file checksum
                    if(bufferCheckSumGenerator != null) {
                        bufferCheckSumGenerator.update(buffer, 0, read);
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
                                + ", attempt: #" + attempt + "/" + fetcher.getMaxAttempts()
                                + ", current file: " + dest.getName();
                        if (this.status == null) {
                            // This is to accommodate tests and the old ReadOnlyStoreManagementServlet code path
                            // FIXME: Delete this when we get rid of the old code which does not use status
                            logger.info(message);
                        } else {
                            this.status.setStatus(message);
                            // status.toString() is more detailed than just the message. We print the whole
                            // thing so that server-side logs are very similar to client (BnP) -side logs.
                            logger.info(this.status.toString());
                        }

                        if (reportIntervalPassed) {
                            stats.reset();
                        }
                    }
                }
                if(bufferCheckSumGenerator != null) {
                    checkSum = bufferCheckSumGenerator.getCheckSum();
                }
                stats.reportFileDownloaded(dest,
                                           startTimeMS,
                                           source.getSize(),
                                           System.currentTimeMillis() - startTimeMS,
                                           attempt,
                                           totalBytesRead,
                                           checkSum);
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
                stats.singleFileFetchEnd();

                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(input);
                if(success) {
                    break;
                }
            }
        }

        //second time checksum validation. Check if the local file is consistent with the buffer
        if (bufferCheckSumGenerator != null){
            CheckSum fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(dest));
            int read;

            try {
                while ((read = in.read(buffer)) >= 0) {
                    fileCheckSumGenerator.update(buffer, 0, read);
                }

                if (ByteUtils.compare(fileCheckSumGenerator.getCheckSum(), checkSum) != 0)
                    throw new VoldemortException("Local file: " + dest.getAbsolutePath() +
                            " checksum (" + ByteUtils.toHexString(fileCheckSumGenerator.getCheckSum()) +
                            ") does not match with the checksum in the buffer (" +
                            ByteUtils.toHexString(fileCheckSumGenerator.getCheckSum()) + ")");
            }finally {
                IOUtils.closeQuietly(in);
            }
        }

        return checkSum;
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
    public byte[] fetch(HdfsFile file, File dest, CheckSumType checkSumType) throws IOException {
        return copyFileWithCheckSum(file, dest, checkSumType);
    }

}

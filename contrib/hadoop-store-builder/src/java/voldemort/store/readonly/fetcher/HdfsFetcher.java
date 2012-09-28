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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;
import voldemort.utils.DynamicEventThrottler;
import voldemort.utils.DynamicThrottleLimit;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/*
 * A fetcher that fetches the store files from HDFS
 */
public class HdfsFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);

    private final Long maxBytesPerSecond, reportingIntervalBytes;
    private final int bufferSize;
    private static final AtomicInteger copyCount = new AtomicInteger(0);
    private AsyncOperationStatus status;
    private EventThrottler throttler = null;
    private long minBytesPerSecond = 0;
    private DynamicThrottleLimit globalThrottleLimit = null;
    private static final int NUM_RETRIES = 3;

    public HdfsFetcher(VoldemortConfig config) {
        this(config.getMaxBytesPerSecond(),
             config.getReportingIntervalBytes(),
             config.getFetcherBufferSize());

        logger.info("Created hdfs fetcher with throttle rate " + maxBytesPerSecond
                    + ", buffer size " + bufferSize + ", reporting interval bytes "
                    + reportingIntervalBytes);
    }

    public HdfsFetcher(VoldemortConfig config, DynamicThrottleLimit dynThrottleLimit) {
        this(dynThrottleLimit,
             config.getReportingIntervalBytes(),
             config.getFetcherBufferSize(),
             config.getMinBytesPerSecond());

        logger.info("Created hdfs fetcher with throttle rate " + dynThrottleLimit.getRate()
                    + ", buffer size " + bufferSize + ", reporting interval bytes "
                    + reportingIntervalBytes);
    }

    public HdfsFetcher() {
        this((Long) null,
             VoldemortConfig.REPORTING_INTERVAL_BYTES,
             VoldemortConfig.DEFAULT_BUFFER_SIZE);
    }

    public HdfsFetcher(Long maxBytesPerSecond, Long reportingIntervalBytes, int bufferSize) {
        this(null, maxBytesPerSecond, reportingIntervalBytes, bufferSize, 0);
    }

    public HdfsFetcher(DynamicThrottleLimit dynThrottleLimit,
                       Long reportingIntervalBytes,
                       int bufferSize,
                       long minBytesPerSecond) {
        this(dynThrottleLimit, null, reportingIntervalBytes, bufferSize, minBytesPerSecond);
    }

    public HdfsFetcher(DynamicThrottleLimit dynThrottleLimit,
                       Long maxBytesPerSecond,
                       Long reportingIntervalBytes,
                       int bufferSize,
                       long minBytesPerSecond) {
        if(maxBytesPerSecond != null) {
            this.maxBytesPerSecond = maxBytesPerSecond;
            this.throttler = new EventThrottler(this.maxBytesPerSecond);
        } else if(dynThrottleLimit != null && dynThrottleLimit.getRate() != 0) {
            this.maxBytesPerSecond = dynThrottleLimit.getRate();
            this.throttler = new DynamicEventThrottler(dynThrottleLimit);
            this.globalThrottleLimit = dynThrottleLimit;
            logger.info("Initializing Dynamic Event throttler with rate : "
                        + this.maxBytesPerSecond + " bytes / sec");
        } else
            this.maxBytesPerSecond = null;
        this.reportingIntervalBytes = Utils.notNull(reportingIntervalBytes);
        this.bufferSize = bufferSize;
        this.status = null;
        this.minBytesPerSecond = minBytesPerSecond;
    }

    public File fetch(String sourceFileUrl, String destinationFile) throws IOException {
        if(this.globalThrottleLimit != null) {
            if(this.globalThrottleLimit.getSpeculativeRate() < this.minBytesPerSecond)
                throw new VoldemortException("Too many push jobs.");
            this.globalThrottleLimit.incrementNumJobs();
        }

        ObjectName jmxName = null;
        try {

            Path path = new Path(sourceFileUrl);
            Configuration config = new Configuration();
            config.setInt("io.socket.receive.buffer", bufferSize);
            config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                       ConfigurableSocketFactory.class.getName());
            FileSystem fs = path.getFileSystem(config);

            CopyStats stats = new CopyStats(sourceFileUrl, sizeOfPath(fs, path));
            jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(), stats);
            File destination = new File(destinationFile);

            if(destination.exists()) {
                throw new VoldemortException("Version directory " + destination.getAbsolutePath()
                                             + " already exists");
            }

            boolean result = fetch(fs, path, destination, stats);

            if(result) {
                return destination;
            } else {
                return null;
            }
        } finally {
            if(this.globalThrottleLimit != null) {
                this.globalThrottleLimit.decrementNumJobs();
            }
            JmxUtils.unregisterMbean(jmxName);
        }
    }

    private boolean fetch(FileSystem fs, Path source, File dest, CopyStats stats)
            throws IOException {
        if(!fs.isFile(source)) {
            Utils.mkdirs(dest);
            FileStatus[] statuses = fs.listStatus(source);
            if(statuses != null) {
                // sort the files so that index files come last. Maybe
                // this will help keep them cached until the swap
                Arrays.sort(statuses, new IndexFileLastComparator());
                byte[] origCheckSum = null;
                CheckSumType checkSumType = CheckSumType.NONE;

                // Do a checksum of checksum - Similar to HDFS
                CheckSum checkSumGenerator = null;
                CheckSum fileCheckSumGenerator = null;

                for(FileStatus status: statuses) {

                    // Kept for backwards compatibility
                    if(status.getPath().getName().contains("checkSum.txt")) {

                        // Ignore old checksum files

                    } else if(status.getPath().getName().contains(".metadata")) {

                        logger.debug("Reading .metadata");
                        // Read metadata into local file
                        File copyLocation = new File(dest, status.getPath().getName());
                        copyFileWithCheckSum(fs, status.getPath(), copyLocation, stats, null);

                        // Open the local file to initialize checksum
                        ReadOnlyStorageMetadata metadata;
                        try {
                            metadata = new ReadOnlyStorageMetadata(copyLocation);
                        } catch(IOException e) {
                            logger.error("Error reading metadata file ", e);
                            throw new VoldemortException(e);
                        }

                        // Read checksum
                        String checkSumTypeString = (String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM_TYPE);
                        String checkSumString = (String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM);

                        if(checkSumTypeString != null && checkSumString != null) {

                            try {
                                origCheckSum = Hex.decodeHex(checkSumString.toCharArray());
                            } catch(DecoderException e) {
                                logger.error("Exception reading checksum file. Ignoring checksum ",
                                             e);
                                continue;
                            }

                            logger.debug("Checksum from .metadata "
                                         + new String(Hex.encodeHex(origCheckSum)));
                            checkSumType = CheckSum.fromString(checkSumTypeString);
                            checkSumGenerator = CheckSum.getInstance(checkSumType);
                            fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
                        }

                    } else if(!status.getPath().getName().startsWith(".")) {

                        // Read other (.data , .index files)
                        File copyLocation = new File(dest, status.getPath().getName());
                        copyFileWithCheckSum(fs,
                                             status.getPath(),
                                             copyLocation,
                                             stats,
                                             fileCheckSumGenerator);

                        if(fileCheckSumGenerator != null && checkSumGenerator != null) {
                            byte[] checkSum = fileCheckSumGenerator.getCheckSum();
                            logger.debug("Checksum for " + status.getPath() + " - "
                                         + new String(Hex.encodeHex(checkSum)));
                            checkSumGenerator.update(checkSum);
                        }
                    }

                }

                logger.info("Completed reading all files from " + source.toString() + " to "
                            + dest.getAbsolutePath());
                // Check checksum
                if(checkSumType != CheckSumType.NONE) {
                    byte[] newCheckSum = checkSumGenerator.getCheckSum();
                    boolean checkSumComparison = (ByteUtils.compare(newCheckSum, origCheckSum) == 0);

                    logger.info("Checksum generated from streaming - "
                                + new String(Hex.encodeHex(newCheckSum)));
                    logger.info("Checksum on file - " + new String(Hex.encodeHex(origCheckSum)));
                    logger.info("Check-sum verification - " + checkSumComparison);

                    return checkSumComparison;
                } else {
                    logger.info("No check-sum verification required");
                    return true;
                }
            }
        }
        logger.error("Source " + source.toString() + " should be a directory");
        return false;

    }

    private void copyFileWithCheckSum(FileSystem fs,
                                      Path source,
                                      File dest,
                                      CopyStats stats,
                                      CheckSum fileCheckSumGenerator) throws IOException {
        logger.info("Starting copy of " + source + " to " + dest);
        FSDataInputStream input = null;
        OutputStream output = null;
        for(int attempt = 0; attempt < NUM_RETRIES; attempt++) {
            boolean success = true;
            try {

                input = fs.open(source);
                output = new BufferedOutputStream(new FileOutputStream(dest));
                byte[] buffer = new byte[bufferSize];
                while(true) {
                    int read = input.read(buffer);
                    if(read < 0) {
                        break;
                    } else {
                        output.write(buffer, 0, read);
                    }

                    if(fileCheckSumGenerator != null)
                        fileCheckSumGenerator.update(buffer, 0, read);
                    if(throttler != null)
                        throttler.maybeThrottle(read);
                    stats.recordBytes(read);
                    if(stats.getBytesSinceLastReport() > reportingIntervalBytes) {
                        NumberFormat format = NumberFormat.getNumberInstance();
                        format.setMaximumFractionDigits(2);
                        logger.info(stats.getTotalBytesCopied() / (1024 * 1024) + " MB copied at "
                                    + format.format(stats.getBytesPerSecond() / (1024 * 1024))
                                    + " MB/sec - " + format.format(stats.getPercentCopied())
                                    + " % complete");
                        if(this.status != null) {
                            this.status.setStatus(stats.getTotalBytesCopied()
                                                  / (1024 * 1024)
                                                  + " MB copied at "
                                                  + format.format(stats.getBytesPerSecond()
                                                                  / (1024 * 1024)) + " MB/sec - "
                                                  + format.format(stats.getPercentCopied())
                                                  + " % complete");
                        }
                        stats.reset();
                    }
                }
                logger.info("Completed copy of " + source + " to " + dest);

            } catch(IOException ioe) {
                success = false;
                logger.error("Error during copying file ", ioe);
                if(attempt < NUM_RETRIES - 1)
                    logger.info("retrying copying");
                else
                    throw ioe;

            } finally {
                IOUtils.closeQuietly(output);
                IOUtils.closeQuietly(input);
                if(success)
                    break;

            }

        }
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

    public static class CopyStats {

        private final String fileName;
        private volatile long bytesSinceLastReport;
        private volatile long totalBytesCopied;
        private volatile long lastReportNs;
        private volatile long totalBytes;

        public CopyStats(String fileName, long totalBytes) {
            this.fileName = fileName;
            this.totalBytesCopied = 0L;
            this.bytesSinceLastReport = 0L;
            this.totalBytes = totalBytes;
            this.lastReportNs = System.nanoTime();
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
            return this.fileName;
        }
    }

    /**
     * A comparator that sorts index files last. This is a heuristic for
     * retaining the index file in page cache until the swap occurs
     * 
     */
    public static class IndexFileLastComparator implements Comparator<FileStatus> {

        public int compare(FileStatus fs1, FileStatus fs2) {
            // directories before files
            if(fs1.isDir())
                return fs2.isDir() ? 0 : -1;
            if(fs2.isDir())
                return fs1.isDir() ? 0 : 1;

            String f1 = fs1.getPath().getName(), f2 = fs2.getPath().getName();

            // All metadata files given priority
            if(f1.endsWith("metadata"))
                return -1;
            if(f2.endsWith("metadata"))
                return 1;

            // if both same, lexicographically
            if((f1.endsWith(".index") && f2.endsWith(".index"))
               || (f1.endsWith(".data") && f2.endsWith(".data"))) {
                return f1.compareToIgnoreCase(f2);
            }

            if(f1.endsWith(".index")) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public void setAsyncOperationStatus(AsyncOperationStatus status) {
        this.status = status;
    }

    /*
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java " + HdfsFetcher.class.getName() + " url");
        String url = args[0];
        long maxBytesPerSec = 1024 * 1024 * 1024;
        Path p = new Path(url);
        Configuration config = new Configuration();
        config.setInt("io.file.buffer.size", VoldemortConfig.DEFAULT_BUFFER_SIZE);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        config.setInt("io.socket.receive.buffer", 1 * 1024 * 1024 - 10000);
        FileStatus status = p.getFileSystem(config).getFileStatus(p);
        long size = status.getLen();
        HdfsFetcher fetcher = new HdfsFetcher(maxBytesPerSec,
                                              VoldemortConfig.REPORTING_INTERVAL_BYTES,
                                              VoldemortConfig.DEFAULT_BUFFER_SIZE);
        long start = System.currentTimeMillis();
        File location = fetcher.fetch(url, System.getProperty("java.io.tmpdir") + File.separator
                                           + start);
        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
    }
}

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.JmxUtils;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.Utils;

/**
 * A fetcher that fetches the store files from HDFS
 * 
 * 
 */
public class HdfsFetcher implements FileFetcher {

    private static final Logger logger = Logger.getLogger(HdfsFetcher.class);
    private static final String DEFAULT_TEMP_DIR = new File(System.getProperty("java.io.tmpdir"),
                                                            "hdfs-fetcher").getAbsolutePath();
    private static final int REPORTING_INTERVAL_BYTES = 100 * 1024 * 1024;
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private File tempDir;
    private final Long maxBytesPerSecond;
    private final int bufferSize;
    private final AtomicInteger copyCount = new AtomicInteger(0);
    private AsyncOperationStatus status;

    public HdfsFetcher(Props props) {
        this(props.containsKey("fetcher.max.bytes.per.sec") ? props.getBytes("fetcher.max.bytes.per.sec")
                                                           : null,
             new File(props.getString("hdfs.fetcher.tmp.dir", DEFAULT_TEMP_DIR)),
             (int) props.getBytes("hdfs.fetcher.buffer.size", DEFAULT_BUFFER_SIZE));
        logger.info("Created hdfs fetcher with temp dir = " + tempDir.getAbsolutePath()
                    + " and throttle rate " + maxBytesPerSecond + " and buffer size " + bufferSize);
    }

    public HdfsFetcher() {
        this((Long) null, null, DEFAULT_BUFFER_SIZE);
    }

    public HdfsFetcher(Long maxBytesPerSecond, File tempDir, int bufferSize) {
        if(tempDir == null)
            this.tempDir = new File(DEFAULT_TEMP_DIR);
        else
            this.tempDir = Utils.notNull(new File(tempDir, "hdfs-fetcher"));
        this.maxBytesPerSecond = maxBytesPerSecond;
        this.bufferSize = bufferSize;
        this.status = null;
        Utils.mkdirs(this.tempDir);
    }

    public File fetch(String fileUrl, String storeName) throws IOException {
        Path path = new Path(fileUrl);
        Configuration config = new Configuration();
        config.setInt("io.socket.receive.buffer", bufferSize);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        FileSystem fs = path.getFileSystem(config);
        EventThrottler throttler = null;
        if(maxBytesPerSecond != null)
            throttler = new EventThrottler(maxBytesPerSecond);

        CopyStats stats = new CopyStats(fileUrl);
        ObjectName jmxName = JmxUtils.registerMbean("hdfs-copy-" + copyCount.getAndIncrement(),
                                                    stats);
        try {
            File storeDir = new File(this.tempDir, storeName + "_" + System.currentTimeMillis());
            Utils.mkdirs(storeDir);

            File destination = new File(storeDir.getAbsoluteFile(), path.getName());
            boolean result = fetch(fs, path, destination, throttler, stats);
            if(result) {
                return destination;
            } else {
                return null;
            }
        } finally {
            JmxUtils.unregisterMbean(jmxName);
        }
    }

    private boolean fetch(FileSystem fs,
                          Path source,
                          File dest,
                          EventThrottler throttler,
                          CopyStats stats) throws IOException {
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

                    if(status.getPath().getName().contains("checkSum.txt")) {
                        checkSumType = CheckSum.fromString(status.getPath().getName());
                        checkSumGenerator = CheckSum.getInstance(checkSumType);
                        fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
                        FSDataInputStream input = fs.open(status.getPath());
                        origCheckSum = new byte[CheckSum.checkSumLength(checkSumType)];
                        input.read(origCheckSum);
                        input.close();
                        continue;
                    }
                    if(!status.getPath().getName().startsWith(".")) {
                        File copyLocation = new File(dest, status.getPath().getName());
                        copyFileWithCheckSum(fs,
                                             status.getPath(),
                                             copyLocation,
                                             throttler,
                                             stats,
                                             fileCheckSumGenerator);

                        if(fileCheckSumGenerator != null && checkSumGenerator != null) {
                            checkSumGenerator.update(fileCheckSumGenerator.getCheckSum());
                        }
                    }

                }

                // Check checksum
                if(checkSumType != CheckSumType.NONE) {
                    byte[] newCheckSum = checkSumGenerator.getCheckSum();
                    return (ByteUtils.compare(newCheckSum, origCheckSum) == 0);
                } else {
                    // If checkSum file does not exist
                    return true;
                }
            }
        }
        return false;

    }

    private void copyFileWithCheckSum(FileSystem fs,
                                      Path source,
                                      File dest,
                                      EventThrottler throttler,
                                      CopyStats stats,
                                      CheckSum fileCheckSumGenerator) throws IOException {
        logger.info("Starting copy of " + source + " to " + dest);
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            input = fs.open(source);
            output = new FileOutputStream(dest);
            byte[] buffer = new byte[bufferSize];
            while(true) {
                int read = input.read(buffer);
                if(read < 0) {
                    break;
                } else if(read < bufferSize) {
                    buffer = ByteUtils.copy(buffer, 0, read);
                }
                output.write(buffer);
                if(fileCheckSumGenerator != null)
                    fileCheckSumGenerator.update(buffer);
                if(throttler != null)
                    throttler.maybeThrottle(read);
                stats.recordBytes(read);
                if(stats.getBytesSinceLastReport() > REPORTING_INTERVAL_BYTES) {
                    NumberFormat format = NumberFormat.getNumberInstance();
                    format.setMaximumFractionDigits(2);
                    logger.info(stats.getTotalBytesCopied() / (1024 * 1024) + " MB copied at "
                                + format.format(stats.getBytesPerSecond() / (1024 * 1024))
                                + " MB/sec");
                    if(this.status != null) {
                        this.status.setStatus(stats.getTotalBytesCopied()
                                              / (1024 * 1024)
                                              + " MB copied at "
                                              + format.format(stats.getBytesPerSecond()
                                                              / (1024 * 1024)) + " MB/sec");
                    }
                    stats.reset();
                }
            }
            logger.info("Completed copy of " + source + " to " + dest);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(input);
        }
    }

    public static class CopyStats {

        private final String fileName;
        private volatile long bytesSinceLastReport;
        private volatile long totalBytesCopied;
        private volatile long lastReportNs;

        public CopyStats(String fileName) {
            this.fileName = fileName;
            this.totalBytesCopied = 0L;
            this.bytesSinceLastReport = 0L;
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
            else if(fs1.getPath().getName().endsWith("checkSum.txt"))
                return -1;
            else if(fs2.getPath().getName().endsWith("checkSum.txt"))
                return 1;
            // index files after all other files
            else if(fs1.getPath().getName().endsWith(".index"))
                return fs2.getPath().getName().endsWith(".index") ? 0 : 1;
            // everything else is equivalent
            else
                return 0;
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
            Utils.croak("USAGE: java " + HdfsFetcher.class.getName() + " url storeName");
        String url = args[0];
        String storeName = args[1];
        long maxBytesPerSec = 1024 * 1024 * 1024;
        Path p = new Path(url);
        Configuration config = new Configuration();
        config.setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
        config.set("hadoop.rpc.socket.factory.class.ClientProtocol",
                   ConfigurableSocketFactory.class.getName());
        config.setInt("io.socket.receive.buffer", 1 * 1024 * 1024 - 10000);
        FileStatus status = p.getFileSystem(config).getFileStatus(p);
        long size = status.getLen();
        HdfsFetcher fetcher = new HdfsFetcher(maxBytesPerSec, null, DEFAULT_BUFFER_SIZE);
        long start = System.currentTimeMillis();
        File location = fetcher.fetch(url, storeName);
        double rate = size * Time.MS_PER_SECOND / (double) (System.currentTimeMillis() - start);
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMaximumFractionDigits(2);
        System.out.println("Fetch to " + location + " completed: "
                           + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
    }
}
